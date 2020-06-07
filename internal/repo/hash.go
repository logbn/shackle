package repo

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/kevburnsjr/tci-lru/lru"

	"highvolume.io/shackle/internal/config"
	"highvolume.io/shackle/internal/entity"
)

type Hash interface {
	Lock(batch entity.Batch) (res []int8, err error)
	Rollback(batch entity.Batch) (res []int8, err error)
	Close()
}

type hash struct {
	clock    clock.Clock
	keypfx   int
	keylen   int
	delRatio float64
	keyExp   time.Duration
	lockExp  time.Duration
	ixenv    *lmdb.Env
	tsenv    *lmdb.Env
	ixdbi    lmdb.DBI
	tsdbi    lmdb.DBI
	mutex    sync.Mutex
	cache    lru.LRUCache
}

// NewHash returns a hash respository
func NewHash(cfg *config.Hash, partition int) (r *hash, err error) {
	var h string
	var (
		keylen     = cfg.Length
		keypfx     = cfg.KeySize
		partitions = cfg.Partitions
		delRatio   = 1.25

		ixenv *lmdb.Env
		tsenv *lmdb.Env
		ixdbi  lmdb.DBI
		tsdbi  lmdb.DBI

		ixenvopt = lmdb.NoReadahead | lmdb.NoMemInit | lmdb.NoSync | lmdb.NoMetaSync
		tsenvopt = lmdb.NoReadahead | lmdb.NoMemInit
		ixdbiopt  = lmdb.DupSort | lmdb.DupFixed | lmdb.Create
		tsdbiopt  = lmdb.DupSort | lmdb.DupFixed | lmdb.Create
	)
	if _, err = os.Stat(cfg.IndexPath); os.IsNotExist(err) {
		return
	}
	if _, err = os.Stat(cfg.TimeseriesPath); os.IsNotExist(err) {
		return
	}
	b := byte(partition % partitions)
	h = hex.EncodeToString([]byte{b})
	var mkdb = func(path string, envopt, dbopt int) (env *lmdb.Env, db lmdb.DBI, err error) {
		os.MkdirAll(path, 0777)
		env, err = lmdb.NewEnv()
		env.SetMaxDBs(1)
		env.SetMapSize(int64(1 << 37))
		env.Open(path, uint(envopt), 0777)
		err = env.Update(func(txn *lmdb.Txn) error {
			db, err = txn.OpenDBI("index", uint(dbopt))
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return
		}
		return
	}
	ixpath := fmt.Sprintf("%s/%s/%s.ix.lmdb", cfg.IndexPath, string(h[0]), h)
	ixenv, ixdbi, err = mkdb(ixpath, ixenvopt, ixdbiopt)
	if err != nil {
		return
	}
	tspath := fmt.Sprintf("%s/%s/%s.ts.lmdb", cfg.TimeseriesPath, string(h[0]), h)
	tsenv, tsdbi, err = mkdb(tspath, tsenvopt, tsdbiopt)
	if err != nil {
		return
	}
	cache, err := lru.NewLRU(cfg.CacheSize, nil)
	if err != nil {
		return
	}

	return &hash{
		clock.New(),
		keylen,
		keypfx,
		delRatio,
		cfg.KeyExpiration,
		cfg.LockExpiration,
		ixenv,
		tsenv,
		ixdbi,
		tsdbi,
		sync.Mutex{},
		cache,
	}, nil
}

// Lock determines whether each hash has been seen and locks for processing
func (c *hash) Lock(batch entity.Batch) (res []int8, err error) {
	if len(batch) < 1 {
		return
	}
	var keys []byte
	res = make([]int8, len(batch))

	c.mutex.Lock()
	defer c.mutex.Unlock()

	var tsi = c.clock.Now().UnixNano()
	var tss = strconv.Itoa(int(tsi))
	var ts = make([]byte, 16)
	binary.BigEndian.PutUint64(ts, uint64(tsi))

	err = c.tsenv.Update(func(tstxn *lmdb.Txn) error {
		err = c.ixenv.View(func(ixtxn *lmdb.Txn) error {
			ixtxn.RawRead = true
			cur, err := ixtxn.OpenCursor(c.ixdbi)
			if err != nil {
				return err
			}
			for i, item := range batch {
				if c.cache.Contains(string(item.Hash)) {
					res[i] = entity.LOCK_BUSY
				}
			}
			hcur, err := tstxn.OpenCursor(c.tsdbi)
			if err != nil {
				return err
			}
			defer hcur.Close()
			for i, item := range batch {
				if res[i] != 0 {
					continue
				}
				_, _, err = cur.Get(item.Hash[:c.keypfx], item.Hash[c.keypfx:], 0)
				if err == nil {
					res[i] = entity.LOCK_EXISTS
				} else if !lmdb.IsNotFound(err) {
					return err
				} else {
					keys = append(keys, item.Hash...)
					c.cache.Add(string(item.Hash), tss, tss)
					err = hcur.Put(ts, item.Hash, 0)
				}
			}
			if len(keys) == 0 {
				return nil
			}
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		c.cache.Invalidate([]string{tss})
	}

	return
}

// Rollback removes locks
func (c *hash) Rollback(batch entity.Batch) (res []int8, err error) {
	var removed = map[string]string{}
	res = make([]int8, len(batch))
	err = c.tsenv.Update(func(tstxn *lmdb.Txn) error {
		for i, item := range batch {
			hs := string(item.Hash)
			v, ok := c.cache.Get(hs)
			if ok {
				tsi, err := strconv.Atoi(v.(string))
				if err != nil {
					return err
				}
				var ts = make([]byte, 16)
				binary.BigEndian.PutUint64(ts, uint64(tsi))
				err = tstxn.Del(c.tsdbi, ts, item.Hash)
				if err == nil {
					removed[hs] = v.(string)
				} else if lmdb.IsNotFound(err) {
					res[i] = entity.ROLLBACK_UNEXPECTED
				} else {
					res[i] = entity.ROLLBACK_ERROR
					return err
				}
			} else {
				res[i] = entity.ROLLBACK_UNEXPECTED
			}
		}
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		for k, tss := range removed {
			c.cache.Add(string(k), tss, tss)
		}
	}
	return
}

// Close the database
func (c *hash) Close() {
	c.ixenv.Close()
	c.tsenv.Close()
}
