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

var (
	lmdbixenvopt = lmdb.NoReadahead | lmdb.NoMemInit | lmdb.NoSync | lmdb.NoMetaSync
	lmdbtsenvopt = lmdb.NoReadahead | lmdb.NoMemInit
	lmdbixdbiopt = lmdb.DupSort | lmdb.DupFixed | lmdb.Create
	lmdbtsdbiopt = lmdb.DupSort | lmdb.DupFixed | lmdb.Create
)

// Hash is a primary hash repository
type Hash interface {
	Lock(batch entity.Batch) (res []int8, err error)
	Rollback(batch entity.Batch) (res []int8, err error)
	Commit(batch entity.Batch) (res []int8, err error)
	Close()
}

type hash struct {
	clock    clock.Clock
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
		partitions = cfg.Partitions
		delRatio   = 1.25

		ixenv *lmdb.Env
		tsenv *lmdb.Env
		ixdbi  lmdb.DBI
		tsdbi  lmdb.DBI
	)
	if _, err = os.Stat(cfg.IndexPath); os.IsNotExist(err) {
		return
	}
	if _, err = os.Stat(cfg.TimeseriesPath); os.IsNotExist(err) {
		return
	}
	b := byte(partition % partitions)
	h = hex.EncodeToString([]byte{b})
	var mkdb = func(path string, envopt, dbopt int, name string) (env *lmdb.Env, db lmdb.DBI, err error) {
		os.MkdirAll(path, 0777)
		env, err = lmdb.NewEnv()
		env.SetMaxDBs(1)
		env.SetMapSize(int64(1 << 37))
		env.Open(path, uint(envopt), 0777)
		err = env.Update(func(txn *lmdb.Txn) (err error) {
			db, err = txn.OpenDBI(name, uint(dbopt))
			return
		})
		return
	}
	ixpath := fmt.Sprintf("%s/%s/%s.ix.lmdb", cfg.IndexPath, string(h[0]), h)
	ixenv, ixdbi, err = mkdb(ixpath, lmdbixenvopt, lmdbixdbiopt, "index")
	if err != nil {
		return
	}
	tspath := fmt.Sprintf("%s/%s/%s.ts.lmdb", cfg.TimeseriesPath, string(h[0]), h)
	tsenv, tsdbi, err = mkdb(tspath, lmdbtsenvopt, lmdbtsdbiopt, "history")
	if err != nil {
		return
	}
	cache, err := lru.NewLRU(cfg.CacheSize, nil)
	if err != nil {
		return
	}

	return &hash{
		clock.New(),
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
	res = make([]int8, len(batch))
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var tsi = c.clock.Now().UnixNano()
	var tss = strconv.Itoa(int(tsi))
	var ts = make([]byte, 8)
	binary.BigEndian.PutUint64(ts, uint64(tsi))
	for i, item := range batch {
		if c.cache.Contains(string(item.Hash)) {
			res[i] = entity.ITEM_BUSY
		}
	}
	err = c.tsenv.Update(func(tstxn *lmdb.Txn) (err error) {
		err = c.ixenv.View(func(ixtxn *lmdb.Txn) (err error) {
			ixtxn.RawRead = true
			hcur, err := tstxn.OpenCursor(c.tsdbi)
			if err != nil {
				return
			}
			defer hcur.Close()
			for i, item := range batch {
				if res[i] != 0 {
					continue
				}
				_, err = ixtxn.Get(c.ixdbi, item.Hash)
				if err == nil {
					res[i] = entity.ITEM_EXISTS
				} else if lmdb.IsNotFound(err) {
					res[i] = entity.ITEM_LOCKED
					c.cache.Add(string(item.Hash), tss, tss)
					err = hcur.Put(ts, item.Hash, 0)
					if err != nil {
						res[i] = entity.ITEM_ERROR
						return
					}
				} else {
					res[i] = entity.ITEM_ERROR
					return
				}
			}
			return
		})
		return
	})
	if err != nil {
		c.cache.Invalidate([]string{tss})
	}

	return
}

// Rollback removes locks
func (c *hash) Rollback(batch entity.Batch) (res []int8, err error) {
	if len(batch) < 1 {
		return
	}
	res = make([]int8, len(batch))
	var removedFromCache = map[string]string{}
	err = c.ixenv.View(func(ixtxn *lmdb.Txn) (err error) {
		err = c.tsenv.Update(func(tstxn *lmdb.Txn) (err error) {
			for i, item := range batch {
				hs := string(item.Hash)
				v, ok := c.cache.Get(hs)
				if ok {
					tsi, _ := strconv.Atoi(v.(string))
					var ts = make([]byte, 8)
					binary.BigEndian.PutUint64(ts, uint64(tsi))
					err = tstxn.Del(c.tsdbi, ts, item.Hash)
					if err == nil {
						c.cache.Remove(hs)
						removedFromCache[hs] = v.(string)
					} else {
						res[i] = entity.ITEM_ERROR
						return
					}
				} else {
					_, err = ixtxn.Get(c.ixdbi, item.Hash)
					if err == nil {
						res[i] = entity.ITEM_EXISTS
					} else if lmdb.IsNotFound(err) {
						res[i] = entity.ITEM_OPEN
						err = nil
					} else {
						res[i] = entity.ITEM_ERROR
						return
					}
				}
			}
			return
		})
		if err != nil {
			// Restore cache on transaction rollback
			for k, tss := range removedFromCache {
				c.cache.Add(string(k), tss, tss)
			}
		}
		return
	})
	return
}

// Commit adds item to index and removes lock
func (c *hash) Commit(batch entity.Batch) (res []int8, err error) {
	if len(batch) < 1 {
		return
	}
	res = make([]int8, len(batch))
	var removedFromCache = map[string]string{}
	var newtsi = int(c.clock.Now().UnixNano())
	err = c.ixenv.Update(func(ixtxn *lmdb.Txn) (err error) {
		err = c.tsenv.Update(func(tstxn *lmdb.Txn) (err error) {
			for i, item := range batch {
				hs := string(item.Hash)
				v, _ := c.cache.Get(hs)
				var tsi int
				if v == nil {
					tsi = newtsi
				} else {
					tsi, _ = strconv.Atoi(v.(string))
				}
				var ts = make([]byte, 8)
				binary.BigEndian.PutUint64(ts, uint64(tsi))
				err = ixtxn.Put(c.ixdbi, item.Hash, ts, 0)
				if err == nil {
					res[i] = entity.ITEM_EXISTS
					if c.cache.Remove(hs) {
						removedFromCache[hs] = v.(string)
					} else {
						err = tstxn.Put(c.tsdbi, ts, item.Hash, 0)
						if err != nil {
							res[i] = entity.ITEM_ERROR
							return
						}
					}
				} else if lmdb.IsErrno(err, lmdb.KeyExist) {
					res[i] = entity.ITEM_EXISTS
					err = nil
				} else {
					res[i] = entity.ITEM_ERROR
					return
				}
			}
			return
		})
		return
	})
	if err != nil {
		// Restore cache on transaction rollback
		for k, tss := range removedFromCache {
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
