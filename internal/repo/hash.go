package repo

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/kevburnsjr/tci-lru/lru"

	"highvolume.io/shackle/internal/config"
	"highvolume.io/shackle/internal/entity"
)

type Hash interface {
	Lock(hashes entity.Batch) (res []int8, err error)
	Close()
}

type hash struct {
	clock    clock.Clock
	keypfx   int
	keylen   int
	delRatio float64
	expTime  time.Duration
	ixenv    *lmdb.Env
	ixdb     lmdb.DBI
	tsenv    *lmdb.Env
	tsdb     lmdb.DBI
	mutex    sync.Mutex
	cache    lru.LRUCache
}

// NewHash returns a hash respository
func NewHash(cfg *config.Hash, partition int) (r *hash, err error) {
	var h string
	var (
		keylen     = cfg.Length
		keypfx     = cfg.KeySize
		expTime    = cfg.Expiration
		partitions = cfg.Partitions
		delRatio   = 1.25

		ixenv *lmdb.Env
		tsenv *lmdb.Env
		ixdb  lmdb.DBI
		tsdb  lmdb.DBI

		ixenvopt = lmdb.NoReadahead | lmdb.NoMemInit | lmdb.NoSync | lmdb.NoMetaSync
		tsenvopt = lmdb.NoReadahead | lmdb.NoMemInit
		ixdbopt  = lmdb.DupSort | lmdb.DupFixed | lmdb.Create
		tsdbopt  = lmdb.DupSort | lmdb.DupFixed | lmdb.Create
	)
	if _, err = os.Stat(cfg.IndexPath); os.IsNotExist(err) {
		return
	}
	if _, err = os.Stat(cfg.TimeseriesPath); os.IsNotExist(err) {
		return
	}

	b := byte(partition % partitions)
	h = hex.EncodeToString([]byte{b})
	var path = fmt.Sprintf("%s/%s/%s.ix.lmdb", cfg.IndexPath, string(h[0]), h)
	os.MkdirAll(path, 0777)
	ixenv, err = lmdb.NewEnv()
	ixenv.SetMaxDBs(1)
	ixenv.SetMapSize(int64(1 << 37))
	ixenv.Open(path, uint(ixenvopt), 0777)
	err = ixenv.Update(func(txn *lmdb.Txn) error {
		ixdb, err = txn.OpenDBI("index", uint(ixdbopt))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return
	}
	path = fmt.Sprintf("%s/%s/%s.ts.lmdb", cfg.TimeseriesPath, string(h[0]), h)
	os.MkdirAll(path, 0777)
	tsenv, err = lmdb.NewEnv()
	tsenv.SetMaxDBs(1)
	tsenv.SetMapSize(int64(1 << 37))
	tsenv.Open(path, uint(tsenvopt), 0777)
	err = tsenv.Update(func(txn *lmdb.Txn) error {
		tsdb, err = txn.OpenDBI("history", uint(tsdbopt))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return
	}
	cache, err := lru.NewLRU(cfg.CacheSize, nil)
	if err != nil {
		return
	}

	return &hash{clock.New(), keylen, keypfx, delRatio, expTime, ixenv, ixdb, tsenv, tsdb, sync.Mutex{}, cache}, nil
}

// Lock determines whether each hash has been seen and locks for processing
func (c hash) Lock(batch entity.Batch) (res []int8, err error) {
	if len(batch) < 1 {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var t = c.clock.Now()
	var ts = make([]byte, 8)
	binary.BigEndian.PutUint32(ts, uint32(t.Unix()))
	var expts = make([]byte, 8)
	binary.BigEndian.PutUint32(expts, uint32(t.Add(-1*c.expTime).Unix()))
	var keys []byte
	var tags = []string{string(ts)}
	res = make([]int8, len(batch))
	err = c.tsenv.Update(func(tstxn *lmdb.Txn) error {
		err = c.ixenv.View(func(ixtxn *lmdb.Txn) error {
			ixtxn.RawRead = true
			cur, err := ixtxn.OpenCursor(c.ixdb)
			if err != nil {
				return err
			}
			for i, item := range batch {
				if c.cache.Contains(string(item.Hash)) {
					res[i] = entity.LOCK_BUSY
				}
			}
			for i, item := range batch {
				if res[i] != 0 {
					continue
				}
				_, _, err = cur.Get(item.Hash[:c.keypfx], item.Hash[c.keypfx:], 0)
				if err == nil {
					res[i] = entity.LOCK_EXISTS
					keys = append(keys, item.Hash...)
					c.cache.Add(string(item.Hash), tags)
				} else if !lmdb.IsNotFound(err) {
					return err
				}
			}
			if len(keys) == 0 {
				return nil
			}
			hcur, err := tstxn.OpenCursor(c.tsdb)
			if err != nil {
				return err
			}
			defer hcur.Close()
			if len(keys) > 0 {
				err = hcur.PutMulti(ts, keys, c.keylen, 0)
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err == nil {
		c.cache.Invalidate(tags)
	}

	return
}

// Close the database
func (c hash) Close() {
	c.ixenv.Close()
	c.tsenv.Close()
}
