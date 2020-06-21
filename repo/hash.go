package repo

import (
	"bytes"
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

	"highvolume.io/shackle/config"
	"highvolume.io/shackle/entity"
)

var (
	lmdbixenvopt   = lmdb.NoMemInit | lmdb.NoReadahead | lmdb.NoSync | lmdb.NoMetaSync
	lmdbtsenvopt   = lmdb.NoMemInit
	lmdbixdbiopt   = lmdb.DupSort | lmdb.DupFixed | lmdb.Create
	lmdbtsdbiopt   = lmdb.DupSort | lmdb.DupFixed | lmdb.Create
	lmdbmetadbiopt = lmdb.Create

	chkLock  = []byte("CHKPT_LOCK")
	chkTrunc = []byte("CHKPT_TRUNC")
)

// FactoryHash is a factory that returns a hash repository
type FactoryHash func(cfg *config.RepoHash, partition int) (r Hash, err error)

// Hash is a primary hash repository
type Hash interface {
	Lock(batch entity.Batch) (res []int8, err error)
	Rollback(batch entity.Batch) (res []int8, err error)
	Commit(batch entity.Batch) (res []int8, err error)
	SweepExpired(expts []byte, limit int) (maxAge time.Duration, deleted int, err error)
	SweepLocked(expts []byte) (scanned int, deleted int, err error)
	Close()
}

type hash struct {
	clock     clock.Clock
	keyExp    time.Duration
	lockExp   time.Duration
	ixenv     *lmdb.Env
	tsenv     *lmdb.Env
	ixdbi     lmdb.DBI
	tsdbi     lmdb.DBI
	ixmetadbi lmdb.DBI
	tsmetadbi lmdb.DBI
	mutex     sync.Mutex
	cache     lru.LRUCache
}

// NewHash returns a hash respository
func NewHash(cfg *config.RepoHash, partition int) (r Hash, err error) {
	var h string
	var (
		ixenv     *lmdb.Env
		tsenv     *lmdb.Env
		ixdbi     lmdb.DBI
		tsdbi     lmdb.DBI
		ixmetadbi lmdb.DBI
		tsmetadbi lmdb.DBI
	)
	if _, err = os.Stat(cfg.PathIndex); os.IsNotExist(err) {
		return
	}
	if _, err = os.Stat(cfg.PathTimeseries); os.IsNotExist(err) {
		return
	}
	b := byte(partition)
	h = hex.EncodeToString([]byte{b})
	var mkdb = func(path string, envopt, dbopt int, name string) (env *lmdb.Env, db lmdb.DBI, chkdb lmdb.DBI, err error) {
		os.MkdirAll(path, 0777)
		env, err = lmdb.NewEnv()
		env.SetMaxDBs(2)
		env.SetMapSize(int64(1 << 37))
		env.Open(path, uint(envopt), 0777)
		err = env.Update(func(txn *lmdb.Txn) (err error) {
			db, err = txn.OpenDBI(name, uint(dbopt))
			if err != nil {
				return
			}
			chkdb, err = txn.OpenDBI("meta", uint(lmdbmetadbiopt))
			return
		})
		return
	}
	ixpath := fmt.Sprintf("%s/%s/%s.ix.lmdb", cfg.PathIndex, string(h[0]), h)
	ixenv, ixdbi, ixmetadbi, err = mkdb(ixpath, lmdbixenvopt, lmdbixdbiopt, "index")
	if err != nil {
		return
	}
	tspath := fmt.Sprintf("%s/%s/%s.ts.lmdb", cfg.PathTimeseries, string(h[0]), h)
	tsenv, tsdbi, tsmetadbi, err = mkdb(tspath, lmdbtsenvopt, lmdbtsdbiopt, "history")
	if err != nil {
		return
	}
	cache, err := lru.NewLRU(cfg.CacheSize, nil)
	if err != nil {
		return
	}

	return &hash{
		clock.New(),
		cfg.KeyExpiration,
		cfg.LockExpiration,
		ixenv,
		tsenv,
		ixdbi,
		tsdbi,
		ixmetadbi,
		tsmetadbi,
		sync.Mutex{},
		cache,
	}, nil
}

// Lock locks items for processing if they are not already locked or committed
func (c *hash) Lock(batch entity.Batch) (res []int8, err error) {
	if len(batch) < 1 {
		return
	}
	res = make([]int8, len(batch))
	var ts = make([]byte, 8)

	c.mutex.Lock()
	defer c.mutex.Unlock()
	var tsi = c.clock.Now().UnixNano()
	var tss = strconv.Itoa(int(tsi))
	binary.BigEndian.PutUint64(ts, uint64(tsi))
	err = c.tsenv.Update(func(tstxn *lmdb.Txn) (err error) {
		err = c.ixenv.View(func(ixtxn *lmdb.Txn) (err error) {
			ixtxn.RawRead = true
			for i, item := range batch {
				if c.cache.Contains(string(item.Hash)) {
					res[i] = entity.ITEM_BUSY
					continue
				}
				_, err = ixtxn.Get(c.ixdbi, item.Hash)
				if err == nil {
					res[i] = entity.ITEM_EXISTS
				} else if lmdb.IsNotFound(err) {
					res[i] = entity.ITEM_LOCKED
					c.cache.Add(string(item.Hash), tss, tss)
					err = tstxn.Put(c.tsdbi, ts, item.Hash, 0)
					if err != nil {
						return
					}
				} else {
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
	var ts = make([]byte, 8)
	var removedFromCache = map[string]string{}

	c.mutex.Lock()
	defer c.mutex.Unlock()
	err = c.ixenv.View(func(ixtxn *lmdb.Txn) (err error) {
		err = c.tsenv.Update(func(tstxn *lmdb.Txn) (err error) {
			for i, item := range batch {
				hs := string(item.Hash)
				v, ok := c.cache.Get(hs)
				if ok {
					tsi, _ := strconv.Atoi(v.(string))
					binary.BigEndian.PutUint64(ts, uint64(tsi))
					err = tstxn.Del(c.tsdbi, ts, item.Hash)
					if err == nil {
						c.cache.Remove(hs)
						removedFromCache[hs] = v.(string)
						res[i] = entity.ITEM_OPEN
					} else {
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
						return
					}
				}
			}
			return
		})
		if err != nil {
			// Restore cache on transaction rollback
			for k, tss := range removedFromCache {
				c.cache.Add(k, tss, tss)
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
	var tsi int
	var ts = make([]byte, 8)
	res = make([]int8, len(batch))
	var removedFromCache = map[string]string{}

	c.mutex.Lock()
	defer c.mutex.Unlock()
	var newtsi = int(c.clock.Now().UnixNano())
	err = c.ixenv.Update(func(ixtxn *lmdb.Txn) (err error) {
		err = c.tsenv.Update(func(tstxn *lmdb.Txn) (err error) {
			for i, item := range batch {
				hs := string(item.Hash)
				_, err = ixtxn.Get(c.ixdbi, item.Hash)
				if err == nil {
					res[i] = entity.ITEM_EXISTS
					err = nil
					continue
				} else if !lmdb.IsNotFound(err) {
					return
				}
				v, _ := c.cache.Get(hs)
				if v == nil {
					tsi = newtsi
				} else {
					tsi, _ = strconv.Atoi(v.(string))
				}
				binary.BigEndian.PutUint64(ts, uint64(tsi))
				err = ixtxn.Put(c.ixdbi, item.Hash, ts, 0)
				if err == nil {
					res[i] = entity.ITEM_EXISTS
					if c.cache.Remove(hs) {
						removedFromCache[hs] = v.(string)
					} else {
						err = tstxn.Put(c.tsdbi, ts, item.Hash, 0)
						if err != nil {
							return
						}
					}
				} else {
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
			c.cache.Add(k, tss, tss)
		}
	}
	return
}

// SweepExpired deletes expired items
func (c *hash) SweepExpired(expts []byte, limit int) (maxAge time.Duration, deleted int, err error) {
	var v []byte
	var ts []byte
	var tss string
	var keys []byte
	var first []byte
	var stride int
	var lastts = make([]byte, 8)
	var removedFromHistory = map[string][]byte{}

	c.mutex.Lock()
	defer c.mutex.Unlock()
	err = c.ixenv.Update(func(ixtxn *lmdb.Txn) (err error) {
		err = c.tsenv.Update(func(tstxn *lmdb.Txn) (err error) {
			hcur, err := tstxn.OpenCursor(c.tsdbi)
			if err != nil {
				return err
			}
			defer hcur.Close()
			for {
				ts, first, err = hcur.Get(nil, nil, lmdb.NextNoDup)
				if lmdb.IsNotFound(err) {
					err = nil
					break
				}
				if err != nil {
					return err
				}
				if bytes.Compare(ts, expts) > 0 {
					break
				}
				lastts = ts
				keys = []byte("")
				stride = len(first)
				for {
					_, v, err = hcur.Get(nil, nil, lmdb.NextMultiple)
					if lmdb.IsNotFound(err) || ts == nil {
						err = nil
						break
					}
					if err != nil {
						return err
					}
					keys = append(keys, v...)
					multi := lmdb.WrapMulti(v, stride)
					for i := 0; i < multi.Len(); i++ {
						err = ixtxn.Del(c.ixdbi, multi.Val(i), ts)
						if err != nil && !lmdb.IsNotFound(err) {
							return err
						}
					}
					deleted += multi.Len()
				}
				tss = strconv.Itoa(int(binary.BigEndian.Uint64(lastts)))
				removedFromHistory[tss] = keys
				hcur.Del(lmdb.NoDupData)
				if limit > 0 && deleted >= limit {
					break
				}
			}
			maxAge = c.clock.Now().Sub(time.Unix(0, int64(binary.BigEndian.Uint64(lastts))))
			return
		})
		return
	})
	if err != nil {
		err = c.restoreHistory(removedFromHistory, err)
		return
	}
	err = c.ixenv.Update(func(ixtxn *lmdb.Txn) (err error) {
		err = ixtxn.Put(c.ixmetadbi, chkTrunc, lastts, 0)
		return
	})
	return
}

// Restore history on index transaction failure
func (c *hash) restoreHistory(removedFromHistory map[string][]byte, origErr error) (err error) {
	var ts = make([]byte, 8)
	err = origErr
	err2 := c.tsenv.Update(func(tstxn *lmdb.Txn) (err error) {
		for tss, k := range removedFromHistory {
			tsi, _ := strconv.Atoi(tss)
			binary.BigEndian.PutUint64(ts, uint64(tsi))
			err = tstxn.Put(c.tsdbi, ts, k, 0)
			if err != nil {
				return
			}
		}
		return
	})
	if err2 != nil {
		err = fmt.Errorf("Rollback failed: %s for error: %s", err2.Error(), err.Error())
	}
	return
}

// SweepLocked deletes items from history if present in lock cache and removes them from lock cache
// Starts from lock expiration checkpoint and evaluates all keys up to (time - expiration)
// No limit because it only touches history and in-memory cache. Doesn't affect the set.
// Cursor reads are sequential (fast)
func (c *hash) SweepLocked(expts []byte) (scanned int, deleted int, err error) {
	var v []byte
	var ts []byte
	var hs string
	var tss string
	var start []byte
	var first []byte
	var stride int
	var lastts = make([]byte, 8)
	var removedFromCache = map[string]string{}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	err = c.tsenv.Update(func(tstxn *lmdb.Txn) (err error) {
		hcur, err := tstxn.OpenCursor(c.tsdbi)
		if err != nil {
			return err
		}
		defer hcur.Close()
		start, err = tstxn.Get(c.tsmetadbi, chkLock)
		if err != nil && !lmdb.IsNotFound(err) {
			return err
		}
		if start == nil {
			ts, first, err = hcur.Get(nil, nil, lmdb.NextNoDup)
		} else {
			ts, first, err = hcur.Get(start, nil, lmdb.SetRange)
			if bytes.Compare(ts, start) == 0 {
				ts, first, err = hcur.Get(nil, nil, lmdb.NextNoDup)
			}
		}
		for {
			if lmdb.IsNotFound(err) || ts == nil {
				err = nil
				break
			}
			if err != nil {
				return err
			}
			if bytes.Compare(ts, expts) > 0 {
				break
			}
			lastts = ts
			tss = strconv.Itoa(int(binary.BigEndian.Uint64(ts)))
			stride = len(first)
			for {
				_, keys, err := hcur.Get(nil, nil, lmdb.NextMultiple)
				if lmdb.IsNotFound(err) || keys == nil {
					err = nil
					break
				}
				if err != nil {
					return err
				}
				multi := lmdb.WrapMulti(keys, stride)
				for i := 0; i < multi.Len(); i++ {
					v = multi.Val(i)
					hs = string(v)
					if c.cache.Remove(hs) {
						err = tstxn.Del(c.tsdbi, ts, v)
						if err != nil {
							return err
						}
						removedFromCache[hs] = tss
						deleted++
					}
					scanned++
				}
			}
			ts, _, err = hcur.Get(nil, nil, lmdb.NextNoDup)
		}
		err = tstxn.Put(c.tsmetadbi, chkLock, lastts, 0)
		if err != nil {
			return
		}
		return
	})
	if err != nil {
		// Restore cache on transaction rollback
		for k, tss := range removedFromCache {
			c.cache.Add(k, tss, tss)
		}
		return
	}
	err = c.ixenv.Update(func(ixtxn *lmdb.Txn) (err error) {
		err = ixtxn.Put(c.ixmetadbi, chkLock, lastts, 0)
		return
	})
	return
}

// Close the database
func (c *hash) Close() {
	c.ixenv.Close()
	c.tsenv.Close()
}
