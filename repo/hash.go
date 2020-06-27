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
	lmdbixdbiopt   = lmdb.DupSort | lmdb.DupFixed
	lmdbtsdbiopt   = lmdb.DupSort | lmdb.DupFixed
	lmdbmetadbiopt = lmdb.Create

	// Partition hex prefixed (0f00_CHK_LOCK)
	chkLock  = []byte("_CHK_LOCK")
	chkTrunc = []byte("_CHK_TRUNC")
)

// FactoryHash is a factory that returns a hash repository
type FactoryHash func(cfg *config.RepoHash, id string, partitions []uint16) (r Hash, err error)

// Hash is a primary hash repository
type Hash interface {
	Lock(batch entity.Batch) (res []int8, err error)
	Rollback(batch entity.Batch) (res []int8, err error)
	Commit(batch entity.Batch) (res []int8, err error)
	SweepExpired(exp time.Time, limit int) (maxAge time.Duration, notFound, deleted int, err error)
	SweepLocked(exp time.Time) (scanned int, deleted int, err error)
	Close()
}

type hash struct {
	clock     clock.Clock
	keyExp    time.Duration
	lockExp   time.Duration
	ixenv     *lmdb.Env
	tsenv     *lmdb.Env
	ixdbi     map[uint16]lmdb.DBI
	tsdbi     map[uint16]lmdb.DBI
	ixmetadbi lmdb.DBI
	tsmetadbi lmdb.DBI
	mutex     sync.Mutex
	cache     lru.LRUCache
}

// NewHash returns a hash respository
func NewHash(cfg *config.RepoHash, id string, partitions []uint16) (r Hash, err error) {
	var (
		ixenv     *lmdb.Env
		tsenv     *lmdb.Env
		ixdbi     = map[uint16]lmdb.DBI{}
		tsdbi     = map[uint16]lmdb.DBI{}
		ixmetadbi lmdb.DBI
		tsmetadbi lmdb.DBI
	)
	if _, err = os.Stat(cfg.PathIndex); os.IsNotExist(err) {
		return
	}
	if _, err = os.Stat(cfg.PathTimeseries); os.IsNotExist(err) {
		return
	}
	var mkdb = func(path string, envopt, dbopt int) (env *lmdb.Env, dbs map[uint16]lmdb.DBI, meta lmdb.DBI, err error) {
		dbs = map[uint16]lmdb.DBI{}
		os.MkdirAll(path, 0777)
		env, err = lmdb.NewEnv()
		env.SetMaxDBs(65536)
		env.SetMapSize(int64(1 << 37))
		env.Open(path, uint(envopt), 0777)
		err = env.Update(func(txn *lmdb.Txn) (err error) {
			for _, p := range partitions {
				name := fmt.Sprintf("%04x", p)
				dbs[p], err = txn.OpenDBI(name, uint(dbopt|lmdb.Create))
				if err != nil {
					return
				}
			}
			meta, err = txn.OpenDBI("meta", uint(lmdbmetadbiopt|lmdb.Create))
			return
		})
		return
	}
	ixpath := fmt.Sprintf("%s/%s.ix.lmdb", cfg.PathIndex, id)
	ixenv, ixdbi, ixmetadbi, err = mkdb(ixpath, lmdbixenvopt, lmdbixdbiopt)
	if err != nil {
		return
	}
	tspath := fmt.Sprintf("%s/%s.ts.lmdb", cfg.PathTimeseries, id)
	tsenv, tsdbi, tsmetadbi, err = mkdb(tspath, lmdbtsenvopt, lmdbtsdbiopt)
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
	var keys = make(map[uint16][]byte)
	var stride = len(batch[0].Hash)

	c.mutex.Lock()
	defer c.mutex.Unlock()
	var tsi = c.clock.Now().UnixNano()
	var tss = strconv.Itoa(int(tsi))
	binary.BigEndian.PutUint64(ts, uint64(tsi))
	err = c.ixenv.View(func(ixtxn *lmdb.Txn) (err error) {
		err = c.tsenv.Update(func(tstxn *lmdb.Txn) (err error) {
			ixtxn.RawRead = true
			for i, item := range batch {
				if c.cache.Contains(string(item.Hash)) {
					res[i] = entity.ITEM_BUSY
					continue
				}
				_, err = ixtxn.Get(c.ixdbi[item.Partition], item.Hash)
				if err == nil {
					res[i] = entity.ITEM_EXISTS
				} else if lmdb.IsNotFound(err) {
					// Add hash to lock cache and queue hash for addition to history
					c.cache.Add(string(item.Hash), tss, tss)
					if _, ok := keys[item.Partition]; !ok {
						keys[item.Partition] = nil
					}
					keys[item.Partition] = append(keys[item.Partition], item.Hash...)
					res[i] = entity.ITEM_LOCKED
				} else {
					return
				}
			}
			for p, k := range keys {
				// Write hashes to history in batches
				var hcur *lmdb.Cursor
				hcur, err = tstxn.OpenCursor(c.tsdbi[p])
				if err != nil {
					return err
				}
				defer hcur.Close()
				err = hcur.PutMulti(ts, k, stride, 0)
				if err != nil {
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
					err = tstxn.Del(c.tsdbi[item.Partition], ts, item.Hash)
					if err == nil {
						c.cache.Remove(hs)
						removedFromCache[hs] = v.(string)
						res[i] = entity.ITEM_OPEN
					} else {
						return
					}
				} else {
					_, err = ixtxn.Get(c.ixdbi[item.Partition], item.Hash)
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
				_, err = ixtxn.Get(c.ixdbi[item.Partition], item.Hash)
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
				err = ixtxn.Put(c.ixdbi[item.Partition], item.Hash, ts, 0)
				if err == nil {
					res[i] = entity.ITEM_EXISTS
					if c.cache.Remove(hs) {
						removedFromCache[hs] = v.(string)
					} else {
						err = tstxn.Put(c.tsdbi[item.Partition], ts, item.Hash, 0)
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
func (c *hash) SweepExpired(exp time.Time, limit int) (maxAge time.Duration, notFound, deleted int, err error) {
	var v []byte
	var ts []byte
	var keys []byte
	var first []byte
	var stride int
	var tss string
	var chk = make([]byte, 4)
	var pbytes = make([]byte, 2)
	var pmaxAge time.Duration
	var expts = make([]byte, 8)
	var lastts = make([]byte, 8)
	var removedFromHistory = map[uint16]map[string][]byte{}
	binary.BigEndian.PutUint64(expts, uint64(exp.UnixNano()))

	c.mutex.Lock()
	defer c.mutex.Unlock()
	err = c.ixenv.Update(func(ixtxn *lmdb.Txn) (err error) {
		err = c.tsenv.Update(func(tstxn *lmdb.Txn) (err error) {
			for p, tsdbi := range c.tsdbi {
				hcur, err := tstxn.OpenCursor(tsdbi)
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
					for i := 0; ; i++ {
						_, v, err = hcur.Get(nil, nil, lmdb.NextMultiple)
						if lmdb.IsNotFound(err) {
							err = nil
							if i == 0 {
								v = first
							} else {
								break
							}
						}
						if err != nil {
							return err
						}
						keys = append(keys, v...)
						multi := lmdb.WrapMulti(v, stride)
						for i := 0; i < multi.Len(); i++ {
							err = ixtxn.Del(c.ixdbi[p], multi.Val(i), ts)
							if err != nil {
								if lmdb.IsNotFound(err) {
									notFound++
								} else {
									return err
								}
							}
						}
						deleted += multi.Len()
					}
					tss = strconv.Itoa(int(binary.BigEndian.Uint64(lastts)))
					if _, ok := removedFromHistory[p]; !ok {
						removedFromHistory[p] = map[string][]byte{}
					}
					removedFromHistory[p][tss] = keys
					hcur.Del(lmdb.NoDupData)
					if limit > 0 && deleted >= limit {
						break
					}
				}
				pmaxAge = c.clock.Now().Sub(time.Unix(0, int64(binary.BigEndian.Uint64(lastts))))
				if pmaxAge > maxAge {
					maxAge = pmaxAge
				}
				// Prefix meta checkpoint keys (6fc0_CHK_TRUNC)
				binary.BigEndian.PutUint16(pbytes, p)
				hex.Encode(chk, pbytes)
				err = ixtxn.Put(c.ixmetadbi, append(chk, chkTrunc...), lastts, 0)
			}
			return
		})
		return
	})
	if err != nil {
		err = c.restoreHistory(removedFromHistory, err)
		return
	}
	return
}

// Restore history on index transaction failure
func (c *hash) restoreHistory(removedFromHistory map[uint16]map[string][]byte, origErr error) (err error) {
	var ts = make([]byte, 8)
	err = origErr
	err2 := c.tsenv.Update(func(tstxn *lmdb.Txn) (err error) {
		for p, rfh := range removedFromHistory {
			for tss, k := range rfh {
				tsi, _ := strconv.Atoi(tss)
				binary.BigEndian.PutUint64(ts, uint64(tsi))
				err = tstxn.Put(c.tsdbi[p], ts, k, 0)
				if err != nil {
					return
				}
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
func (c *hash) SweepLocked(exp time.Time) (scanned int, deleted int, err error) {
	var v []byte
	var ts []byte
	var hs string
	var tss string
	var hcur *lmdb.Cursor
	var start []byte
	var partChkLock []byte
	var chk = make([]byte, 4)
	var pbytes = make([]byte, 2)
	var expts = make([]byte, 8)
	var lastts = make([]byte, 8)
	var removedFromCache = map[string]string{}
	binary.BigEndian.PutUint64(expts, uint64(exp.UnixNano()))
	c.mutex.Lock()
	defer c.mutex.Unlock()

	err = c.ixenv.Update(func(ixtxn *lmdb.Txn) (err error) {
		err = c.tsenv.Update(func(tstxn *lmdb.Txn) (err error) {
			for p, tsdbi := range c.tsdbi {
				// Prefix meta checkpoint keys (6fc0_CHK_TRUNC)
				binary.BigEndian.PutUint16(pbytes, p)
				hex.Encode(chk, pbytes)
				partChkLock = append(chk, chkLock...)

				hcur, err = tstxn.OpenCursor(tsdbi)
				if err != nil {
					return err
				}
				defer hcur.Close()
				start, err = tstxn.Get(c.tsmetadbi, partChkLock)
				if err != nil && !lmdb.IsNotFound(err) {
					return err
				}
				if start == nil {
					ts, v, err = hcur.Get(nil, nil, lmdb.First)
				} else {
					ts, v, err = hcur.Get(start, nil, lmdb.SetRange)
					if bytes.Compare(ts, start) == 0 {
						ts, v, err = hcur.Get(nil, nil, lmdb.NextNoDup)
					}
				}
				for {
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
					copy(lastts, ts)
					tss = strconv.Itoa(int(binary.BigEndian.Uint64(ts)))
					for {
						hs = string(v)
						scanned++
						if c.cache.Remove(hs) {
							err = hcur.Del(0)
							if err != nil {
								return err
							}
							removedFromCache[hs] = tss
							deleted++
						}
						ts, v, err = hcur.Get(nil, nil, lmdb.NextDup)
						if lmdb.IsNotFound(err) {
							err = nil
							break
						}
						if err != nil {
							return err
						}
					}
					err = tstxn.Put(c.tsmetadbi, partChkLock, lastts, 0)
					if err != nil {
						return
					}
					ts, v, err = hcur.Get(lastts, nil, lmdb.SetRange)
					if bytes.Compare(ts, lastts) == 0 {
						ts, v, err = hcur.Get(nil, nil, lmdb.NextNoDup)
					}
				}
				err = ixtxn.Put(c.ixmetadbi, partChkLock, lastts, 0)
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
		return
	}
	return
}

// Close the database
func (c *hash) Close() {
	c.ixenv.Close()
	c.tsenv.Close()
}
