package repo

import (
	"encoding/binary"
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
	lmdbixdbiopt   = lmdb.DupSort | lmdb.DupFixed
	lmdbmetadbiopt = 0

	// Partition hex prefixed (3a800000_CHK_LOCK)
	chkLock  = []byte("_CHK_LOCK")
	chkTrunc = []byte("_CHK_TRUNC")
)

// FactoryHash is a factory that returns a hash repository
type FactoryHash func(cfg *config.RepoHash, id uint16) (r Hash, err error)

// Hash is a primary hash repository
type Hash interface {
	Lock(batch entity.Batch) (res []uint8, err error)
	Rollback(batch entity.Batch) (res []uint8, err error)
	Commit(batch entity.Batch) (res []uint8, err error)
	MultiExec(ops []uint8, batches []entity.Batch) (res [][]uint8, err error)
	// Remove(batch entity.Batch) (res []uint8, err error)
	// Peek(batch entity.Batch) (res []uint32, err error)
	Sync() error
	Close()
}

type hash struct {
	clock     clock.Clock
	keyExp    time.Duration
	lockExp   time.Duration
	ixenv     *lmdb.Env
	ixdbi     lmdb.DBI
	ixmetadbi lmdb.DBI
	mutex     sync.Mutex
	cache     lru.LRUCache
}

// NewHash returns a hash respository
func NewHash(cfg *config.RepoHash, id uint16) (r Hash, err error) {
	var (
		ixenv     *lmdb.Env
		ixdbi     lmdb.DBI
		ixmetadbi lmdb.DBI
	)
	if _, err = os.Stat(cfg.PathIndex); os.IsNotExist(err) {
		return
	}
	var mkdb = func(path string, envopt, dbopt int) (env *lmdb.Env, db lmdb.DBI, meta lmdb.DBI, err error) {
		err = os.MkdirAll(path, 0777)
		if err != nil {
			return
		}
		env, err = lmdb.NewEnv()
		env.SetMaxDBs(2)
		env.SetMapSize(int64(1 << 39))
		env.Open(path, uint(envopt), 0777)
		err = env.Update(func(txn *lmdb.Txn) (err error) {
			name := fmt.Sprintf("index")
			db, err = txn.OpenDBI(name, uint(dbopt|lmdb.Create))
			if err != nil {
				return
			}
			meta, err = txn.OpenDBI("meta", uint(lmdbmetadbiopt|lmdb.Create))
			return
		})
		return
	}
	ixpath := fmt.Sprintf("%s/%04x.ix.lmdb", cfg.PathIndex, id)
	ixenv, ixdbi, ixmetadbi, err = mkdb(ixpath, lmdbixenvopt, lmdbixdbiopt)
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
		ixdbi,
		ixmetadbi,
		sync.Mutex{},
		cache,
	}, nil
}

// Lock locks items for processing if they are not already locked or committed
func (c *hash) Lock(batch entity.Batch) (res []uint8, err error) {
	if len(batch) < 1 {
		return
	}
	res = make([]uint8, len(batch))
	var ts = make([]byte, 4)
	var addedToCache = map[string]string{}

	c.mutex.Lock()
	defer c.mutex.Unlock()
	var tsi = c.clock.Now().Unix()
	var tss = strconv.Itoa(int(tsi))
	binary.BigEndian.PutUint32(ts, uint32(tsi))
	err = c.ixenv.View(func(ixtxn *lmdb.Txn) (err error) {
		ixtxn.RawRead = true
		for i, item := range batch {
			var hash = string(item.Hash)
			if c.cache.Contains(hash) {
				res[i] = entity.ITEM_BUSY
				continue
			}
			_, err = ixtxn.Get(c.ixdbi, item.Hash)
			if err == nil {
				res[i] = entity.ITEM_EXISTS
			} else if lmdb.IsNotFound(err) {
				c.cache.Add(hash, tss, tss)
				addedToCache[hash] = tss
				res[i] = entity.ITEM_LOCKED
				err = nil
			} else {
				return
			}
		}
		return
	})
	if err != nil {
		for hash := range addedToCache {
			c.cache.Remove(hash)
		}
	}

	return
}

// Rollback removes locks
func (c *hash) Rollback(batch entity.Batch) (res []uint8, err error) {
	if len(batch) < 1 {
		return
	}
	res = make([]uint8, len(batch))
	var removedFromCache = map[string]string{}

	c.mutex.Lock()
	defer c.mutex.Unlock()
	err = c.ixenv.View(func(ixtxn *lmdb.Txn) (err error) {
		for i, item := range batch {
			hash := string(item.Hash)
			v, _ := c.cache.Get(hash)
			if c.cache.Remove(hash) {
				removedFromCache[hash] = v.(string)
				res[i] = entity.ITEM_OPEN
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
		for hash, tss := range removedFromCache {
			c.cache.Add(hash, tss, tss)
		}
	}
	return
}

// Commit adds item to index and removes lock
func (c *hash) Commit(batch entity.Batch) (res []uint8, err error) {
	if len(batch) < 1 {
		return
	}
	var tsi int
	var ts = make([]byte, 4)
	res = make([]uint8, len(batch))
	var removedFromCache = map[string]string{}

	c.mutex.Lock()
	defer c.mutex.Unlock()
	var newtsi = c.clock.Now().Unix()
	err = c.ixenv.Update(func(ixtxn *lmdb.Txn) (err error) {
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
				tsi = int(newtsi)
			} else {
				tsi, _ = strconv.Atoi(v.(string))
			}
			binary.BigEndian.PutUint32(ts, uint32(tsi))
			err = ixtxn.Put(c.ixdbi, item.Hash, ts, 0)
			if err == nil {
				res[i] = entity.ITEM_EXISTS
				if c.cache.Remove(hs) {
					removedFromCache[hs] = v.(string)
				}
			} else {
				return
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
}

// MultiExec executes a slice of batches of various operations returning a slice of responses.
func (c *hash) MultiExec(ops []uint8, batches []entity.Batch) (res [][]uint8, err error) {
	res = make([][]uint8, len(batches))
	var tsi int
	var ts = make([]byte, 4)
	var newtsi = c.clock.Now().Unix()
	var tss = strconv.Itoa(int(newtsi))
	binary.BigEndian.PutUint32(ts, uint32(newtsi))
	var removedFromCache = map[string]string{}
	var addedToCache = map[string]string{}
	c.mutex.Lock()
	defer c.mutex.Unlock()

	err = c.ixenv.Update(func(ixtxn *lmdb.Txn) (err error) {
		for bi, batch := range batches {
			res[bi] = make([]uint8, len(batch))
			switch ops[bi] {
			case entity.OP_COMMIT:
				for i, item := range batch {
					hs := string(item.Hash)
					_, err = ixtxn.Get(c.ixdbi, item.Hash)
					if err == nil {
						res[bi][i] = entity.ITEM_EXISTS
						err = nil
						continue
					} else if !lmdb.IsNotFound(err) {
						return
					}
					v, _ := c.cache.Get(hs)
					if v == nil {
						tsi = int(newtsi)
					} else {
						tsi, _ = strconv.Atoi(v.(string))
					}
					binary.BigEndian.PutUint32(ts, uint32(tsi))
					err = ixtxn.Put(c.ixdbi, item.Hash, ts, 0)
					if err == nil {
						res[bi][i] = entity.ITEM_EXISTS
						if c.cache.Remove(hs) {
							removedFromCache[hs] = v.(string)
						}
					} else {
						return
					}
				}
			case entity.OP_ROLLBACK:
				for i, item := range batch {
					hash := string(item.Hash)
					v, _ := c.cache.Get(hash)
					if c.cache.Remove(hash) {
						removedFromCache[hash] = v.(string)
						res[bi][i] = entity.ITEM_OPEN
					} else {
						_, err = ixtxn.Get(c.ixdbi, item.Hash)
						if err == nil {
							res[bi][i] = entity.ITEM_EXISTS
						} else if lmdb.IsNotFound(err) {
							res[bi][i] = entity.ITEM_OPEN
							err = nil
						} else {
							return
						}
					}
				}
			case entity.OP_LOCK:
				for i, item := range batch {
					var hash = string(item.Hash)
					if c.cache.Contains(hash) {
						res[bi][i] = entity.ITEM_BUSY
						continue
					}
					_, err = ixtxn.Get(c.ixdbi, item.Hash)
					if err == nil {
						res[bi][i] = entity.ITEM_EXISTS
					} else if lmdb.IsNotFound(err) {
						c.cache.Add(hash, tss, tss)
						addedToCache[hash] = tss
						res[bi][i] = entity.ITEM_LOCKED
						err = nil
					} else {
						return
					}
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
		for hash := range addedToCache {
			c.cache.Remove(hash)
		}
	}
	return
}

// Sync calls fsync
func (c *hash) Sync() (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	err = c.ixenv.Sync(true)
	return
}

// Close the database
func (c *hash) Close() {
	c.ixenv.Close()
}
