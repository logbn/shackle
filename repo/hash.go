package repo

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"sync"
	"bytes"
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
type FactoryHash func(cfg *config.RepoHash, partitions, id uint16) (r Hash, err error)

// Hash is a primary hash repository
type Hash interface {
	MultiExec(ops []uint8, batches []entity.Batch) (res [][]uint8, err error)
	// Remove(batch entity.Batch) (res []uint8, err error)
	// Peek(batch entity.Batch) (res []uint32, err error)
	Sync() error
	Close()
}

type hash struct {
	id        uint16
	partitions uint16
	clock     clock.Clock
	keyExp    time.Duration
	lockExp   time.Duration
	ixenv     *lmdb.Env
	ixdbi     map[uint16]lmdb.DBI
	ixmetadbi lmdb.DBI
	mutex     sync.Mutex
	cache     lru.LRUCache
}

// NewHash returns a hash respository
func NewHash(cfg *config.RepoHash, partitions, id uint16) (r Hash, err error) {
	var (
		ixenv     *lmdb.Env
		ixdbi     map[uint16]lmdb.DBI
		ixmetadbi lmdb.DBI
	)
	if _, err = os.Stat(cfg.PathIndex); os.IsNotExist(err) {
		return
	}
	if partitions == 0 {
		err = fmt.Errorf("Partition count missing in repo.NewHash")
		return
	}
	var mkdb = func(path string, envopt, dbopt int) (env *lmdb.Env, dbi map[uint16]lmdb.DBI, meta lmdb.DBI, err error) {
		err = os.MkdirAll(path, 0777)
		if err != nil {
			return
		}
		dbi = map[uint16]lmdb.DBI{}
		env, err = lmdb.NewEnv()
		env.SetMaxDBs(65536)
		env.SetMapSize(int64(1 << 39))
		env.Open(path, uint(envopt), 0777)
		err = env.Update(func(txn *lmdb.Txn) (err error) {
			for i := int(id); i < int(id) + 65536/int(partitions); i++ {
				name := fmt.Sprintf("%04x", i)
				dbi[uint16(i)], err = txn.OpenDBI(name, uint(dbopt|lmdb.Create))
				if err != nil {
					return
				}
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
		id,
		partitions,
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

// MultiExec executes a slice of batches of various operations returning a slice of responses.
func (c *hash) MultiExec(ops []uint8, batches []entity.Batch) (res [][]uint8, err error) {
	var tsi int
	var ts = make([]byte, 4)
	var now = c.clock.Now().Unix()
	var tss = strconv.Itoa(int(now))
	binary.BigEndian.PutUint32(ts, uint32(now))
	var removedFromCache = map[string]string{}
	var addedToCache = map[string]string{}
	var k []byte
	var v []byte
	var p uint16
	var cur *lmdb.Cursor
	res = make([][]uint8, len(batches))
	for bi, batch := range batches {
		res[bi] = make([]uint8, len(batch))
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()

	err = c.ixenv.Update(func(ixtxn *lmdb.Txn) (err error) {
		for bi, batch := range batches {
			switch ops[bi] {
			case entity.OP_COMMIT:
				for i, item := range batch {
					p = binary.BigEndian.Uint16(item.Hash[:2])
					if _, ok := c.ixdbi[p]; !ok {
						return fmt.Errorf("Wrong partition %04x for repo %04x .. %x", p, c.id, item.Hash)
					}
					cur, err = ixtxn.OpenCursor(c.ixdbi[p])
					if err != nil {
						return
					}
					hs := string(item.Hash)
					k, v, err = cur.Get(item.Hash[2:5], nil, lmdb.SetRange)
					if err == nil {
						for err == nil && len(v) > 0 && bytes.Compare(item.Hash[2:5], k) == 0 {
							if bytes.Compare(item.Hash[5:], v[:len(v)-4]) == 0 {
								res[bi][i] = entity.ITEM_EXISTS
								break
							}
							k, v, err = cur.Get(nil, nil, lmdb.NextDup)
						}
					}
					if res[bi][i] > 0 {
						// Monitor duplicate commit
						continue
					}
					if lmdb.IsNotFound(err) {
						err = nil
					} else if err != nil {
						return
					}
					v, _ := c.cache.Get(hs)
					if v == nil {
						tsi = int(now)
					} else {
						tsi, _ = strconv.Atoi(v.(string))
					}
					binary.BigEndian.PutUint32(ts, uint32(tsi))
					hash := append([]byte{}, item.Hash[5:]...)
					err = cur.Put(item.Hash[2:5], append(hash, ts...), 0)
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
					cv, _ := c.cache.Get(hash)
					if c.cache.Remove(hash) {
						removedFromCache[hash] = cv.(string)
						res[bi][i] = entity.ITEM_OPEN
					} else {
						p = binary.BigEndian.Uint16(item.Hash[:2])
						if _, ok := c.ixdbi[p]; !ok {
							return fmt.Errorf("Wrong partition %04x for repo %04x .. %x", p, c.id, item.Hash)
						}
						cur, err = ixtxn.OpenCursor(c.ixdbi[p])
						if err != nil {
							return
						}
						k, v, err = cur.Get(item.Hash[2:5], nil, lmdb.SetRange)
						if err == nil {
							for err == nil && len(v) > 0 && bytes.Compare(item.Hash[2:5], k) == 0 {
								if bytes.Compare(item.Hash[5:], v[:len(v)-4]) == 0 {
									res[bi][i] = entity.ITEM_EXISTS
									break
								}
								k, v, err = cur.Get(nil, nil, lmdb.NextDup)
							}
						}
						if lmdb.IsNotFound(err) {
							res[bi][i] = entity.ITEM_OPEN
							err = nil
						} else if err != nil {
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
					p = binary.BigEndian.Uint16(item.Hash[:2])
					if _, ok := c.ixdbi[p]; !ok {
						return fmt.Errorf("Wrong partition %04x for repo %04x .. %x", p, c.id, item.Hash)
					}
					cur, err = ixtxn.OpenCursor(c.ixdbi[p])
					if err != nil {
						return
					}
					k, v, err = cur.Get(item.Hash[2:5], nil, lmdb.SetRange)
					for err == nil && len(v) > 0 && bytes.Compare(item.Hash[2:5], k) == 0 {
						if bytes.Compare(item.Hash[5:], v[:len(v)-4]) == 0 {
							res[bi][i] = entity.ITEM_EXISTS
							break
						}
						k, v, err = cur.Get(nil, nil, lmdb.NextDup)
					}
					if lmdb.IsNotFound(err) || res[bi][i] == 0 {
						c.cache.Add(hash, tss, tss)
						addedToCache[hash] = tss
						res[bi][i] = entity.ITEM_LOCKED
						err = nil
					} else if err != nil {
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
