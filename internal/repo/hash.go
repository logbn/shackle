package repo

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/bmatsuo/lmdb-go/lmdb"

	"highvolume.io/shackle/internal/config"
)

type lmdbBatch []lmdbBatchItem
type lmdbBatchItem struct {
	i int
	k []byte
}

type Hash interface {
	Lock(hashes [][]byte) (res []int, err error)
}

type hash struct {
	clock         clock.Clock
	keypfx        int
	keylen        int
	delRatio      float64
	expTime       time.Duration
	ixenv         map[byte]*lmdb.Env
	ixdb          map[byte]lmdb.DBI
	tsenv         map[byte]*lmdb.Env
	tsdb          map[byte]lmdb.DBI
}

// NewHash returns a hash respository
func NewHash(cfg *config.Hash) (r *hash, err error) {
	var h string
	var (
		keylen   = 16
		keypfx   = 8
		delRatio = 1.25
		expTime  = time.Duration(730 * time.Hour)

		ixenv    = map[byte]*lmdb.Env{}
		tsenv    = map[byte]*lmdb.Env{}
		ixenvopt = lmdb.NoReadahead | lmdb.NoMemInit | lmdb.NoSync | lmdb.NoMetaSync
		tsenvopt = lmdb.NoReadahead | lmdb.NoMemInit
		ixdb     = map[byte]lmdb.DBI{}
		tsdb     = map[byte]lmdb.DBI{}
		ixdbopt  = lmdb.DupSort | lmdb.DupFixed | lmdb.Create
		tsdbopt  = lmdb.DupSort | lmdb.DupFixed | lmdb.Create
	)
	if _, err = os.Stat(cfg.IndexPath); os.IsNotExist(err) {
		return
	}
	if _, err = os.Stat(cfg.TimeseriesPath); os.IsNotExist(err) {
		return
	}
	for i := 0; i < cfg.Partitions; i++ {
		b := byte(i)
		h = hex.EncodeToString([]byte{b})
		var path = fmt.Sprintf("%s/%s/%s.ix.lmdb", cfg.IndexPath, string(h[0]), h)
		os.MkdirAll(path, 0777)
		ixenv[b], err = lmdb.NewEnv()
		ixenv[b].SetMaxDBs(1)
		ixenv[b].SetMapSize(int64(1 << 37))
		ixenv[b].Open(path, uint(ixenvopt), 0777)
		err = ixenv[b].Update(func(txn *lmdb.Txn) error {
			ixdb[b], err = txn.OpenDBI("index", uint(ixdbopt))
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
		tsenv[b], err = lmdb.NewEnv()
		tsenv[b].SetMaxDBs(1)
		tsenv[b].SetMapSize(int64(1 << 37))
		tsenv[b].Open(path, uint(tsenvopt), 0777)
		err = tsenv[b].Update(func(txn *lmdb.Txn) error {
			tsdb[b], err = txn.OpenDBI("history", uint(tsdbopt))
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return
		}
	}

	return &hash{clock.New(), keylen, keypfx, delRatio, expTime, ixenv, ixdb, tsenv, tsdb}, nil
}

// Lock determines whether each hash has been seen and locks for processing
func (c hash) Lock(hashes [][]byte) (res []int, err error) {
	batches := map[byte]lmdbBatch{}
	res = make([]int, len(hashes))
	for i, s := range hashes {
		p := s[0] % 64
		if _, ok := batches[p]; !ok {
			batches[p] = lmdbBatch{}
		}
		batches[p] = append(batches[p], lmdbBatchItem{i, s[:]})
	}
	var scanned int
	var deleted int
	var duplicates int
	var t = c.clock.Now()
	var ts = make([]byte, 8)
	binary.BigEndian.PutUint32(ts, uint32(t.Unix()))
	var expts = make([]byte, 8)
	binary.BigEndian.PutUint32(expts, uint32(t.Add(-1*c.expTime).Unix()))
	for k, b := range batches {
		var keys []byte
		err = c.ixenv[k].Update(func(txn *lmdb.Txn) error {
			cur, err := txn.OpenCursor(c.ixdb[k])
			if err != nil {
				return err
			}
			defer cur.Close()
			// Insert items
			for _, item := range b {
				err = cur.Put(item.k[:c.keypfx], item.k[c.keypfx:], lmdb.NoDupData)
				if lmdb.IsErrno(err, lmdb.KeyExist) {
					res[item.i] = 1
					duplicates++
				} else if err != nil {
					return err
				} else {
					keys = append(keys, item.k...)
				}
				n, _ := cur.Count()
				scanned += int(n - 1)
			}
			err = c.tsenv[k].Update(func(tstxn *lmdb.Txn) error {
				hcur, err := tstxn.OpenCursor(c.tsdb[k])
				if err != nil {
					return err
				}
				defer hcur.Close()
				// Sweep history
				var sweep bool
				for {
					_, _, err := hcur.Get(nil, nil, lmdb.NextNoDup)
					if lmdb.IsNotFound(err) {
						break
					}
					if err != nil {
						return err
					}
					sweep = false
					for {
						hts, vals, err := hcur.Get(nil, nil, lmdb.NextMultiple)
						if lmdb.IsNotFound(err) {
							break
						}
						if err != nil {
							return err
						}
						if bytes.Compare(hts, expts) > 0 {
							break
						}
						sweep = true
						deleted += len(vals)
						break
					}
					if sweep {
						hcur.Del(lmdb.NoDupData)
					} else {
						break
					}
					if float64(deleted) >= float64(len(keys))*c.delRatio {
						break
					}
				}
				// Insert history
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
		if err != nil {
			return
		}
	}
	return
}

// Close the database
func (c hash) Close() {
	for _, db := range c.ixenv {
		db.Close()
	}
	for _, db := range c.tsenv {
		db.Close()
	}
}
