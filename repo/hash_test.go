package repo

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"highvolume.io/shackle/config"
	"highvolume.io/shackle/entity"
)

// NewHash
func TestNewHash(t *testing.T) {
	fakedir := os.TempDir() + "/shackletest/nonexistant"
	realdir := os.TempDir() + "/shackletest/existant"
	os.RemoveAll(fakedir)
	os.MkdirAll(realdir, 0777)
	t.Run("NonExistantDatabase", func(t *testing.T) {
		_, err := NewHash(&config.RepoHash{
			PathIndex:      realdir,
			PathTimeseries: fakedir,
			CacheSize:      1000,
		}, "vnode1", []uint16{0x0000, 0x8000})
		require.True(t, os.IsNotExist(err))
		_, err = NewHash(&config.RepoHash{
			PathIndex:      fakedir,
			PathTimeseries: realdir,
			CacheSize:      1000,
		}, "vnode1", []uint16{0x0000, 0x8000})
		require.True(t, os.IsNotExist(err))
	})
	t.Run("Success", func(t *testing.T) {
		_, err := NewHash(&config.RepoHash{
			PathIndex:      realdir,
			PathTimeseries: realdir,
			CacheSize:      1000,
		}, "vnode1", []uint16{0x0000, 0x8000})
		require.Nil(t, err)
	})
}

// Hash
func TestHash(t *testing.T) {
	clk := clock.NewMock()
	tmpdir := os.TempDir() + "/shackletest/lmdb"
	os.RemoveAll(tmpdir)
	os.MkdirAll(tmpdir, 0777)
	repo, err := NewHash(&config.RepoHash{
		PathIndex:      tmpdir,
		PathTimeseries: tmpdir,
		CacheSize:      1000,
	}, "vnode1", []uint16{0x0000, 0x8000})
	require.Nil(t, err)
	repo.(*hash).clock = clk

	t.Run("LockCommit", func(t *testing.T) {
		items := entity.Batch{
			entity.BatchItem{0, 0, []byte("0000000000000000")},
			entity.BatchItem{1, 0, []byte("0000000000000001")},
			entity.BatchItem{2, 0, []byte("0000000000000002")},
			entity.BatchItem{3, 0, []byte("0000000000000003")},
		}
		// Lock Items
		res, err := repo.Lock(items)
		require.Nil(t, err)
		assert.Len(t, res, 4)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_LOCKED, res[i])
		}

		clk.Add(time.Second)

		// Lock Items again fails
		res, err = repo.Lock(items)
		require.Nil(t, err)
		assert.Len(t, res, 4)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_BUSY, res[i])
		}

		clk.Add(time.Second)

		// Lock Items again fails
		res, err = repo.Lock(items)
		require.Nil(t, err)
		assert.Len(t, res, 4)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_BUSY, res[i])
		}

		// Commit succeeeds
		res, err = repo.Commit(items)
		require.Nil(t, err)
		assert.Len(t, res, 4)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[i])
		}
		// Double Commit succeeeds
		res, err = repo.Commit(items)
		require.Nil(t, err)
		assert.Len(t, res, 4)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[i])
		}
	})
	t.Run("LockRollbackCommit", func(t *testing.T) {
		items := entity.Batch{
			entity.BatchItem{0, 0, []byte("0000000000000010")},
			entity.BatchItem{1, 0, []byte("0000000000000011")},
			entity.BatchItem{2, 0, []byte("0000000000000012")},
			entity.BatchItem{3, 0, []byte("0000000000000013")},
		}
		// Lock Items
		res, err := repo.Lock(items)
		require.Nil(t, err)
		assert.Len(t, res, 4)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_LOCKED, res[i])
		}
		// Rollback Items
		res, err = repo.Rollback(items)
		require.Nil(t, err)
		assert.Len(t, res, 4)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_OPEN, res[i])
		}
		// Commit succeeeds
		res, err = repo.Commit(items)
		require.Nil(t, err)
		assert.Len(t, res, 4)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[i])
		}
	})
	// Commit without prior lock succeeds
	// Double commit will not indicate whether item already existed
	//
	t.Run("BlindCommit", func(t *testing.T) {
		items := entity.Batch{
			entity.BatchItem{0, 0, []byte("0000000000000020")},
			entity.BatchItem{1, 0, []byte("0000000000000021")},
			entity.BatchItem{2, 0, []byte("0000000000000022")},
			entity.BatchItem{3, 0, []byte("0000000000000023")},
		}
		// Commit succeeeds
		res, err := repo.Commit(items)
		require.Nil(t, err)
		assert.Len(t, res, 4)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[i])
		}
	})
	t.Run("BlindRollback", func(t *testing.T) {
		items := entity.Batch{
			entity.BatchItem{0, 0, []byte("0000000000000030")},
			entity.BatchItem{1, 0, []byte("0000000000000031")},
			entity.BatchItem{2, 0, []byte("0000000000000032")},
			entity.BatchItem{3, 0, []byte("0000000000000033")},
		}
		// Rollback Items
		res, err := repo.Rollback(items)
		require.Nil(t, err)
		assert.Len(t, res, 4)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_OPEN, res[i])
		}
	})
	t.Run("LockExisting", func(t *testing.T) {
		items := entity.Batch{
			entity.BatchItem{0, 0, []byte("0000000000000040")},
			entity.BatchItem{1, 0, []byte("0000000000000041")},
			entity.BatchItem{2, 0, []byte("0000000000000042")},
			entity.BatchItem{3, 0, []byte("0000000000000043")},
		}
		// Commit succeeeds
		res, err := repo.Commit(items)
		require.Nil(t, err)
		assert.Len(t, res, 4)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[i])
		}
		// Lock Items
		res, err = repo.Lock(items)
		require.Nil(t, err)
		assert.Len(t, res, 4)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[i])
		}
	})
	t.Run("RollbackExisting", func(t *testing.T) {
		items := entity.Batch{
			entity.BatchItem{0, 0, []byte("0000000000000050")},
			entity.BatchItem{1, 0, []byte("0000000000000051")},
			entity.BatchItem{2, 0, []byte("0000000000000052")},
			entity.BatchItem{3, 0, []byte("0000000000000053")},
		}
		// Commit succeeeds
		res, err := repo.Commit(items)
		require.Nil(t, err)
		assert.Len(t, res, 4)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[i])
		}
		// Rollback Items
		res, err = repo.Rollback(items)
		require.Nil(t, err)
		assert.Len(t, res, 4)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[i])
		}
	})
	t.Run("Duplicates", func(t *testing.T) {
		items := entity.Batch{
			entity.BatchItem{0, 0, []byte("0000000000000060")},
			entity.BatchItem{1, 0, []byte("0000000000000060")},
			entity.BatchItem{2, 0, []byte("0000000000000060")},
			entity.BatchItem{3, 0, []byte("0000000000000060")},
		}
		// Lock Items
		res, err := repo.Lock(items)
		require.Nil(t, err)
		assert.Len(t, res, 4)
		// First item is locked, all duplicates are busy
		assert.Equal(t, entity.ITEM_LOCKED, res[0])
		for i := 1; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_BUSY, res[i])
		}
		// Rollback Items
		res, err = repo.Rollback(items)
		require.Nil(t, err)
		assert.Len(t, res, 4)
		// All rolled back duplicates should be open
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_OPEN, res[i])
		}
		// Commit Items
		res, err = repo.Lock(items)
		res, err = repo.Commit(items)
		require.Nil(t, err)
		assert.Len(t, res, 4)
		// All committed duplicates should exist
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[i])
		}
		// Double Commit Items
		res, err = repo.Commit(items)
		require.Nil(t, err)
		assert.Len(t, res, 4)
		// All double committed duplicates should exist
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[i])
		}
	})
	t.Run("EmptyBatch", func(t *testing.T) {
		items := entity.Batch{}
		// Lock Items
		res, err := repo.Lock(items)
		require.Nil(t, err)
		assert.Len(t, res, 0)
		// Rollback Items
		res, err = repo.Rollback(items)
		require.Nil(t, err)
		assert.Len(t, res, 0)
		// Commit succeeeds
		res, err = repo.Commit(items)
		require.Nil(t, err)
		assert.Len(t, res, 0)
	})
	t.Run("restoreHistory", func(t *testing.T) {
		err := repo.(*hash).restoreHistory(map[uint16]map[string][]byte{
			0: map[string][]byte{
				"00000000": []byte("0000000000000070" + "0000000000000071"),
				"00000001": []byte("0000000000000072" + "0000000000000073"),
			},
		}, fmt.Errorf("orig"))
		assert.Equal(t, "orig", err.Error())
	})
}

// SweepLocked
func TestHashSweepLocked(t *testing.T) {
	tmpdir := os.TempDir() + "/shackletest/lmdbsweeplocked"
	os.RemoveAll(tmpdir)
	os.MkdirAll(tmpdir, 0777)
	repo, err := NewHash(&config.RepoHash{
		PathIndex:      tmpdir,
		PathTimeseries: tmpdir,
		CacheSize:      1000,
	}, "vnode1", []uint16{0, 0x8000})
	require.Nil(t, err)
	var clk = clock.NewMock()
	var lockExp = 30 * time.Second
	repo.(*hash).clock = clk

	t.Run("SweepLocked", func(t *testing.T) {
		items := entity.Batch{
			entity.BatchItem{0,  0, []byte("0000000000000000")},
			entity.BatchItem{1,  0, []byte("0000000000000001")},
			entity.BatchItem{2,  0, []byte("0000000000000002")},
			entity.BatchItem{3,  0, []byte("0000000000000003")},
			entity.BatchItem{4,  0, []byte("0000000000000004")},
			entity.BatchItem{5,  0, []byte("0000000000000005")},
		}
		// Lock
		res, err := repo.Lock(items)
		require.Nil(t, err)
		_, err = repo.Rollback(items[:2])
		require.Nil(t, err)
		_, err = repo.Commit(items[2:4])
		require.Nil(t, err)
		clk.Add(lockExp + time.Second)
		_, err = repo.Lock(entity.Batch{
			entity.BatchItem{0, 0, []byte("0000000000000010")},
			entity.BatchItem{1, 0, []byte("0000000000000011")},
		})
		scanned, abandoned, err := repo.SweepLocked(clk.Now().Add(-1 * lockExp))
		require.Nil(t, err)
		assert.Equal(t, 4, scanned)
		assert.Equal(t, 2, abandoned)

		// Lock
		clk.Add(lockExp + time.Second)
		scanned, abandoned, err = repo.SweepLocked(clk.Now().Add(-1 * lockExp))
		require.Nil(t, err)
		assert.Equal(t, 2, scanned)
		assert.Equal(t, 2, abandoned)

		// Lock
		clk.Add(lockExp + time.Second)
		scanned, abandoned, err = repo.SweepLocked(clk.Now().Add(-1 * lockExp))
		require.Nil(t, err)
		assert.Equal(t, 0, scanned)
		assert.Equal(t, 0, abandoned)

		// Lock
		items = entity.Batch{
			entity.BatchItem{0, 0, []byte("0000000000000020")},
			entity.BatchItem{1, 0, []byte("0000000000000021")},
			entity.BatchItem{2, 0, []byte("0000000000000022")},
			entity.BatchItem{3, 0, []byte("0000000000000023")},
			entity.BatchItem{4, 0, []byte("0000000000000024")},
			entity.BatchItem{5, 0, []byte("0000000000000025")},
		}
		res, err = repo.Lock(items)
		require.Nil(t, err)
		assert.Len(t, res, 6)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_LOCKED, res[i])
		}
		_, err = repo.Rollback(items[:2])
		require.Nil(t, err)
		_, err = repo.Commit(items[2:4])
		require.Nil(t, err)
		clk.Add(lockExp + time.Second)
		scanned, abandoned, err = repo.SweepLocked(clk.Now().Add(-1 * lockExp))
		require.Nil(t, err)
		assert.Equal(t, 4, scanned)
		assert.Equal(t, 2, abandoned)

		clk.Add(time.Second)

		// Single key
		items = entity.Batch{
			entity.BatchItem{0, 0, []byte("0000000000000030")},
		}
		res, err = repo.Lock(items)
		require.Nil(t, err)
		assert.Len(t, res, 1)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_LOCKED, res[i])
		}
		clk.Add(lockExp + time.Second)
		scanned, abandoned, err = repo.SweepLocked(clk.Now().Add(-1 * lockExp))
		require.Nil(t, err)
		assert.Equal(t, 1, scanned)
		assert.Equal(t, 1, abandoned)

		clk.Add(time.Second)

		// Three single keys
		res, err = repo.Lock(entity.Batch{
			entity.BatchItem{0, 0, []byte("0000000000000040")},
		})
		require.Nil(t, err)
		assert.Len(t, res, 1)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_LOCKED, res[i])
		}

		clk.Add(time.Second)

		res, err = repo.Lock(entity.Batch{
			entity.BatchItem{0, 0, []byte("0000000000000041")},
		})
		require.Nil(t, err)
		assert.Len(t, res, 1)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_LOCKED, res[i])
		}

		clk.Add(time.Second)

		res, err = repo.Lock(entity.Batch{
			entity.BatchItem{0, 0, []byte("0000000000000042")},
		})
		require.Nil(t, err)
		assert.Len(t, res, 1)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_LOCKED, res[i])
		}

		clk.Add(lockExp + time.Second)

		scanned, abandoned, err = repo.SweepLocked(clk.Now().Add(-1 * lockExp))
		require.Nil(t, err)
		assert.Equal(t, 3, scanned)
		assert.Equal(t, 3, abandoned)

		clk.Add(time.Second)

		// Sweep batch of 1000
		// items = make(entity.Batch, 1000)
		// for i := 0; i < 1000; i++ {
		// items[i] = entity.BatchItem{i, []byte("BIGBATCH0000" + fmt.Sprintf("%04d", i))}
		// }
		// res, err = repo.Lock(items)
		// require.Nil(t, err)
		// assert.Len(t, res, 1000)
		// clk.Add(lockExp + time.Second)
		// scanned, abandoned, err = repo.SweepLocked(clk.Now().Add(-1*lockExp))
		// require.Nil(t, err)
		// assert.Equal(t, 1000, scanned)
		// assert.Equal(t, 1000, abandoned)
	})

	repo.Close()
}

// SweepExpired
func TestHashSweepExpired(t *testing.T) {
	tmpdir := os.TempDir() + "/shackletest/lmdbsweepexpired"
	os.RemoveAll(tmpdir)
	os.MkdirAll(tmpdir, 0777)
	repo, err := NewHash(&config.RepoHash{
		PathIndex:      tmpdir,
		PathTimeseries: tmpdir,
		CacheSize:      1000,
	}, "vnode1", []uint16{0, 0x8000})
	require.Nil(t, err)
	var clk = clock.NewMock()
	var keyExp = 24 * time.Hour
	var res []int8
	repo.(*hash).clock = clk

	t.Run("SweepExpired", func(t *testing.T) {
		items := entity.Batch{
			entity.BatchItem{0, 0, []byte("0000000000000000")},
			entity.BatchItem{1, 0, []byte("0000000000000001")},
			entity.BatchItem{2, 0, []byte("0000000000000002")},
			entity.BatchItem{3, 0, []byte("0000000000000003")},
			entity.BatchItem{4, 0, []byte("0000000000000004")},
			entity.BatchItem{5, 0, []byte("0000000000000005")},
		}
		// Lock, commit, and roll back some items
		_, err := repo.Lock(items)
		require.Nil(t, err)
		_, err = repo.Rollback(items[:2])
		require.Nil(t, err)
		_, err = repo.Commit(items[2:4])
		require.Nil(t, err)
		// Increment time and commit some more
		clk.Add(keyExp + time.Second)
		_, err = repo.Commit(entity.Batch{
			entity.BatchItem{0, 0, []byte("0000000000000010")},
			entity.BatchItem{1, 0, []byte("0000000000000011")},
		})
		// Sweep
		maxAge, notFound, deleted, err := repo.SweepExpired(clk.Now().Add(-1*keyExp), 0)
		require.Nil(t, err)
		assert.Equal(t, 2, notFound) // Lock sweep was not run to catch these abandonned locks
		assert.Equal(t, 4, deleted)

		// Wait and sweep again
		clk.Add(keyExp + time.Second)
		maxAge, notFound, deleted, err = repo.SweepExpired(clk.Now().Add(-1*keyExp), 0)
		assert.Nil(t, err)
		assert.Equal(t, 0, notFound)
		assert.Equal(t, 2, deleted)

		clk.Add(time.Second)

		// Lock
		_, err = repo.Commit(entity.Batch{
			entity.BatchItem{0, 0, []byte("0000000000000020")},
			entity.BatchItem{1, 0, []byte("0000000000000021")},
		})
		clk.Add(time.Second)
		_, err = repo.Commit(entity.Batch{
			entity.BatchItem{0, 0, []byte("0000000000000030")},
			entity.BatchItem{1, 0, []byte("0000000000000031")},
		})
		clk.Add(time.Second)
		_, err = repo.Commit(entity.Batch{
			entity.BatchItem{0, 0, []byte("0000000000000040")},
			entity.BatchItem{1, 0, []byte("0000000000000041")},
		})
		clk.Add(keyExp + time.Second)
		maxAge, notFound, deleted, err = repo.SweepExpired(clk.Now().Add(-1*keyExp), 2)
		require.Nil(t, err)
		assert.Equal(t, 0, notFound)
		assert.Equal(t, 2, deleted)
		maxAge, notFound, deleted, err = repo.SweepExpired(clk.Now().Add(-1*keyExp), 2)
		require.Nil(t, err)
		assert.Equal(t, 0, notFound)
		assert.Equal(t, 2, deleted)
		maxAge, notFound, deleted, err = repo.SweepExpired(clk.Now().Add(-1*keyExp), 2)
		require.Nil(t, err)
		assert.Equal(t, 0, notFound)
		assert.Equal(t, 2, deleted)
		maxAge, notFound, deleted, err = repo.SweepExpired(clk.Now().Add(-1*keyExp), 2)
		require.Nil(t, err)
		assert.Equal(t, 0, notFound)
		assert.Equal(t, 0, deleted)
		_ = maxAge

		clk.Add(time.Second)

		// Three single keys
		res, err = repo.Commit(entity.Batch{
			entity.BatchItem{0, 0, []byte("0000000000000050")},
		})
		require.Nil(t, err)
		assert.Len(t, res, 1)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[i])
		}

		clk.Add(time.Second)

		res, err = repo.Commit(entity.Batch{
			entity.BatchItem{0, 0, []byte("0000000000000051")},
		})
		require.Nil(t, err)
		assert.Len(t, res, 1)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[i])
		}

		clk.Add(time.Second)

		res, err = repo.Commit(entity.Batch{
			entity.BatchItem{0, 0, []byte("0000000000000052")},
		})
		require.Nil(t, err)
		assert.Len(t, res, 1)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[i])
		}

		clk.Add(keyExp + time.Second)

		maxAge, notFound, deleted, err = repo.SweepExpired(clk.Now().Add(-1*keyExp), 10)
		require.Nil(t, err)
		assert.Equal(t, 0, notFound)
		assert.Equal(t, 3, deleted)

		clk.Add(time.Second)

		// Sweep batch of 1000
		items = make(entity.Batch, 1000)
		for i := 0; i < 1000; i++ {
			items[i] = entity.BatchItem{0, 0, []byte("BIGBATCH0000" + fmt.Sprintf("%04d", i))}
		}
		res, err = repo.Commit(items)
		require.Nil(t, err)
		assert.Len(t, res, 1000)
		clk.Add(keyExp + time.Second)
		maxAge, notFound, deleted, err = repo.SweepExpired(clk.Now().Add(-1*keyExp), 2000)
		require.Nil(t, err)
		assert.Equal(t, 0, notFound)
		assert.Equal(t, 1000, deleted)
		_ = maxAge
	})

	repo.Close()
}

// HashClose
func TestHashClose(t *testing.T) {
	tmpdir := os.TempDir() + "/shackletest/lmdbclose"
	os.RemoveAll(tmpdir)
	os.MkdirAll(tmpdir, 0777)
	repo, err := NewHash(&config.RepoHash{
		PathIndex:      tmpdir,
		PathTimeseries: tmpdir,
		CacheSize:      1000,
	}, "vnode1", []uint16{0, 0x8000})
	require.Nil(t, err)
	repo.Close()
}
