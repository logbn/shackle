package repo

import (
	"encoding/binary"
	"os"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"highvolume.io/shackle/internal/config"
	"highvolume.io/shackle/internal/entity"
)

// NewHash
func TestNewHash(t *testing.T) {
	fakedir := os.TempDir() + "/shackletest/nonexistant"
	realdir := os.TempDir() + "/shackletest/existant"
	os.RemoveAll(fakedir)
	os.MkdirAll(realdir, 0777)
	t.Run("NonExistantDatabase", func(t *testing.T) {
		_, err := NewHash(&config.Hash{
			IndexPath:      realdir,
			TimeseriesPath: fakedir,
			CacheSize:      1000,
			Partitions:     1,
		}, 0)
		require.True(t, os.IsNotExist(err))
		_, err = NewHash(&config.Hash{
			IndexPath:      fakedir,
			TimeseriesPath: realdir,
			CacheSize:      1000,
			Partitions:     1,
		}, 0)
		require.True(t, os.IsNotExist(err))
	})
	t.Run("Success", func(t *testing.T) {
		_, err := NewHash(&config.Hash{
			IndexPath:      realdir,
			TimeseriesPath: realdir,
			CacheSize:      1000,
			Partitions:     1,
		}, 0)
		require.Nil(t, err)
	})
}

// Hash
func TestHash(t *testing.T) {
	clk := clock.NewMock()
	tmpdir := os.TempDir() + "/shackletest/lmdb"
	os.RemoveAll(tmpdir)
	os.MkdirAll(tmpdir, 0777)
	repo, err := NewHash(&config.Hash{
		IndexPath:      tmpdir,
		TimeseriesPath: tmpdir,
		CacheSize:      1000,
		Partitions:     1,
	}, 0)
	require.Nil(t, err)
	repo.(*hash).clock = clk

	t.Run("LockCommit", func(t *testing.T) {
		items := entity.Batch{
			entity.BatchItem{0, []byte("0000000000000000")},
			entity.BatchItem{1, []byte("0000000000000001")},
			entity.BatchItem{2, []byte("0000000000000002")},
			entity.BatchItem{3, []byte("0000000000000003")},
		}
		// Lock Items
		res, err := repo.Lock(items)
		require.Nil(t, err)
		assert.Len(t, res, 4)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_LOCKED, res[i])
		}
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
			entity.BatchItem{0, []byte("0000000000000010")},
			entity.BatchItem{1, []byte("0000000000000011")},
			entity.BatchItem{2, []byte("0000000000000012")},
			entity.BatchItem{3, []byte("0000000000000013")},
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
			entity.BatchItem{0, []byte("0000000000000020")},
			entity.BatchItem{1, []byte("0000000000000021")},
			entity.BatchItem{2, []byte("0000000000000022")},
			entity.BatchItem{3, []byte("0000000000000023")},
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
			entity.BatchItem{0, []byte("0000000000000030")},
			entity.BatchItem{1, []byte("0000000000000031")},
			entity.BatchItem{2, []byte("0000000000000032")},
			entity.BatchItem{3, []byte("0000000000000033")},
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
			entity.BatchItem{0, []byte("0000000000000040")},
			entity.BatchItem{1, []byte("0000000000000041")},
			entity.BatchItem{2, []byte("0000000000000042")},
			entity.BatchItem{3, []byte("0000000000000043")},
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
			entity.BatchItem{0, []byte("0000000000000050")},
			entity.BatchItem{1, []byte("0000000000000051")},
			entity.BatchItem{2, []byte("0000000000000052")},
			entity.BatchItem{3, []byte("0000000000000053")},
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
			entity.BatchItem{0, []byte("0000000000000060")},
			entity.BatchItem{1, []byte("0000000000000060")},
			entity.BatchItem{2, []byte("0000000000000060")},
			entity.BatchItem{3, []byte("0000000000000060")},
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
}

// SweepLocked
func TestHashSweepLocked(t *testing.T) {
	tmpdir := os.TempDir() + "/shackletest/lmdbsweeplocked"
	os.RemoveAll(tmpdir)
	os.MkdirAll(tmpdir, 0777)
	repo, err := NewHash(&config.Hash{
		IndexPath:      tmpdir,
		TimeseriesPath: tmpdir,
		CacheSize:      1000,
		Partitions:     1,
		KeyLength:      16,
	}, 0)
	require.Nil(t, err)
	var clk = clock.NewMock()
	var lockexpts = make([]byte, 8)
	var lockExp = 30 * time.Second
	// var keyExp = 24 * time.Hour
	clk.Set(time.Now())
	repo.(*hash).clock = clk

	t.Run("SweepLocked", func(t *testing.T) {
		items := entity.Batch{
			entity.BatchItem{0, []byte("0000000000000000")},
			entity.BatchItem{1, []byte("0000000000000001")},
			entity.BatchItem{2, []byte("0000000000000002")},
			entity.BatchItem{3, []byte("0000000000000003")},
			entity.BatchItem{4, []byte("0000000000000004")},
			entity.BatchItem{5, []byte("0000000000000005")},
		}
		// Lock
		res, err := repo.Lock(items)
		require.Nil(t, err)
		_, err = repo.Rollback(items[:2])
		require.Nil(t, err)
		_, err = repo.Commit(items[2:4])
		require.Nil(t, err)
		clk.Add(lockExp + time.Second)
		binary.BigEndian.PutUint64(lockexpts, uint64(clk.Now().Add(-1*lockExp).UnixNano()))
		_, err = repo.Lock(entity.Batch{
			entity.BatchItem{0, []byte("0000000000000010")},
			entity.BatchItem{1, []byte("0000000000000011")},
		})
		scanned, abandoned, err := repo.SweepLocked(lockexpts)
		require.Nil(t, err)
		assert.Equal(t, 4, scanned)
		assert.Equal(t, 2, abandoned)

		// Lock
		clk.Add(lockExp + time.Second)
		binary.BigEndian.PutUint64(lockexpts, uint64(clk.Now().Add(-1*lockExp).UnixNano()))
		scanned, abandoned, err = repo.SweepLocked(lockexpts)
		require.Nil(t, err)
		assert.Equal(t, 2, scanned)
		assert.Equal(t, 2, abandoned)

		items = entity.Batch{
			entity.BatchItem{0, []byte("0000000000000020")},
			entity.BatchItem{1, []byte("0000000000000021")},
			entity.BatchItem{2, []byte("0000000000000022")},
			entity.BatchItem{3, []byte("0000000000000023")},
			entity.BatchItem{4, []byte("0000000000000024")},
			entity.BatchItem{5, []byte("0000000000000025")},
		}
		// Lock
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
		binary.BigEndian.PutUint64(lockexpts, uint64(clk.Now().Add(-1*lockExp).UnixNano()))
		scanned, abandoned, err = repo.SweepLocked(lockexpts)
		require.Nil(t, err)
		assert.Equal(t, 4, scanned)
		assert.Equal(t, 2, abandoned)
	})

	repo.Close()
}

// SweepExpired
func TestHashSweepExpired(t *testing.T) {
	tmpdir := os.TempDir() + "/shackletest/lmdbsweepexpired"
	os.RemoveAll(tmpdir)
	os.MkdirAll(tmpdir, 0777)
	repo, err := NewHash(&config.Hash{
		IndexPath:      tmpdir,
		TimeseriesPath: tmpdir,
		CacheSize:      1000,
		Partitions:     1,
		KeyLength:      16,
	}, 0)
	require.Nil(t, err)
	var clk = clock.NewMock()
	var keyExpts = make([]byte, 8)
	var keyExp = 24 * time.Hour
	clk.Set(time.Now())
	repo.(*hash).clock = clk

	t.Run("SweepExpired", func(t *testing.T) {
		items := entity.Batch{
			entity.BatchItem{0, []byte("0000000000000000")},
			entity.BatchItem{1, []byte("0000000000000001")},
			entity.BatchItem{2, []byte("0000000000000002")},
			entity.BatchItem{3, []byte("0000000000000003")},
			entity.BatchItem{4, []byte("0000000000000004")},
			entity.BatchItem{5, []byte("0000000000000005")},
		}
		// Lock
		_, err := repo.Lock(items)
		require.Nil(t, err)
		_, err = repo.Rollback(items[:2])
		require.Nil(t, err)
		_, err = repo.Commit(items[2:4])
		require.Nil(t, err)
		clk.Add(keyExp + time.Second)
		binary.BigEndian.PutUint64(keyExpts, uint64(clk.Now().Add(-1*keyExp).UnixNano()))
		_, err = repo.Commit(entity.Batch{
			entity.BatchItem{0, []byte("0000000000000010")},
			entity.BatchItem{1, []byte("0000000000000011")},
		})
		maxAge, deleted, err := repo.SweepExpired(keyExpts, 0)
		require.Nil(t, err)
		assert.Equal(t, 4, deleted)

		// Lock
		clk.Add(keyExp + time.Second)
		binary.BigEndian.PutUint64(keyExpts, uint64(clk.Now().Add(-1*keyExp).UnixNano()))
		maxAge, deleted, err = repo.SweepExpired(keyExpts, 0)
		require.Nil(t, err)
		assert.Equal(t, 2, deleted)

		// Lock
		_, err = repo.Commit(entity.Batch{
			entity.BatchItem{0, []byte("0000000000000020")},
			entity.BatchItem{1, []byte("0000000000000021")},
		})
		clk.Add(time.Second)
		_, err = repo.Commit(entity.Batch{
			entity.BatchItem{0, []byte("0000000000000030")},
			entity.BatchItem{1, []byte("0000000000000031")},
		})
		clk.Add(time.Second)
		_, err = repo.Commit(entity.Batch{
			entity.BatchItem{0, []byte("0000000000000040")},
			entity.BatchItem{1, []byte("0000000000000041")},
		})
		clk.Add(keyExp + time.Second)
		binary.BigEndian.PutUint64(keyExpts, uint64(clk.Now().Add(-1*keyExp).UnixNano()))
		maxAge, deleted, err = repo.SweepExpired(keyExpts, 2)
		require.Nil(t, err)
		assert.Equal(t, 2, deleted)
		maxAge, deleted, err = repo.SweepExpired(keyExpts, 2)
		require.Nil(t, err)
		assert.Equal(t, 2, deleted)
		maxAge, deleted, err = repo.SweepExpired(keyExpts, 2)
		require.Nil(t, err)
		assert.Equal(t, 2, deleted)
		maxAge, deleted, err = repo.SweepExpired(keyExpts, 2)
		require.Nil(t, err)
		assert.Equal(t, 0, deleted)
		_ = maxAge
	})

	repo.Close()
}

// HashClose
func TestHashClose(t *testing.T) {
	tmpdir := os.TempDir() + "/shackletest/lmdbclose"
	os.RemoveAll(tmpdir)
	os.MkdirAll(tmpdir, 0777)
	repo, err := NewHash(&config.Hash{
		IndexPath:      tmpdir,
		TimeseriesPath: tmpdir,
		CacheSize:      1000,
		Partitions:     1,
	}, 0)
	require.Nil(t, err)
	repo.Close()
}
