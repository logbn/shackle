package repo

import (
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
		}, 1)
		require.True(t, os.IsNotExist(err))
		_, err = NewHash(&config.RepoHash{
			PathIndex:      fakedir,
			PathTimeseries: realdir,
			CacheSize:      1000,
		}, 2)
		require.True(t, os.IsNotExist(err))
	})
	t.Run("Success", func(t *testing.T) {
		_, err := NewHash(&config.RepoHash{
			PathIndex:      realdir,
			PathTimeseries: realdir,
			CacheSize:      1000,
		}, 3)
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
	}, 4)
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
	}, 7)
	require.Nil(t, err)
	repo.Close()
}
