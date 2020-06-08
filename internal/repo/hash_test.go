package repo

import (
	"os"
	"testing"

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
			IndexPath: realdir,
			TimeseriesPath: fakedir,
			CacheSize: 1000,
			Partitions: 1,
		}, 0)
		require.True(t, os.IsNotExist(err))
		_, err = NewHash(&config.Hash{
			IndexPath: fakedir,
			TimeseriesPath: realdir,
			CacheSize: 1000,
			Partitions: 1,
		}, 0)
		require.True(t, os.IsNotExist(err))
	})
	t.Run("Success", func(t *testing.T) {
		_, err := NewHash(&config.Hash{
			IndexPath: realdir,
			TimeseriesPath: realdir,
			CacheSize: 1000,
			Partitions: 1,
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
		IndexPath: tmpdir,
		TimeseriesPath: tmpdir,
		CacheSize: 1000,
		Partitions: 1,
	}, 0)
	require.Nil(t, err)
	repo.clock = clk

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
	repo, err := NewHash(&config.Hash{
		IndexPath: tmpdir,
		TimeseriesPath: tmpdir,
		CacheSize: 1000,
		Partitions: 1,
	}, 0)
	require.Nil(t, err)
	repo.Close()
}