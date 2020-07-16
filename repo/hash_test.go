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
			PathIndex: fakedir,
			CacheSize: 1000,
		}, 4, 2)
		require.True(t, os.IsNotExist(err))
	})
	t.Run("Success", func(t *testing.T) {
		_, err := NewHash(&config.RepoHash{
			PathIndex: realdir,
			CacheSize: 1000,
		}, 4, 3)
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
		PathIndex: tmpdir,
		CacheSize: 1000,
	}, 4, 0)
	require.Nil(t, err)
	repo.(*hash).clock = clk

	t.Run("LockCommit", func(t *testing.T) {
		items := []entity.Batch{{
			entity.BatchItem{0, []byte("0000000000000000"), 0},
			entity.BatchItem{1, []byte("0000000000000001"), 0},
			entity.BatchItem{2, []byte("0000000000000002"), 0},
			entity.BatchItem{3, []byte("0000000000000003"), 0},
		}}
		// Lock Items
		res, err := repo.MultiExec([]uint8{entity.OP_LOCK}, items)
		require.Nil(t, err)
		assert.Len(t, res[0], 4)
		for i := 0; i < len(res[0]); i++ {
			assert.Equal(t, entity.ITEM_LOCKED, res[0][i])
		}

		clk.Add(time.Second)

		// Lock Items again fails
		res, err = repo.MultiExec([]uint8{entity.OP_LOCK}, items)
		require.Nil(t, err)
		assert.Len(t, res[0], 4)
		for i := 0; i < len(res[0]); i++ {
			assert.Equal(t, entity.ITEM_BUSY, res[0][i])
		}

		clk.Add(time.Second)

		// Lock Items again fails
		res, err = repo.MultiExec([]uint8{entity.OP_LOCK}, items)
		require.Nil(t, err)
		assert.Len(t, res[0], 4)
		for i := 0; i < len(res[0]); i++ {
			assert.Equal(t, entity.ITEM_BUSY, res[0][i])
		}

		// Commit succeeeds
		res, err = repo.MultiExec([]uint8{entity.OP_COMMIT}, items)
		require.Nil(t, err)
		assert.Len(t, res[0], 4)
		for i := 0; i < len(res[0]); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[0][i])
		}
		// Double Commit succeeeds
		res, err = repo.MultiExec([]uint8{entity.OP_COMMIT}, items)
		require.Nil(t, err)
		assert.Len(t, res[0], 4)
		for i := 0; i < len(res[0]); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[0][i])
		}
	})
	t.Run("LockRollbackCommit", func(t *testing.T) {
		items := []entity.Batch{{
			entity.BatchItem{0, []byte("0000000000000010"), 0},
			entity.BatchItem{1, []byte("0000000000000011"), 0},
			entity.BatchItem{2, []byte("0000000000000012"), 0},
			entity.BatchItem{3, []byte("0000000000000013"), 0},
		}}
		// Lock Items
		res, err := repo.MultiExec([]uint8{entity.OP_LOCK}, items)
		require.Nil(t, err)
		assert.Len(t, res[0], 4)
		for i := 0; i < len(res[0]); i++ {
			assert.Equal(t, entity.ITEM_LOCKED, res[0][i])
		}
		// Rollback Items
		res, err = repo.MultiExec([]uint8{entity.OP_ROLLBACK}, items)
		require.Nil(t, err)
		assert.Len(t, res[0], 4)
		for i := 0; i < len(res[0]); i++ {
			assert.Equal(t, entity.ITEM_OPEN, res[0][i])
		}
		// Commit succeeeds
		res, err = repo.MultiExec([]uint8{entity.OP_COMMIT}, items)
		require.Nil(t, err)
		assert.Len(t, res[0], 4)
		for i := 0; i < len(res[0]); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[0][i])
		}
	})
	// Commit without prior lock succeeds
	// Double commit will not indicate whether item already existed
	//
	t.Run("BlindCommit", func(t *testing.T) {
		items := []entity.Batch{{
			entity.BatchItem{0, []byte("0000000000000020"), 0},
			entity.BatchItem{1, []byte("0000000000000021"), 0},
			entity.BatchItem{2, []byte("0000000000000022"), 0},
			entity.BatchItem{3, []byte("0000000000000023"), 0},
		}}
		// Commit succeeeds
		res, err := repo.MultiExec([]uint8{entity.OP_COMMIT}, items)
		require.Nil(t, err)
		assert.Len(t, res[0], 4)
		for i := 0; i < len(res[0]); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[0][i])
		}
	})
	t.Run("BlindRollback", func(t *testing.T) {
		items := []entity.Batch{{
			entity.BatchItem{0, []byte("0000000000000030"), 0},
			entity.BatchItem{1, []byte("0000000000000031"), 0},
			entity.BatchItem{2, []byte("0000000000000032"), 0},
			entity.BatchItem{3, []byte("0000000000000033"), 0},
		}}
		// Rollback Items
		res, err := repo.MultiExec([]uint8{entity.OP_ROLLBACK}, items)
		require.Nil(t, err)
		assert.Len(t, res[0], 4)
		for i := 0; i < len(res[0]); i++ {
			assert.Equal(t, entity.ITEM_OPEN, res[0][i])
		}
	})
	t.Run("LockExisting", func(t *testing.T) {
		items := []entity.Batch{{
			entity.BatchItem{0, []byte("0000000000000040"), 0},
			entity.BatchItem{1, []byte("0000000000000041"), 0},
			entity.BatchItem{2, []byte("0000000000000042"), 0},
			entity.BatchItem{3, []byte("0000000000000043"), 0},
		}}
		// Commit succeeeds
		res, err := repo.MultiExec([]uint8{entity.OP_COMMIT}, items)
		require.Nil(t, err)
		assert.Len(t, res[0], 4)
		for i := 0; i < len(res[0]); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[0][i])
		}
		// Lock Items
		res, err = repo.MultiExec([]uint8{entity.OP_LOCK}, items)
		require.Nil(t, err)
		assert.Len(t, res[0], 4)
		for i := 0; i < len(res[0]); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[0][i])
		}
	})
	t.Run("RollbackExisting", func(t *testing.T) {
		items := []entity.Batch{{
			entity.BatchItem{0, []byte("0000000000000050"), 0},
			entity.BatchItem{1, []byte("0000000000000051"), 0},
			entity.BatchItem{2, []byte("0000000000000052"), 0},
			entity.BatchItem{3, []byte("0000000000000053"), 0},
		}}
		// Commit succeeeds
		res, err := repo.MultiExec([]uint8{entity.OP_COMMIT}, items)
		require.Nil(t, err)
		assert.Len(t, res[0], 4)
		for i := 0; i < len(res[0]); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[0][i])
		}
		// Rollback Items
		res, err = repo.MultiExec([]uint8{entity.OP_ROLLBACK}, items)
		require.Nil(t, err)
		assert.Len(t, res[0], 4)
		for i := 0; i < len(res[0]); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[0][i])
		}
	})
	t.Run("Duplicates", func(t *testing.T) {
		items := []entity.Batch{{
			entity.BatchItem{0, []byte("0000000000000060"), 0},
			entity.BatchItem{1, []byte("0000000000000060"), 0},
			entity.BatchItem{2, []byte("0000000000000060"), 0},
			entity.BatchItem{3, []byte("0000000000000060"), 0},
		}}
		// Lock Items
		res, err := repo.MultiExec([]uint8{entity.OP_LOCK}, items)
		require.Nil(t, err)
		assert.Len(t, res[0], 4)
		// First item is locked, all duplicates are busy
		assert.Equal(t, entity.ITEM_LOCKED, res[0][0])
		for i := 1; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_BUSY, res[0][i])
		}
		// Rollback Items
		res, err = repo.MultiExec([]uint8{entity.OP_ROLLBACK}, items)
		require.Nil(t, err)
		assert.Len(t, res[0], 4)
		// All rolled back duplicates should be open
		for i := 0; i < len(res[0]); i++ {
			assert.Equal(t, entity.ITEM_OPEN, res[0][i])
		}
		// Commit Items
		res, err = repo.MultiExec([]uint8{entity.OP_LOCK}, items)
		res, err = repo.MultiExec([]uint8{entity.OP_COMMIT}, items)
		require.Nil(t, err)
		assert.Len(t, res[0], 4)
		// All committed duplicates should exist
		for i := 0; i < len(res[0]); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[0][i])
		}
		// Double Commit Items
		res, err = repo.MultiExec([]uint8{entity.OP_COMMIT}, items)
		require.Nil(t, err)
		assert.Len(t, res[0], 4)
		// All double committed duplicates should exist
		for i := 0; i < len(res[0]); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[0][i])
		}
	})
	t.Run("EmptyBatch", func(t *testing.T) {
		items := []entity.Batch{}
		// Lock Items
		res, err := repo.MultiExec([]uint8{entity.OP_LOCK}, items)
		require.Nil(t, err)
		assert.Len(t, res, 0)
		// Rollback Items
		res, err = repo.MultiExec([]uint8{entity.OP_ROLLBACK}, items)
		require.Nil(t, err)
		assert.Len(t, res, 0)
		// Commit succeeeds
		res, err = repo.MultiExec([]uint8{entity.OP_COMMIT}, items)
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
		PathIndex: tmpdir,
		CacheSize: 1000,
	}, 4, 7)
	require.Nil(t, err)
	repo.Close()
}
