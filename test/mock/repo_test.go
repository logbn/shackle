package mock

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"highvolume.io/shackle/config"
	"highvolume.io/shackle/entity"
)

func TestRepoFactoryhash(t *testing.T) {
	repo, err := RepoFactoryhash(&config.RepoHash{}, 1)
	require.Nil(t, err)
	require.NotNil(t, repo)
	batch1 := entity.Batch{
		entity.BatchItem{0, []byte("ERROR00000"), 0},
		entity.BatchItem{1, []byte("OPEN000000"), 0},
		entity.BatchItem{2, []byte("LOCKED0000"), 0},
		entity.BatchItem{3, []byte("BUSY000000"), 0},
		entity.BatchItem{4, []byte("EXISTS0000"), 0},
		entity.BatchItem{5, []byte("nonsense00"), 0},
	}
	batch2 := append(batch1, entity.Batch{
		entity.BatchItem{6, []byte("FATAL0000"), 0},
	}...)
	test := func(res []uint8) {
		assert.Equal(t, entity.ITEM_ERROR, res[0])
		assert.Equal(t, entity.ITEM_OPEN, res[1])
		assert.Equal(t, entity.ITEM_LOCKED, res[2])
		assert.Equal(t, entity.ITEM_BUSY, res[3])
		assert.Equal(t, entity.ITEM_EXISTS, res[4])
		assert.Equal(t, entity.ITEM_ERROR, res[5])
		assert.Equal(t, entity.ITEM_ERROR, res[6])
	}

	// Lock
	res, err := repo.Lock(batch1)
	require.Nil(t, err)
	require.Len(t, res, len(batch1))
	res, err = repo.Lock(batch2)
	require.NotNil(t, err)
	require.Len(t, res, len(batch2))
	test(res)

	// Rollback
	res, err = repo.Rollback(batch1)
	require.Nil(t, err)
	require.Len(t, res, len(batch1))
	res, err = repo.Rollback(batch2)
	require.NotNil(t, err)
	require.Len(t, res, len(batch2))
	test(res)

	// Commit
	res, err = repo.Commit(batch1)
	require.Nil(t, err)
	require.Len(t, res, len(batch1))
	res, err = repo.Commit(batch2)
	require.NotNil(t, err)
	require.Len(t, res, len(batch2))
	test(res)

	repo.Close()
	assert.Equal(t, repo.(*RepoHash).Closes, 1)
	repo.Close()
	assert.Equal(t, repo.(*RepoHash).Closes, 2)

}
