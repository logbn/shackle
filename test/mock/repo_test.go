package mock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"highvolume.io/shackle/config"
	"highvolume.io/shackle/entity"
)

func TestRepoFactoryhash(t *testing.T) {
	repo, err := RepoFactoryhash(&config.RepoHash{}, 0)
	require.Nil(t, err)
	require.NotNil(t, repo)
	batch1 := entity.Batch{
		entity.BatchItem{0, []byte("EXISTS")},
		entity.BatchItem{1, []byte("LOCKED")},
		entity.BatchItem{2, []byte("BUSY")},
		entity.BatchItem{3, []byte("ERROR")},
		entity.BatchItem{4, []byte("nonsense")},
	}
	batch2 := append(batch1, entity.Batch{
		entity.BatchItem{5, []byte("FATAL")},
	}...)
	test := func(res []int8) {
		assert.Equal(t, entity.ITEM_EXISTS, res[0])
		assert.Equal(t, entity.ITEM_LOCKED, res[1])
		assert.Equal(t, entity.ITEM_BUSY, res[2])
		assert.Equal(t, entity.ITEM_ERROR, res[3])
		assert.Equal(t, entity.ITEM_OPEN, res[4])
		assert.Equal(t, entity.ITEM_OPEN, res[5])
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

	// Sweep
	m, nf, d, err := repo.SweepExpired(time.Now(), 11)
	require.Nil(t, err)
	assert.Equal(t, 0, int(m))
	assert.Equal(t, 0, nf)
	assert.Equal(t, 0, d)
	tt, d, err := repo.SweepLocked(time.Now())
	require.Nil(t, err)
	assert.Equal(t, 0, tt)
	assert.Equal(t, 0, d)
	repo.(*RepoHash).SweepExpiredFunc = func(exp time.Time, limit int) (maxAge time.Duration, notFound, deleted int, err error) {
		return time.Duration(0), 0, limit, nil
	}
	repo.(*RepoHash).SweepLockedFunc = func(exp time.Time) (total int, deleted int, err error) {
		return 0, 8, nil
	}
	m, nf, d, err = repo.SweepExpired(time.Now(), 11)
	require.Nil(t, err)
	assert.Equal(t, 0, nf)
	assert.Equal(t, 11, d)
	tt, d, err = repo.SweepLocked(time.Now())
	require.Nil(t, err)
	assert.Equal(t, 8, d)

}
