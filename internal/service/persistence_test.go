package service

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"highvolume.io/shackle/internal/config"
	"highvolume.io/shackle/internal/entity"
	"highvolume.io/shackle/internal/repo"
	"highvolume.io/shackle/internal/test/mock"
)

func TestNewPersistence(t *testing.T) {
	logger := &mock.Logger{}
	svc, err := NewPersistence(&config.Hash{
		Partitions: 4,
	}, func(cfg *config.Hash, partition int) (r repo.Hash, err error) {
		return nil, fmt.Errorf("asdf")
	}, logger)
	require.NotNil(t, err)
	require.Nil(t, svc)
}

func TestPersistence(t *testing.T) {
	logger := &mock.Logger{}
	svc, err := NewPersistence(&config.Hash{
		Partitions: 4,
	}, mock.RepoFactoryhash, logger)
	require.Nil(t, err)
	require.NotNil(t, svc)
	assert.Equal(t, 4, len(svc.repos))

	t.Run("Lock", func(t *testing.T) {
		svc.log = &mock.Logger{}
		items := entity.Batch{
			entity.BatchItem{0, []byte("LOCKED-000000000")},
			entity.BatchItem{1, []byte("LOCKED-000000001")},
			entity.BatchItem{2, []byte("LOCKED-000000002")},
			entity.BatchItem{3, []byte("LOCKED-000000003")},
		}
		// Lock Items
		res, err := svc.Lock(items)
		require.Nil(t, err)
		assert.Len(t, res, 4)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_LOCKED, res[i])
		}
		assert.Len(t, svc.log.(*mock.Logger).Errors, 0)
	})
	t.Run("Lock Error", func(t *testing.T) {
		svc.log = &mock.Logger{}
		items := entity.Batch{
			entity.BatchItem{0, []byte("0LOCKED-00000010")},
			entity.BatchItem{1, []byte("1ERROR-000000011")},
			entity.BatchItem{2, []byte("2FATAL-000000012")},
			entity.BatchItem{3, []byte("3FATAL-000000013")},
		}
		// Lock Items
		res, err := svc.Lock(items)
		require.NotNil(t, err)
		assert.Equal(t, entity.ITEM_LOCKED, res[0])
		assert.Equal(t, entity.ITEM_ERROR, res[1])
		assert.Equal(t, entity.ITEM_ERROR, res[2])
		assert.Equal(t, entity.ITEM_ERROR, res[3])
		assert.Len(t, svc.log.(*mock.Logger).Errors, 2)
	})
	t.Run("Commit", func(t *testing.T) {
		svc.log = &mock.Logger{}
		items := entity.Batch{
			entity.BatchItem{0, []byte("EXISTS-000000020")},
			entity.BatchItem{1, []byte("EXISTS-000000021")},
			entity.BatchItem{2, []byte("EXISTS-000000022")},
			entity.BatchItem{3, []byte("EXISTS-000000023")},
		}
		// Commit Items
		res, err := svc.Commit(items)
		require.Nil(t, err)
		assert.Len(t, res, 4)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[i])
		}
		assert.Len(t, svc.log.(*mock.Logger).Errors, 0)
	})
	t.Run("Commit Error", func(t *testing.T) {
		svc.log = &mock.Logger{}
		items := entity.Batch{
			entity.BatchItem{0, []byte("0EXISTS-00000030")},
			entity.BatchItem{1, []byte("1ERROR-000000031")},
			entity.BatchItem{2, []byte("2FATAL-000000032")},
			entity.BatchItem{3, []byte("3FATAL-000000033")},
		}
		// Commit Items
		res, err := svc.Commit(items)
		require.NotNil(t, err)
		assert.Equal(t, entity.ITEM_EXISTS, res[0])
		assert.Equal(t, entity.ITEM_ERROR, res[1])
		assert.Equal(t, entity.ITEM_ERROR, res[2])
		assert.Equal(t, entity.ITEM_ERROR, res[3])
		assert.Len(t, svc.log.(*mock.Logger).Errors, 2)
	})
	t.Run("Rollback", func(t *testing.T) {
		svc.log = &mock.Logger{}
		items := entity.Batch{
			entity.BatchItem{0, []byte("EXISTS-000000040")},
			entity.BatchItem{1, []byte("EXISTS-000000041")},
			entity.BatchItem{2, []byte("EXISTS-000000042")},
			entity.BatchItem{3, []byte("EXISTS-000000043")},
		}
		// Lock Items
		res, err := svc.Lock(items)
		require.Nil(t, err)
		// Rollback Items
		res, err = svc.Rollback(items)
		require.Nil(t, err)
		assert.Len(t, res, 4)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[i])
		}
		assert.Len(t, svc.log.(*mock.Logger).Errors, 0)
	})
	t.Run("Rollback Error", func(t *testing.T) {
		svc.log = &mock.Logger{}
		items := entity.Batch{
			entity.BatchItem{0, []byte("0EXISTS-00000050")},
			entity.BatchItem{1, []byte("1ERROR-000000051")},
			entity.BatchItem{2, []byte("2FATAL-000000052")},
			entity.BatchItem{3, []byte("3FATAL-000000053")},
		}
		// Lock Items
		res, err := svc.Lock(items)
		require.NotNil(t, err)
		// Rollback Items
		res, err = svc.Rollback(items)
		require.NotNil(t, err)
		assert.Equal(t, entity.ITEM_EXISTS, res[0])
		assert.Equal(t, entity.ITEM_ERROR, res[1])
		assert.Equal(t, entity.ITEM_ERROR, res[2])
		assert.Equal(t, entity.ITEM_ERROR, res[3])
		assert.Len(t, svc.log.(*mock.Logger).Errors, 4)
	})

	svc.Close()
	for _, repo := range svc.repos {
		assert.Equal(t, 1, repo.(*mock.RepoHash).Closes)
	}
}
