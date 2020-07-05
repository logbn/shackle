package service

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"highvolume.io/shackle/config"
	"highvolume.io/shackle/entity"
	"highvolume.io/shackle/repo"
	"highvolume.io/shackle/test/mock"
)

func TestNewPersistence(t *testing.T) {
	logger := &mock.Logger{}
	svc, err := NewPersistence(&config.App{
		Host: &config.Host{
			Partitions: 4,
		},
		Repo: config.Repo{
			Hash: &config.RepoHash{},
		},
	}, logger, func(cfg *config.RepoHash, id uint64) (r repo.Hash, err error) {
		return nil, fmt.Errorf("asdf")
	})
	require.Nil(t, err)
	require.NotNil(t, svc)
}

func TestPersistenceInit(t *testing.T) {
	logger := &mock.Logger{}
	svc, err := NewPersistence(&config.App{
		Host: &config.Host{
			Partitions: 4,
		},
		Repo: config.Repo{
			Hash: &config.RepoHash{},
		},
	}, logger, mock.RepoFactoryhash)
	require.Nil(t, err)
	require.NotNil(t, svc)
	manifest := &entity.Manifest{
		DeploymentID:     1,
		Status: entity.DEPLOYMENT_STATUS_INITIALIZING,
		Catalog: entity.Catalog{
			Partitions: 8,
			ReplicaCount: 2,
			WitnessCount: 1,
			Hosts: []entity.Host{
				{ID: 1, RaftAddr:"127.0.0.1:1001", Status: entity.HOST_STATUS_INITIALIZING},
				{ID: 2, RaftAddr:"127.0.0.2:1002", Status: entity.HOST_STATUS_INITIALIZING},
				{ID: 3, RaftAddr:"127.0.0.3:1003", Status: entity.HOST_STATUS_INITIALIZING},
			},
		},
	}
	manifest.Allocate()
	err = svc.Init(manifest.Catalog, 5)
	require.NotNil(t, err)
	err = svc.Init(manifest.Catalog, 1)
	require.Nil(t, err)
	svc, err = NewPersistence(&config.App{
		Host: &config.Host{
			Partitions: 4,
		},
		Repo: config.Repo{
			Hash: &config.RepoHash{
				PathIndex: "error",
			},
		},
	}, logger, mock.RepoFactoryhash)
	require.Nil(t, err)
	require.NotNil(t, svc)
	err = svc.Init(manifest.Catalog, 1)
	require.NotNil(t, err)

}

func TestPersistence(t *testing.T) {
	logger := &mock.Logger{}
	svc, err := NewPersistence(&config.App{
		Host: &config.Host{
			Partitions: 4,
		},
		Repo: config.Repo{
			Hash: &config.RepoHash{},
		},
	}, logger, mock.RepoFactoryhash)
	require.Nil(t, err)
	require.NotNil(t, svc)
	manifest := &entity.Manifest{
		DeploymentID:     1,
		Status: entity.DEPLOYMENT_STATUS_INITIALIZING,
		Catalog: entity.Catalog{
			Partitions: 4,
			ReplicaCount: 3,
			Hosts: []entity.Host{
				{ID: 1, RaftAddr:"127.0.0.1:1001", Status: entity.HOST_STATUS_INITIALIZING},
				{ID: 2, RaftAddr:"127.0.0.2:1002", Status: entity.HOST_STATUS_INITIALIZING},
				{ID: 3, RaftAddr:"127.0.0.3:1003", Status: entity.HOST_STATUS_INITIALIZING},
			},
		},
	}
	manifest.Allocate()
	svc.Init(manifest.Catalog, 2)

	assert.Equal(t, 4, len(svc.repos))

	t.Run("Lock", func(t *testing.T) {
		svc.log = &mock.Logger{}
		items := entity.Batch{
			entity.BatchItem{0, 0, []byte("LOCKED-000000000")},
			entity.BatchItem{1, 0, []byte("LOCKED-000000001")},
			entity.BatchItem{2, 0, []byte("LOCKED-000000002")},
			entity.BatchItem{3, 0, []byte("LOCKED-000000003")},
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
			entity.BatchItem{0, 0x0000000000000000, []byte("0LOCKED-00000010")},
			entity.BatchItem{1, 0x4000000000000000, []byte("1ERROR-000000011")},
			entity.BatchItem{2, 0x8000000000000000, []byte("2FATAL-000000012")},
			entity.BatchItem{3, 0xb000000000000000, []byte("3FATAL-000000013")},
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
			entity.BatchItem{0, 0, []byte("EXISTS-000000020")},
			entity.BatchItem{1, 0, []byte("EXISTS-000000021")},
			entity.BatchItem{2, 0, []byte("EXISTS-000000022")},
			entity.BatchItem{3, 0, []byte("EXISTS-000000023")},
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
			entity.BatchItem{0, 0x0000000000000000, []byte("0EXISTS-00000030")},
			entity.BatchItem{1, 0x4000000000000000, []byte("1ERROR-000000031")},
			entity.BatchItem{2, 0x8000000000000000, []byte("2FATAL-000000032")},
			entity.BatchItem{3, 0xb000000000000000, []byte("3FATAL-000000033")},
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
			entity.BatchItem{0, 0, []byte("EXISTS-000000040")},
			entity.BatchItem{1, 0, []byte("EXISTS-000000041")},
			entity.BatchItem{2, 0, []byte("EXISTS-000000042")},
			entity.BatchItem{3, 0, []byte("EXISTS-000000043")},
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
			entity.BatchItem{0, 0x0000000000000000, []byte("0EXISTS-00000050")},
			entity.BatchItem{1, 0x4000000000000000, []byte("1ERROR-000000051")},
			entity.BatchItem{2, 0x8000000000000000, []byte("2FATAL-000000052")},
			entity.BatchItem{3, 0xb000000000000000, []byte("3FATAL-000000053")},
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

	svc.Stop()
	for _, repo := range svc.repos {
		assert.Equal(t, 1, repo.(*mock.RepoHash).Closes)
	}
}

func TestPersistenceSweep(t *testing.T) {
	var sweepExpiredCalls int
	var sweepLockedCalls int
	var mutex sync.Mutex
	var expiredErr error
	var lockedErr error
	logger := &mock.Logger{}
	svc, err := NewPersistence(&config.App{
		Host: &config.Host{
			Partitions: 4,
		},
		Repo: config.Repo{
			Hash: &config.RepoHash{
				SweepInterval:  time.Second,
				KeyExpiration:  10 * time.Second,
				LockExpiration: 10 * time.Second,
			},
		},
	}, logger, func(cfg *config.RepoHash, id uint64) (r repo.Hash, err error) {
		r = &mock.RepoHash{
			SweepExpiredFunc: func(exp time.Time, limit int) (maxAge time.Duration, notFound, deleted int, err error) {
				mutex.Lock()
				defer mutex.Unlock()
				sweepExpiredCalls++
				err = expiredErr
				return
			},
			SweepLockedFunc: func(exp time.Time) (total int, deleted int, err error) {
				mutex.Lock()
				defer mutex.Unlock()
				sweepLockedCalls++
				err = lockedErr
				return
			},
		}
		return
	})
	require.Nil(t, err)
	require.NotNil(t, svc)
	manifest := &entity.Manifest{
		DeploymentID:     1,
		Status: entity.DEPLOYMENT_STATUS_INITIALIZING,
		Catalog: entity.Catalog{
			Partitions: 4,
			ReplicaCount: 3,
			Hosts: []entity.Host{
				{ID: 1, RaftAddr:"127.0.0.1:1001", Status: entity.HOST_STATUS_INITIALIZING},
				{ID: 2, RaftAddr:"127.0.0.2:1002", Status: entity.HOST_STATUS_INITIALIZING},
				{ID: 3, RaftAddr:"127.0.0.3:1003", Status: entity.HOST_STATUS_INITIALIZING},
			},
		},
	}
	manifest.Allocate()
	svc.Init(manifest.Catalog, 1)
	assert.Equal(t, 4, len(svc.repos))
	clk := clock.NewMock()
	svc.clock = clk

	svc.Stop()
}
