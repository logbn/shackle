package service

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"logbin.io/shackle/config"
	"logbin.io/shackle/entity"
	"logbin.io/shackle/repo"
	"logbin.io/shackle/test/mock"
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
	}, logger, func(cfg *config.RepoHash, p, id uint16) (r repo.Hash, err error) {
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
		DeploymentID: 1,
		Status:       entity.DEPLOYMENT_STATUS_INITIALIZING,
		Catalog: entity.Catalog{
			Partitions:   8,
			ReplicaCount: 2,
			WitnessCount: 1,
			Hosts: []entity.Host{
				{ID: 1, RaftAddr: "127.0.0.1:1001", Status: entity.HOST_STATUS_INITIALIZING},
				{ID: 2, RaftAddr: "127.0.0.2:1002", Status: entity.HOST_STATUS_INITIALIZING},
				{ID: 3, RaftAddr: "127.0.0.3:1003", Status: entity.HOST_STATUS_INITIALIZING},
			},
		},
	}
	cat, err := manifest.Allocate()
	require.Nil(t, err)
	err = svc.Init(*cat, 5)
	require.NotNil(t, err)
	err = svc.Init(*cat, 1)
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
	err = svc.Init(*cat, 1)
	require.Nil(t, err)
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
		DeploymentID: 1,
		Status:       entity.DEPLOYMENT_STATUS_INITIALIZING,
		Catalog: entity.Catalog{
			Partitions:   4,
			ReplicaCount: 3,
			Hosts: []entity.Host{
				{ID: 1, RaftAddr: "127.0.0.1:1001", Status: entity.HOST_STATUS_INITIALIZING},
				{ID: 2, RaftAddr: "127.0.0.2:1002", Status: entity.HOST_STATUS_INITIALIZING},
				{ID: 3, RaftAddr: "127.0.0.3:1003", Status: entity.HOST_STATUS_INITIALIZING},
			},
		},
	}
	cat, err := manifest.Allocate()
	require.Nil(t, err)
	svc.Init(*cat, 2)
	svc.InitRepo(0)

	assert.Equal(t, 1, len(svc.repos))

	t.Run("Lock", func(t *testing.T) {
		svc.log = &mock.Logger{}
		items := []entity.Batch{{
			entity.BatchItem{0, []byte("LOCKED-000000000"), 0},
			entity.BatchItem{1, []byte("LOCKED-000000001"), 0},
			entity.BatchItem{2, []byte("LOCKED-000000002"), 0},
			entity.BatchItem{3, []byte("LOCKED-000000003"), 0},
		}}
		// Lock Items
		res, err := svc.MultiExec(0, []uint8{entity.OP_LOCK}, items)
		require.Nil(t, err)
		assert.Len(t, res[0], 4)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_LOCKED, res[0][i])
		}
		assert.Len(t, svc.log.(*mock.Logger).Errors, 0)
	})
	t.Run("Lock Error", func(t *testing.T) {
		svc.log = &mock.Logger{}
		items := []entity.Batch{{
			entity.BatchItem{0, []byte("0LOCKED-00000010"), 0x0000},
			entity.BatchItem{1, []byte("1ERROR-000000011"), 0x4000},
			entity.BatchItem{2, []byte("2FATAL-000000012"), 0x8000},
			entity.BatchItem{3, []byte("3FATAL-000000013"), 0xb000},
		}}
		// Lock Items
		res, err := svc.MultiExec(0, []uint8{entity.OP_LOCK}, items)
		require.NotNil(t, err)
		assert.Equal(t, entity.ITEM_LOCKED, res[0][0])
		assert.Equal(t, entity.ITEM_ERROR, res[0][1])
		assert.Equal(t, entity.ITEM_ERROR, res[0][2])
		assert.Equal(t, entity.ITEM_ERROR, res[0][3])
		assert.Greater(t, len(svc.log.(*mock.Logger).Errors), 0)
	})
	t.Run("Commit", func(t *testing.T) {
		svc.log = &mock.Logger{}
		items := []entity.Batch{{
			entity.BatchItem{0, []byte("EXISTS-000000020"), 0},
			entity.BatchItem{1, []byte("EXISTS-000000021"), 0},
			entity.BatchItem{2, []byte("EXISTS-000000022"), 0},
			entity.BatchItem{3, []byte("EXISTS-000000023"), 0},
		}}
		// Commit Items
		res, err := svc.MultiExec(0, []uint8{entity.OP_COMMIT}, items)
		require.Nil(t, err)
		assert.Len(t, res[0], 4)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[0][i])
		}
		assert.Len(t, svc.log.(*mock.Logger).Errors, 0)
	})
	t.Run("Commit Error", func(t *testing.T) {
		svc.log = &mock.Logger{}
		items := []entity.Batch{{
			entity.BatchItem{0, []byte("0EXISTS-00000030"), 0x0000},
			entity.BatchItem{1, []byte("1ERROR-000000031"), 0x4000},
			entity.BatchItem{2, []byte("2FATAL-000000032"), 0x8000},
			entity.BatchItem{3, []byte("3FATAL-000000033"), 0xb000},
		}}
		// Commit Items
		res, err := svc.MultiExec(0, []uint8{entity.OP_COMMIT}, items)
		require.NotNil(t, err)
		assert.Equal(t, entity.ITEM_EXISTS, res[0][0])
		assert.Equal(t, entity.ITEM_ERROR, res[0][1])
		assert.Equal(t, entity.ITEM_ERROR, res[0][2])
		assert.Equal(t, entity.ITEM_ERROR, res[0][3])
		assert.Greater(t, len(svc.log.(*mock.Logger).Errors), 0)
	})
	t.Run("Rollback", func(t *testing.T) {
		svc.log = &mock.Logger{}
		items := []entity.Batch{{
			entity.BatchItem{0, []byte("EXISTS-000000040"), 0},
			entity.BatchItem{1, []byte("EXISTS-000000041"), 0},
			entity.BatchItem{2, []byte("EXISTS-000000042"), 0},
			entity.BatchItem{3, []byte("EXISTS-000000043"), 0},
		}}
		// Lock Items
		res, err := svc.MultiExec(0, []uint8{entity.OP_LOCK}, items)
		require.Nil(t, err)
		// Rollback Items
		res, err = svc.MultiExec(0, []uint8{entity.OP_ROLLBACK}, items)
		require.Nil(t, err)
		assert.Len(t, res[0], 4)
		for i := 0; i < len(res); i++ {
			assert.Equal(t, entity.ITEM_EXISTS, res[0][i])
		}
		assert.Len(t, svc.log.(*mock.Logger).Errors, 0)
	})
	t.Run("Rollback Error", func(t *testing.T) {
		svc.log = &mock.Logger{}
		items := []entity.Batch{{
			entity.BatchItem{0, []byte("0EXISTS-00000050"), 0x0000},
			entity.BatchItem{1, []byte("1ERROR-000000051"), 0x4000},
			entity.BatchItem{2, []byte("2FATAL-000000052"), 0x8000},
			entity.BatchItem{3, []byte("3FATAL-000000053"), 0xb000},
		}}
		// Lock Items
		res, err := svc.MultiExec(0, []uint8{entity.OP_LOCK}, items)
		require.NotNil(t, err)
		// Rollback Items
		res, err = svc.MultiExec(0, []uint8{entity.OP_ROLLBACK}, items)
		require.NotNil(t, err)
		assert.Equal(t, entity.ITEM_EXISTS, res[0][0])
		assert.Equal(t, entity.ITEM_ERROR, res[0][1])
		assert.Equal(t, entity.ITEM_ERROR, res[0][2])
		assert.Equal(t, entity.ITEM_ERROR, res[0][3])
		assert.Greater(t, len(svc.log.(*mock.Logger).Errors), 0)
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
	}, logger, func(cfg *config.RepoHash, p, id uint16) (r repo.Hash, err error) {
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
		DeploymentID: 1,
		Status:       entity.DEPLOYMENT_STATUS_INITIALIZING,
		Catalog: entity.Catalog{
			Partitions:   4,
			ReplicaCount: 3,
			Hosts: []entity.Host{
				{ID: 1, RaftAddr: "127.0.0.1:1001", Status: entity.HOST_STATUS_INITIALIZING},
				{ID: 2, RaftAddr: "127.0.0.2:1002", Status: entity.HOST_STATUS_INITIALIZING},
				{ID: 3, RaftAddr: "127.0.0.3:1003", Status: entity.HOST_STATUS_INITIALIZING},
			},
		},
	}
	cat, err := manifest.Allocate()
	require.Nil(t, err)
	svc.Init(*cat, 1)
	svc.InitRepo(0)
	assert.Equal(t, 1, len(svc.repos))
	clk := clock.NewMock()
	svc.clock = clk

	svc.Stop()
}
