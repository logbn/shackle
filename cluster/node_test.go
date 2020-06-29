package cluster

import (
	"testing"

	// "github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"highvolume.io/shackle/config"
	"highvolume.io/shackle/entity"
	"highvolume.io/shackle/test/mock"
)

func TestNode(t *testing.T) {
	var (
		node            *node
		err             error
		logger          = &mock.Logger{}
		svcHash         = &mock.ServiceHash{}
		svcCoordination = &mock.ServiceCoordination{}
		svcPersistence  = &mock.ServicePersistence{}
		svcReplication  = &mock.ServiceReplication{}
		svcDelegation   = &mock.ServiceDelegation{}
	)
	initChan := make(chan entity.ClusterCatalog)
	t.Run("New", func(t *testing.T) {
		node, err = NewNode(
			config.App{
				Cluster: &config.Cluster{
					Node:      config.Node{ID: "test"},
					KeyLength: 16,
				},
			},
			logger,
			svcHash,
			svcCoordination,
			svcPersistence,
			svcReplication,
			svcDelegation,
			initChan,
		)
		require.Nil(t, err)
		require.NotNil(t, node)
	})
	t.Run("Start", func(t *testing.T) {
		node.Start()
	})
	t.Run("Init", func(t *testing.T) {
		assert.Equal(t, false, node.active)
		res, err := node.Lock(entity.Batch{
			{0, 0, []byte("TEST000000000000")},
			{1, 0, []byte("TEST000000000001")},
		})
		require.NotNil(t, err)
		require.Equal(t, 0, len(res))
		assert.Equal(t, "Starting up", err.Error())
		initChan <- entity.ClusterCatalog{}
		<-node.Active()
		assert.Equal(t, true, node.active)
		assert.Equal(t, 1, svcPersistence.Count("Init"))
	})
	t.Run("Lock", func(t *testing.T) {
		res, err := node.Lock(entity.Batch{
			{0, 0, []byte("TEST000000000000")},
			{1, 0, []byte("TEST000000000001")},
		})
		require.NotNil(t, err)
		require.Equal(t, 2, len(res))
		assert.Equal(t, entity.ITEM_ERROR, res[0])
		assert.Equal(t, entity.ITEM_ERROR, res[1])
		svcCoordination.FuncPlanDelegation = mock.ServiceCoordinationFuncPlanDelegationBasic
		svcCoordination.FuncPlanReplication = mock.ServiceCoordinationFuncPlanCoordinationBasic
		res, err = node.Lock(entity.Batch{
			{0, 0, []byte("TEST000000000002")},
			{1, 0, []byte("TEST000000000003")},
		})
		require.Nil(t, err)
		require.Equal(t, 2, len(res))
		assert.Equal(t, entity.ITEM_LOCKED, res[0])
		assert.Equal(t, entity.ITEM_LOCKED, res[1])
		res, err = node.Lock(entity.Batch{
			{0, 0, []byte("FASTFATAL0000004")},
			{1, 0, []byte("TEST000000000005")},
		})
		require.NotNil(t, err)
		require.Equal(t, 0, len(res))
	})
	t.Run("Commit", func(t *testing.T) {
		res, err := node.Commit(entity.Batch{
			{0, 0, []byte("DELEGATE00000010")},
			{1, 0, []byte("TEST000000000011")},
		})
		require.Nil(t, err)
		require.Equal(t, 2, len(res))
		assert.Equal(t, entity.ITEM_EXISTS, res[0])
		assert.Equal(t, entity.ITEM_EXISTS, res[1])
	})
	t.Run("Rollback", func(t *testing.T) {
		res, err := node.Rollback(entity.Batch{
			{0, 0x8000, []byte("DELEGATE00000020")},
			{1, 0x8000, []byte("DELEGATEERROR021")},
			{2, 0, []byte("TEST000000000022")},
			{3, 0, []byte("ERROR00000000023")},
		})
		require.Nil(t, err)
		require.Equal(t, 4, len(res))
		assert.Equal(t, entity.ITEM_OPEN, res[0])
		assert.Equal(t, entity.ITEM_ERROR, res[1])
		assert.Equal(t, entity.ITEM_OPEN, res[2])
		assert.Equal(t, entity.ITEM_ERROR, res[3])
	})
	t.Run("Failed Delegation", func(t *testing.T) {
		res, err := node.Lock(entity.Batch{
			{0, 0, []byte("DELEGATEFAIL0030")},
			{1, 0, []byte("TEST000000000031")},
		})
		require.Nil(t, err)
		require.Equal(t, 2, len(res))
		assert.Equal(t, entity.ITEM_ERROR, res[0])
		assert.Equal(t, entity.ITEM_LOCKED, res[1])
	})
	// Replication failure results in successful response
	// This will likely remain true, but other actions are required
	// Node should report availability disruption windows to leader
	// Node should queue availability disruption window reports if cluster has no leader
	// Replication service should implement a circuit breaker to eliminate replication attempts for downed peers
	t.Run("Failed Replication", func(t *testing.T) {
		res, err := node.Lock(entity.Batch{
			{0, 0, []byte("PROPAGATEFAIL040")},
			{1, 0, []byte("TEST000000000041")},
		})
		require.Nil(t, err)
		require.Equal(t, 2, len(res))
		assert.Equal(t, entity.ITEM_LOCKED, res[0])
		assert.Equal(t, entity.ITEM_LOCKED, res[1])
	})
	t.Run("Stop", func(t *testing.T) {
		node.Stop()
	})
}
