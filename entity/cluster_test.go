package entity

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterAllocate(t *testing.T) {
	// Success
	manifest := makeEmptyManifest(3, 8)
	err := manifest.Allocate(64)
	require.Nil(t, err)
	stats := calcManifestStats(manifest)
	assert.Equal(t, 24, len(manifest.Catalog.VNodes))
	assert.Equal(t, 64, len(manifest.Catalog.Partitions))
	assert.Equal(t, 0, stats.masterlessVnodes)
	assert.Equal(t, 0, stats.replicalessVnodes)

	manifest = makeEmptyManifest(3, 8)
	err = manifest.Allocate(128)
	require.Nil(t, err)
	stats = calcManifestStats(manifest)
	assert.Equal(t, 24, len(manifest.Catalog.VNodes))
	assert.Equal(t, 128, len(manifest.Catalog.Partitions))
	assert.Equal(t, 0, stats.masterlessVnodes)
	assert.Equal(t, 0, stats.replicalessVnodes)

	manifest = makeEmptyManifest(3, 8)
	err = manifest.Allocate(16)
	require.Nil(t, err)
	stats = calcManifestStats(manifest)
	assert.Equal(t, 24, len(manifest.Catalog.VNodes))
	assert.Equal(t, 16, len(manifest.Catalog.Partitions))
	assert.Equal(t, 8, stats.masterlessVnodes)
	assert.Equal(t, 0, stats.replicalessVnodes)

	manifest = makeEmptyManifest(2, 8)
	err = manifest.Allocate(8)
	require.Nil(t, err)
	stats = calcManifestStats(manifest)
	assert.Equal(t, 24, len(manifest.Catalog.VNodes))
	assert.Equal(t, 8, len(manifest.Catalog.Partitions))
	assert.Equal(t, 16, stats.masterlessVnodes)
	assert.Equal(t, 16, stats.replicalessVnodes)
	assert.Equal(t, 1, len(manifest.Catalog.Partitions[0].Replicas))

	manifest = makeEmptyManifest(3, 8)
	err = manifest.Allocate(4096)
	require.Nil(t, err)
	stats = calcManifestStats(manifest)
	assert.Equal(t, 24, len(manifest.Catalog.VNodes))
	assert.Equal(t, 4096, len(manifest.Catalog.Partitions))
	assert.Equal(t, 0, stats.masterlessVnodes)
	assert.Equal(t, 0, stats.replicalessVnodes)

	// Partition count not power of 2
	manifest = makeEmptyManifest(3, 8)
	err = manifest.Allocate(555)
	require.NotNil(t, err)

	// Nodes empty
	manifest = makeEmptyManifest(3, 8)
	manifest.Catalog.Nodes = nil
	err = manifest.Allocate(256)
	require.NotNil(t, err)

	// VNodes not empty
	manifest = makeEmptyManifest(3, 8)
	manifest.Catalog.VNodes = []ClusterVNode{{}, {}}
	err = manifest.Allocate(256)
	require.NotNil(t, err)

	// Partitions not empty
	manifest = makeEmptyManifest(3, 8)
	manifest.Catalog.Partitions = []ClusterPartition{{}, {}}
	err = manifest.Allocate(256)
	require.NotNil(t, err)

	// Cluster not initializing
	manifest = makeEmptyManifest(3, 8)
	manifest.Status = CLUSTER_STATUS_ALLOCATING
	err = manifest.Allocate(256)
	require.NotNil(t, err)

	// Invalid vnode count
	manifest = makeEmptyManifest(3, 8)
	manifest.Catalog.Nodes[0].VNodeCount = 0
	err = manifest.Allocate(256)
	require.NotNil(t, err)

	// Replicas greater than nodes
	manifest = makeEmptyManifest(5, 8)
	err = manifest.Allocate(256)
	require.Nil(t, err)
	assert.Equal(t, 2, len(manifest.Catalog.Partitions[0].Replicas))

	// To/From Json
	manifest = makeEmptyManifest(3, 8)
	err = manifest.Allocate(256)
	data := manifest.ToJson()
	require.NotNil(t, data)
	assert.Greater(t, len(data), 1000)
	manifest = makeEmptyManifest(3, 8)
	err = manifest.FromJson(data)
	require.Nil(t, err)
	assert.Equal(t, 256, len(manifest.Catalog.Partitions))
}

func TestClusterStatus(t *testing.T) {
	manifest := makeEmptyManifest(2, 8)
	assert.Equal(t, true, manifest.ClusterInitializing())
	assert.Equal(t, false, manifest.ClusterAllocating())
	assert.Equal(t, false, manifest.ClusterActive())
	manifest.Status = CLUSTER_STATUS_ALLOCATING
	assert.Equal(t, false, manifest.ClusterInitializing())
	assert.Equal(t, true, manifest.ClusterAllocating())
	assert.Equal(t, false, manifest.ClusterActive())
	manifest.Status = CLUSTER_STATUS_ACTIVE
	assert.Equal(t, false, manifest.ClusterInitializing())
	assert.Equal(t, false, manifest.ClusterAllocating())
	assert.Equal(t, true, manifest.ClusterActive())
}

func TestClusterNodeStatus(t *testing.T) {
	manifest := makeEmptyManifest(2, 8)
	node := manifest.GetNodeByAddrRaft("nonsense")
	require.Nil(t, node)
	node = manifest.GetNodeByAddrRaft("localhost:1001")
	require.NotNil(t, node)
	assert.Equal(t, true, node.Initializing())
	node.Status = CLUSTER_NODE_STATUS_ALLOCATED
	assert.Equal(t, true, node.Allocated())
	node = manifest.GetNodeByID("nonsense")
	require.Nil(t, node)
	node = manifest.GetNodeByID("1")
	require.NotNil(t, node)
	assert.Equal(t, true, node.Allocated())
	node.Status = CLUSTER_NODE_STATUS_ACTIVE
	assert.Equal(t, true, node.Active())
	node.Status = CLUSTER_NODE_STATUS_DOWN
	assert.Equal(t, true, node.Down())
	assert.Equal(t, false, node.Active())
	node.Status = CLUSTER_NODE_STATUS_RECOVERING
	assert.Equal(t, true, node.Recovering())
	assert.Equal(t, false, node.Active())
}

func makeEmptyManifest(replicas, vnodeCount int) *ClusterManifest {
	return &ClusterManifest{
		ID:     "test",
		Status: CLUSTER_STATUS_INITIALIZING,
		Catalog: ClusterCatalog{
			Version:    "1.0.0",
			Replicas:   replicas,
			Surrogates: 1,
			Nodes: []ClusterNode{{
				ID:         "1",
				AddrRaft:   "localhost:1001",
				Status:     CLUSTER_NODE_STATUS_INITIALIZING,
				VNodeCount: vnodeCount,
			}, {
				ID:         "2",
				AddrRaft:   "localhost:1002",
				Status:     CLUSTER_NODE_STATUS_INITIALIZING,
				VNodeCount: vnodeCount,
			}, {
				ID:         "3",
				AddrRaft:   "localhost:1003",
				Status:     CLUSTER_NODE_STATUS_INITIALIZING,
				VNodeCount: vnodeCount,
			}},
		},
	}
}

type manifestStats struct {
	masterlessVnodes  int
	replicalessVnodes int
}

func calcManifestStats(m *ClusterManifest) (s manifestStats) {
	var vnodeMasters = map[int]int{}
	var vnodeReplicas = map[int]int{}
	for _, p := range m.Catalog.Partitions {
		vnodeMasters[p.Master]++
		for _, vnid := range p.Replicas {
			vnodeReplicas[vnid]++
		}
	}
	for i := range m.Catalog.VNodes {
		if vnodeMasters[i] == 0 {
			s.masterlessVnodes++
		}
		if vnodeReplicas[i] == 0 {
			s.replicalessVnodes++
		}
	}
	return
}
