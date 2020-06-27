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
