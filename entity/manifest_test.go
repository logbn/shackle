package entity

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManifestAllocate(t *testing.T) {
	// Success
	manifest := makeEmptyManifest(3, 2, 1, 64)
	err := manifest.Allocate()
	require.Nil(t, err)
	stats := calcManifestStats(manifest)
	assert.Equal(t, 64*2, len(manifest.Catalog.Nodes))
	assert.Equal(t, 64, len(manifest.Catalog.Witnesses))
	assert.Equal(t, 0, stats.leaderlessHosts)
	assert.Equal(t, 0, stats.emptyHosts)

	manifest = makeEmptyManifest(5, 3, 2, 128)
	err = manifest.Allocate()
	require.Nil(t, err)
	stats = calcManifestStats(manifest)
	assert.Equal(t, 128*3, len(manifest.Catalog.Nodes))
	assert.Equal(t, 128*2, len(manifest.Catalog.Witnesses))
	assert.Equal(t, 0, stats.leaderlessHosts)
	assert.Equal(t, 0, stats.emptyHosts)

	manifest = makeEmptyManifest(3, 3, 1, 16)
	err = manifest.Allocate()
	require.Nil(t, err)
	stats = calcManifestStats(manifest)
	assert.Equal(t, 16*3, len(manifest.Catalog.Nodes))
	assert.Equal(t, 0, len(manifest.Catalog.Witnesses))
	assert.Equal(t, 0, stats.leaderlessHosts)
	assert.Equal(t, 0, stats.emptyHosts)

	manifest = makeEmptyManifest(3, 2, 1, 2)
	err = manifest.Allocate()
	require.Nil(t, err)
	stats = calcManifestStats(manifest)
	assert.Equal(t, 4, len(manifest.Catalog.Nodes))
	assert.Equal(t, 2, len(manifest.Catalog.Witnesses))
	assert.Equal(t, 1, stats.leaderlessHosts)
	assert.Equal(t, 0, stats.emptyHosts)

	manifest = makeEmptyManifest(3, 2, 1, 4096)
	err = manifest.Allocate()
	require.Nil(t, err)
	stats = calcManifestStats(manifest)
	assert.Equal(t, 4096*2, len(manifest.Catalog.Nodes))
	assert.Equal(t, 4096, len(manifest.Catalog.Witnesses))
	assert.Equal(t, 0, stats.leaderlessHosts)
	assert.Equal(t, 0, stats.emptyHosts)

	// Partition count not power of 2
	manifest = makeEmptyManifest(3, 2, 1, 555)
	err = manifest.Allocate()
	require.NotNil(t, err)

	// Hosts empty
	manifest = makeEmptyManifest(3, 2, 1, 256)
	manifest.Catalog.Hosts = nil
	err = manifest.Allocate()
	require.NotNil(t, err)

	// Nodes not empty
	manifest = makeEmptyManifest(3, 2, 1, 256)
	manifest.Catalog.Nodes = []Node{{}, {}}
	err = manifest.Allocate()
	require.NotNil(t, err)

	// Witnesses not empty
	manifest = makeEmptyManifest(3, 2, 1, 256)
	manifest.Catalog.Witnesses = []Witness{{}, {}}
	err = manifest.Allocate()
	require.NotNil(t, err)

	// Manifest not initializing
	manifest = makeEmptyManifest(3, 2, 1, 256)
	manifest.Status = DEPLOYMENT_STATUS_ALLOCATING
	err = manifest.Allocate()
	require.NotNil(t, err)

	// Replicas greater than nodes
	manifest = makeEmptyManifest(3, 5, 1, 256)
	err = manifest.Allocate()
	require.Nil(t, err)
	assert.Equal(t, 256*3, len(manifest.Catalog.Nodes))
	assert.Equal(t, 0, len(manifest.Catalog.Witnesses))

	// To/From Json
	manifest = makeEmptyManifest(3, 2, 1, 256)
	err = manifest.Allocate()
	data := manifest.ToJson()
	require.NotNil(t, data)
	assert.Greater(t, len(data), 1000)
	manifest = makeEmptyManifest(3, 2, 1, 256)
	err = manifest.FromJson(data)
	// fmt.Println(string(manifest.ToJson()))
	require.Nil(t, err)
	assert.Equal(t, 256, len(manifest.Catalog.Witnesses))
}

func TestManifestStatus(t *testing.T) {
	manifest := makeEmptyManifest(3, 2, 1, 64)
	assert.Equal(t, true, manifest.Initializing())
	assert.Equal(t, false, manifest.Allocating())
	assert.Equal(t, false, manifest.Active())
	manifest.Status = DEPLOYMENT_STATUS_ALLOCATING
	assert.Equal(t, false, manifest.Initializing())
	assert.Equal(t, true, manifest.Allocating())
	assert.Equal(t, false, manifest.Active())
	manifest.Status = DEPLOYMENT_STATUS_ACTIVE
	assert.Equal(t, false, manifest.Initializing())
	assert.Equal(t, false, manifest.Allocating())
	assert.Equal(t, true, manifest.Active())
}

func TestManifestHostStatus(t *testing.T) {
	manifest := makeEmptyManifest(3, 2, 1, 64)
	host := manifest.GetHostByRaftAddr("nonsense")
	require.Nil(t, host)
	host = manifest.GetHostByRaftAddr("localhost:1001")
	require.NotNil(t, host)
	assert.Equal(t, true, host.Initializing())
	host.Status = HOST_STATUS_ALLOCATED
	assert.Equal(t, true, host.Allocated())
	host = manifest.GetHostByID(0)
	require.Nil(t, host)
	host = manifest.GetHostByID(1)
	require.NotNil(t, host)
	assert.Equal(t, true, host.Allocated())
	host.Status = HOST_STATUS_ACTIVE
	assert.Equal(t, true, host.Active())
	host.Status = HOST_STATUS_DOWN
	assert.Equal(t, true, host.Down())
	assert.Equal(t, false, host.Active())
	host.Status = HOST_STATUS_RECOVERING
	assert.Equal(t, true, host.Recovering())
	assert.Equal(t, false, host.Active())
}

func makeEmptyManifest(hosts, replicas, witnesses, partitions int) (m *Manifest) {
	m = &Manifest{
		Version:      1,
		DeploymentID: 1,
		Status:       DEPLOYMENT_STATUS_INITIALIZING,
		Catalog: Catalog{
			Version:      "1.0.0",
			ReplicaCount: replicas,
			WitnessCount: witnesses,
			Partitions:   partitions,
			Hosts:        make([]Host, hosts),
		},
	}
	for i := 0; i < hosts; i++ {
		m.Catalog.Hosts[i] = Host{
			ID:         uint64(i + 1),
			RaftAddr:   fmt.Sprintf("localhost:1%03d", i+1),
			IntApiAddr: fmt.Sprintf("localhost:2%03d", i+1),
			Status:     HOST_STATUS_INITIALIZING,
		}
	}
	return
}

type manifestStats struct {
	leaderlessHosts int
	emptyHosts      int
}

func calcManifestStats(m *Manifest) (s manifestStats) {
	var hostLeaders = map[uint64]int{}
	var hostsSeen = map[uint64]bool{}
	for _, node := range m.Catalog.Nodes {
		hostsSeen[node.HostID] = true
		if node.IsLeader {
			hostLeaders[node.HostID]++
		}
	}
	for _, h := range m.Catalog.Hosts {
		if hostLeaders[h.ID] == 0 {
			s.leaderlessHosts++
		}
		if _, ok := hostsSeen[h.ID]; !ok {
			s.emptyHosts++
		}
	}
	return
}
