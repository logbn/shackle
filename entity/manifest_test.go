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
	cat, err := manifest.Allocate()
	require.Nil(t, err)
	stats := calcManifestStats(cat)
	assert.Equal(t, 64*2, len(cat.Nodes))
	assert.Equal(t, 64, len(cat.Witnesses))
	assert.Equal(t, 0, stats.leaderlessHosts)
	assert.Equal(t, 0, stats.emptyHosts)

	manifest = makeEmptyManifest(5, 3, 2, 128)
	cat, err = manifest.Allocate()
	require.Nil(t, err)
	stats = calcManifestStats(cat)
	assert.Equal(t, 128*3, len(cat.Nodes))
	assert.Equal(t, 128*2, len(cat.Witnesses))
	assert.Equal(t, 0, stats.leaderlessHosts)
	assert.Equal(t, 0, stats.emptyHosts)

	manifest = makeEmptyManifest(3, 3, 1, 16)
	cat, err = manifest.Allocate()
	require.Nil(t, err)
	stats = calcManifestStats(cat)
	assert.Equal(t, 16*3, len(cat.Nodes))
	assert.Equal(t, 0, len(cat.Witnesses))
	assert.Equal(t, 0, stats.leaderlessHosts)
	assert.Equal(t, 0, stats.emptyHosts)

	manifest = makeEmptyManifest(3, 2, 1, 2)
	cat, err = manifest.Allocate()
	require.Nil(t, err)
	stats = calcManifestStats(cat)
	assert.Equal(t, 4, len(cat.Nodes))
	assert.Equal(t, 2, len(cat.Witnesses))
	assert.Equal(t, 1, stats.leaderlessHosts)
	assert.Equal(t, 0, stats.emptyHosts)

	manifest = makeEmptyManifest(3, 2, 1, 4096)
	cat, err = manifest.Allocate()
	require.Nil(t, err)
	stats = calcManifestStats(cat)
	assert.Equal(t, 4096*2, len(cat.Nodes))
	assert.Equal(t, 4096, len(cat.Witnesses))
	assert.Equal(t, 0, stats.leaderlessHosts)
	assert.Equal(t, 0, stats.emptyHosts)

	// Partition count not power of 2
	manifest = makeEmptyManifest(3, 2, 1, 555)
	_, err = manifest.Allocate()
	require.NotNil(t, err)

	// Hosts empty
	manifest = makeEmptyManifest(3, 2, 1, 256)
	manifest.Catalog.Hosts = nil
	_, err = manifest.Allocate()
	require.NotNil(t, err)

	// Nodes not empty
	manifest = makeEmptyManifest(3, 2, 1, 256)
	manifest.Catalog.Nodes = []Node{{}, {}}
	_, err = manifest.Allocate()
	require.NotNil(t, err)

	// Witnesses not empty
	manifest = makeEmptyManifest(3, 2, 1, 256)
	manifest.Catalog.Witnesses = []Witness{{}, {}}
	_, err = manifest.Allocate()
	require.NotNil(t, err)

	// Manifest not initializing
	manifest = makeEmptyManifest(3, 2, 1, 256)
	manifest.Status = DEPLOYMENT_STATUS_ACTIVE
	_, err = manifest.Allocate()
	require.NotNil(t, err)

	// Replicas greater than nodes
	manifest = makeEmptyManifest(3, 5, 1, 256)
	cat, err = manifest.Allocate()
	require.Nil(t, err)
	assert.Equal(t, 256*3, len(cat.Nodes))
	assert.Equal(t, 0, len(cat.Witnesses))

	// To/From Json
	manifest = makeEmptyManifest(3, 2, 1, 256)
	cat, err = manifest.Allocate()
	data := cat.ToJson()
	require.NotNil(t, data)
	assert.Greater(t, len(data), 1000, string(data))
	manifest = makeEmptyManifest(3, 2, 1, 256)
	err = cat.FromJson(data)
	require.Nil(t, err)
	assert.Equal(t, 256, len(cat.Witnesses))
}

func TestManifestStatus(t *testing.T) {
	manifest := makeEmptyManifest(3, 2, 1, 64)
	assert.Equal(t, true, manifest.Initializing())
	assert.Equal(t, false, manifest.Active())
	manifest.Status = DEPLOYMENT_STATUS_ACTIVE
	assert.Equal(t, false, manifest.Initializing())
	assert.Equal(t, true, manifest.Active())
}

func TestManifestHostStatus(t *testing.T) {
	manifest := makeEmptyManifest(3, 2, 1, 64)
	host := manifest.GetHostByRaftAddr("nonsense")
	require.Nil(t, host)
	host = manifest.GetHostByRaftAddr("localhost:1001")
	require.NotNil(t, host)
	assert.Equal(t, true, host.Initializing())
	host = manifest.GetHostByID(0)
	require.Nil(t, host)
	host = manifest.GetHostByID(1)
	require.NotNil(t, host)
	assert.Equal(t, true, host.Initializing())
	host.Status = HOST_STATUS_ACTIVE
	assert.Equal(t, true, host.Active())
	host.Status = HOST_STATUS_DOWN
	assert.Equal(t, true, host.Down())
	assert.Equal(t, false, host.Active())
	host.Status = HOST_STATUS_RECOVERING
	assert.Equal(t, true, host.Recovering())
	assert.Equal(t, false, host.Active())
}

func makeEmptyManifest(hosts, replicas, witnesses int, partitions uint16) (m *Manifest) {
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

func calcManifestStats(cat *Catalog) (s manifestStats) {
	var hostLeaders = map[uint64]int{}
	var hostsSeen = map[uint64]bool{}
	for _, node := range cat.Nodes {
		hostsSeen[node.HostID] = true
		if node.IsLeader {
			hostLeaders[node.HostID]++
		}
	}
	for _, h := range cat.Hosts {
		if hostLeaders[h.ID] == 0 {
			s.leaderlessHosts++
		}
		if _, ok := hostsSeen[h.ID]; !ok {
			s.emptyHosts++
		}
	}
	return
}
