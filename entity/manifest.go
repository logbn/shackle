package entity

import (
	"encoding/json"
	"fmt"
	"math"
)

const (
	DEPLOYMENT_STATUS_INITIALIZING uint8 = 0
	DEPLOYMENT_STATUS_ALLOCATING   uint8 = 1
	DEPLOYMENT_STATUS_ACTIVE       uint8 = 2

	HOST_STATUS_INITIALIZING uint8 = 0
	HOST_STATUS_ALLOCATED    uint8 = 1
	HOST_STATUS_ACTIVE       uint8 = 2
	HOST_STATUS_DOWN         uint8 = 3
	HOST_STATUS_RECOVERING   uint8 = 4

	NODE_STATUS_STAGING uint8 = 0
	NODE_STATUS_ACTIVE  uint8 = 1
	NODE_STATUS_STANDBY uint8 = 2

	CLUSTER_STATUS_UNAVAILABLE uint8 = 0
	CLUSTER_STATUS_AVAILABLE   uint8 = 1

	MIGRATION_TYPE_SCALE_UP            uint8 = 0
	MIGRATION_TYPE_NODE_REPLACEMENT    uint8 = 1
	MIGRATION_TYPE_CLUSTER_REPLACEMENT uint8 = 2

	MIGRATION_STATUS_PENDING    uint8 = 0
	MIGRATION_STATUS_ACTIVE     uint8 = 1
	MIGRATION_STATUS_WAITING    uint8 = 2
	MIGRATION_STATUS_FINALIZING uint8 = 3
	MIGRATION_STATUS_COMPLETE   uint8 = 4
	MIGRATION_STATUS_CANCELLED  uint8 = 5
)

// Manifest
type Manifest struct {
	DeploymentID uint64      `json:"id"`
	Version      uint8       `json:"v"`
	Status       uint8       `json:"status"`
	Catalog      Catalog     `json:"catalog"`
	Migrations   []Migration `json:"migrations"`
}

func (m *Manifest) Initializing() bool {
	return m.Status == DEPLOYMENT_STATUS_INITIALIZING
}
func (m *Manifest) Allocating() bool {
	return m.Status == DEPLOYMENT_STATUS_ALLOCATING
}
func (m *Manifest) Active() bool {
	return m.Status == DEPLOYMENT_STATUS_ACTIVE
}
func (m *Manifest) ToJson() (data []byte) {
	data, _ = json.Marshal(m)
	return
}
func (m *Manifest) FromJson(data []byte) error {
	return json.Unmarshal(data, m)
}
func (m *Manifest) GetNodeByID(nodeID uint64) *Node {
	for i, n := range m.Catalog.Nodes {
		if n.ID == nodeID {
			return &m.Catalog.Nodes[i]
		}
	}
	return nil
}
func (m *Manifest) GetHostByID(hostID uint64) *Host {
	for i, h := range m.Catalog.Hosts {
		if h.ID == hostID {
			return &m.Catalog.Hosts[i]
		}
	}
	return nil
}
func (m *Manifest) GetHostByRaftAddr(raftAddr string) *Host {
	for i, h := range m.Catalog.Hosts {
		if h.RaftAddr == raftAddr {
			return &m.Catalog.Hosts[i]
		}
	}
	return nil
}

type Catalog struct {
	Version      string    `json:"v"`
	Partitions   int       `json:"p"`
	ReplicaCount int       `json:"rc"`
	WitnessCount int       `json:"wc"`
	KeyLength    uint8     `json:"kl"`
	Vary         []string  `json:"v"`
	Hosts        []Host    `json:"h"`
	Nodes        []Node    `json:"n"`
	Witnesses    []Witness `json:"w"`

	partitionMap map[uint64][]uint64
}

// GetPartitionMap returns a map of HostIDs keyed by ClusterID
func (c *Catalog) GetPartitionMap() map[uint64][]uint64 {
	if c.partitionMap != nil {
		return c.partitionMap
	}
	var m = map[uint64][]uint64{}
	for _, node := range c.Nodes {
		m[node.ClusterID] = append(m[node.ClusterID], node.HostID)
	}
	c.partitionMap = m
	return m
}

// GetHostPartitions returns a slice of clusterIDs for a given host id
func (c *Catalog) GetHostPartitions(hostID uint64) (ret []uint64) {
	for _, node := range c.Nodes {
		if node.HostID == hostID {
			ret = append(ret, node.ClusterID)
		}
	}
	return
}

// Host contains live configuration relevant to node hosts
type Host struct {
	ID         uint64            `json:"id"`
	Status     uint8             `json:"s"`
	RaftAddr   string            `json:"r"`
	IntApiAddr string            `json:"i"`
	Meta       map[string]string `json:"m"`
}

func (h *Host) Initializing() bool {
	return h.Status == HOST_STATUS_INITIALIZING
}
func (h *Host) Allocated() bool {
	return h.Status == HOST_STATUS_ALLOCATED
}
func (h *Host) Active() bool {
	return h.Status == HOST_STATUS_ACTIVE
}
func (h *Host) Down() bool {
	return h.Status == HOST_STATUS_DOWN
}
func (h *Host) Recovering() bool {
	return h.Status == HOST_STATUS_RECOVERING
}

type Node struct {
	ID        uint64 `json:"id"`
	Status    uint8  `json:"s"`
	HostID    uint64 `json:"h"`
	ClusterID uint64 `json:"c"`
	IsLeader  bool   `json:"l"`
}

type Witness struct {
	ID        uint64 `json:"id"`
	Status    uint8  `json:"s"`
	HostID    uint64 `json:"h"`
	ClusterID uint64 `json:"c"`
}

// Allocate is called during deployment initialization to prescribe cluster placement.
// Current allocation algorithm is naive.
// - Distributes nodes as evenly as possible across hosts
// - Does not implement Vary semantics which would prevent duplicate partition replica distibution within a boundary (ie. aws_zone)
// - Only allocates initial state and has no concept of node redistribution or repartitioning
// Uses a Bitwise Key Prefix Partitioning Scheme:
// https://play.golang.org/p/X2F4RQnptAk
func (m *Manifest) Allocate() (err error) {
	if !m.Initializing() {
		err = fmt.Errorf("Deployment is not in initialization state")
		return
	}
	var cat = &m.Catalog
	var hostCount = len(cat.Hosts)
	if hostCount < 1 {
		err = fmt.Errorf("Hosts not yet defined")
		return
	}
	if cat.Nodes != nil {
		err = fmt.Errorf("Nodes already allocated")
		return
	}
	if cat.Witnesses != nil {
		err = fmt.Errorf("Witnesses already allocated")
		return
	}
	var log2pc = math.Log2(float64(cat.Partitions))
	if math.Trunc(log2pc) != log2pc {
		err = fmt.Errorf("Cluster partition count must be power of 2")
		return
	}
	// Limit replica count to node count
	// Distributing 3 replicas across 2 nodes is nonsensical.
	var replicaCount = cat.ReplicaCount
	if replicaCount > hostCount {
		replicaCount = hostCount
	}
	// Limit witness count to (hostCount - replicaCount)
	// Distributing 2 witnesss across a 3 host cluster that already has 2 replicas is nonsensical.
	var witnessCount = cat.WitnessCount
	if witnessCount+replicaCount > hostCount {
		witnessCount = hostCount - replicaCount
	}
	var n int
	var n2 int
	var lastLeader int
	var bits = int(math.Log2(float64(cat.Partitions)))
	for i := 0; i < cat.Partitions; i++ {
		// Distributes leaders more uniformly across nodes
		if n2 > 0 && n2%hostCount == lastLeader {
			n2++
		}
		for j := 0; j < replicaCount; j++ {
			cat.Nodes = append(cat.Nodes, Node{
				ID:        uint64(n + 1),
				Status:    NODE_STATUS_STAGING,
				HostID:    cat.Hosts[n2%hostCount].ID,
				ClusterID: uint64(i << (64 - bits)),
				IsLeader:  j == 0,
			})
			if j == 0 {
				lastLeader = n2 % hostCount
			}
			n++
			n2++
		}
		for j := 0; j < witnessCount; j++ {
			cat.Witnesses = append(cat.Witnesses, Witness{
				ID:        uint64(n + 1),
				Status:    NODE_STATUS_STAGING,
				HostID:    cat.Hosts[n2%hostCount].ID,
				ClusterID: uint64(i << (64 - bits)),
			})
			n++
			n2++
		}
	}
	return
}

type Migration struct {
	Type     string  `json:"t"`
	Status   string  `json:"s"`
	Version  int     `json:"v"`
	Progress int     `json:"p"`
	From     Catalog `json:"fr"`
	To       Catalog `json:"to"`
}
