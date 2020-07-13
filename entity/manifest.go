package entity

import (
	"encoding/json"
	"fmt"
	"math"
)

const (
	DEPLOYMENT_STATUS_INITIALIZING uint8 = 0
	DEPLOYMENT_STATUS_ACTIVE       uint8 = 1

	HOST_STATUS_INITIALIZING uint8 = 0
	HOST_STATUS_ACTIVE       uint8 = 1
	HOST_STATUS_DOWN         uint8 = 2
	HOST_STATUS_RECOVERING   uint8 = 3

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
	Version      uint8       `json:"version"`
	Status       uint8       `json:"status"`
	Catalog      Catalog     `json:"catalog"`
	Epoch        uint32      `json:"epoch"`
	Migrations   []Migration `json:"migrations"`
}

func (m *Manifest) Initializing() bool {
	return m.Status == DEPLOYMENT_STATUS_INITIALIZING
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

// GetPartitionPeers returns a map of host id to host addr for a given partition
func (m *Manifest) GetPartitionPeers(partition uint16) (ret map[uint64]string) {
	ret = map[uint64]string{}
	partMap := m.Catalog.GetPartitionMap()
	for _, id := range partMap[partition] {
		h := m.GetHostByID(uint64(id))
		ret[uint64(id)] = h.RaftAddr
	}
	return
}

// Catalog contains a complete snapshot of the current physical and logical distribution of data within the deployment.
type Catalog struct {
	Version      string    `json:"version"`
	Partitions   int       `json:"partitions"`
	ReplicaCount int       `json:"replicationCount"`
	WitnessCount int       `json:"witnessCount"`
	KeyLength    uint8     `json:"keyLength"`
	Vary         []string  `json:"vary"`
	Hosts        []Host    `json:"hosts"`
	Nodes        []Node    `json:"nodes"`
	Witnesses    []Witness `json:"witnesses"`

	partitionHostMap map[uint16][]uint64
	partitionNodeMap map[uint16]Node
}

// FromJson marshals the catalog to json
func (c *Catalog) ToJson() (data []byte) {
	data, _ = json.Marshal(c)
	return
}

// FromJson unmarshals json into the present catalog
func (c *Catalog) FromJson(data []byte) error {
	return json.Unmarshal(data, c)
}

// GetPartitionMap returns a map of HostIDs keyed by ClusterID
func (c *Catalog) GetPartitionMap() map[uint16][]uint64 {
	if c.partitionHostMap != nil {
		return c.partitionHostMap
	}
	var m = map[uint16][]uint64{}
	for _, node := range c.Nodes {
		m[node.Partition] = append(m[node.Partition], node.HostID)
	}
	c.partitionHostMap = m
	return m
}

// GetLocalNodeByPartition returns the local node responsible for a partition
func (c *Catalog) GetLocalNodeByPartition(p uint16, hostID uint64) *Node {
	if c.partitionNodeMap == nil {
		c.partitionNodeMap = map[uint16]Node{}
		for _, node := range c.Nodes {
			if node.HostID == hostID {
				c.partitionNodeMap[node.Partition] = node
			}
		}
	}
	if node, ok := c.partitionNodeMap[p]; ok {
		return &node
	}
	return nil
}

// GetHostPartitions returns a slice of clusterIDs for a given host id
func (c *Catalog) GetHostPartitions(hostID uint64) (ret []uint16) {
	for _, node := range c.Nodes {
		if node.HostID == hostID {
			ret = append(ret, node.Partition)
		}
	}
	return
}

// Host contains live configuration relevant to node hosts
type Host struct {
	ID         uint64            `json:"id"`
	Status     uint8             `json:"status"`
	RaftAddr   string            `json:"raftAddr"`
	IntApiAddr string            `json:"intApiAddr"`
	Meta       map[string]string `json:"meta"`
	Databases  []string          `json:"db"`
}

func (h *Host) Initializing() bool {
	return h.Status == HOST_STATUS_INITIALIZING
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
	Status    uint8  `json:"status"`
	HostID    uint64 `json:"hostId"`
	ClusterID uint64 `json:"clusterId"`
	Partition uint16 `json:"partition"`
	IsLeader  bool   `json:"isLeader"`
}

type Witness struct {
	ID        uint64 `json:"id"`
	Status    uint8  `json:"status"`
	HostID    uint64 `json:"hostId"`
	ClusterID uint64 `json:"clusterId"`
	Partition uint16 `json:"partition"`
}

// Allocate is called during deployment initialization to prescribe cluster placement.
// Current allocation algorithm is naive.
// - Distributes nodes as evenly as possible across hosts
// - Does not implement Vary semantics which would prevent duplicate partition replica distibution within a boundary (ie. aws_zone)
// - Only allocates initial state and has no concept of node redistribution or repartitioning
// Uses a Bitwise Key Prefix Partitioning Scheme:
// https://play.golang.org/p/X2F4RQnptAk
func (m *Manifest) Allocate() (cat *Catalog, err error) {
	if !m.Initializing() {
		err = fmt.Errorf("Deployment is not in initialization state")
		return
	}
	var hostCount = len(m.Catalog.Hosts)
	if hostCount < 1 {
		err = fmt.Errorf("Hosts not yet defined")
		return
	}
	if m.Catalog.Nodes != nil {
		err = fmt.Errorf("Nodes already allocated")
		return
	}
	if m.Catalog.Witnesses != nil {
		err = fmt.Errorf("Witnesses already allocated")
		return
	}
	var log2pc = math.Log2(float64(m.Catalog.Partitions))
	if math.Trunc(log2pc) != log2pc {
		err = fmt.Errorf("Cluster partition count must be power of 2")
		return
	}
	cat = &Catalog{}
	cat.FromJson(m.Catalog.ToJson())
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
				ClusterID: uint64(i + 1),
				Partition: uint16(i << (16 - bits)),
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
				ClusterID: uint64(i + 1),
				Partition: uint16(i << (16 - bits)),
			})
			n++
			n2++
		}
	}
	return
}

type Migration struct {
	Type     string  `json:"type"`
	Status   string  `json:"status"`
	Version  int     `json:"version"`
	Progress int     `json:"progress"`
	From     Catalog `json:"from"`
	To       Catalog `json:"to"`
}
