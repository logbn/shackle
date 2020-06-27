package entity

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
)

const (
	CLUSTER_STATUS_INITIALIZING = "initializing"
	CLUSTER_STATUS_ALLOCATING   = "allocating"
	CLUSTER_STATUS_ACTIVE       = "active"

	CLUSTER_NODE_STATUS_INITIALIZING = "initializing"
	CLUSTER_NODE_STATUS_ALLOCATED    = "allocated"
	CLUSTER_NODE_STATUS_ACTIVE       = "active"
	CLUSTER_NODE_STATUS_DOWN         = "down"
	CLUSTER_NODE_STATUS_RECOVERING   = "recovering"

	CLUSTER_SHARD_STATUS_STAGING = "staging"
	CLUSTER_SHARD_STATUS_ACTIVE  = "active"
	CLUSTER_SHARD_STATUS_STANDBY = "standby"

	CLUSTER_PARTITION_STATUS_AVAILABLE   = "available"
	CLUSTER_PARTITION_STATUS_UNAVAILABLE = "unavailable"

	CLUSTER_MIGRATION_TYPE_SCALE_UP            = "scale_up"
	CLUSTER_MIGRATION_TYPE_NODE_REPLACEMENT    = "node_replacement"
	CLUSTER_MIGRATION_TYPE_CLUSTER_REPLACEMENT = "cluster_replacement"

	CLUSTER_MIGRATION_STATUS_PENDING    = "pending"
	CLUSTER_MIGRATION_STATUS_ACTIVE     = "active"
	CLUSTER_MIGRATION_STATUS_WAITING    = "waiting"
	CLUSTER_MIGRATION_STATUS_FINALIZING = "finalizing"
	CLUSTER_MIGRATION_STATUS_COMPLETE   = "complete"
	CLUSTER_MIGRATION_STATUS_CANCELLED  = "cancelled"
)

// ClusterManifest
type ClusterManifest struct {
	ID         string             `json:"id"`
	Status     string             `json:"status"`
	Catalog    ClusterCatalog     `json:"catalog"`
	Migrations []ClusterMigration `json:"migrations"`
}

func (e *ClusterManifest) ClusterInitializing() bool {
	return e.Status == CLUSTER_STATUS_INITIALIZING
}
func (e *ClusterManifest) ClusterAllocating() bool {
	return e.Status == CLUSTER_STATUS_ALLOCATING
}
func (e *ClusterManifest) ClusterActive() bool {
	return e.Status == CLUSTER_STATUS_ACTIVE
}
func (e *ClusterManifest) ToJson() (data []byte) {
	data, _ = json.Marshal(e)
	return
}
func (e *ClusterManifest) FromJson(data []byte) error {
	return json.Unmarshal(data, e)
}
func (e *ClusterManifest) GetNodeByID(nodeID string) *ClusterNode {
	for i, n := range e.Catalog.Nodes {
		if n.ID == nodeID {
			return &e.Catalog.Nodes[i]
		}
	}
	return nil
}
func (e *ClusterManifest) GetNodeByAddrRaft(addrRaft string) *ClusterNode {
	for i, n := range e.Catalog.Nodes {
		if n.AddrRaft == addrRaft {
			return &e.Catalog.Nodes[i]
		}
	}
	return nil
}

type ClusterCatalog struct {
	Version    string             `json:"version"`
	Replicas   int                `json:"k"`
	Surrogates int                `json:"s"`
	KeyLength  int                `json:"keylen"`
	Vary       []string           `json:"vary"`
	Nodes      []ClusterNode      `json:"nodes"`
	VNodes     []ClusterVNode     `json:"vnodes"`
	Partitions []ClusterPartition `json:"partitions"`
}

type ClusterNode struct {
	ID         string            `json:"id"`
	Status     string            `json:"status"`
	AddrRaft   string            `json:"addr_raft"`
	AddrIntApi string            `json:"addr_int_api"`
	Meta       map[string]string `json:"meta"`
	VNodeCount int               `json:"vnode_count"`
}

func (e *ClusterNode) Initializing() bool {
	return e.Status == CLUSTER_NODE_STATUS_INITIALIZING
}
func (e *ClusterNode) Allocated() bool {
	return e.Status == CLUSTER_NODE_STATUS_ALLOCATED
}
func (e *ClusterNode) Active() bool {
	return e.Status == CLUSTER_NODE_STATUS_ACTIVE
}
func (e *ClusterNode) Down() bool {
	return e.Status == CLUSTER_NODE_STATUS_DOWN
}
func (e *ClusterNode) Recovering() bool {
	return e.Status == CLUSTER_NODE_STATUS_RECOVERING
}

type ClusterVNode struct {
	ID   string `json:"id"`
	Node string `json:"node"`
}

type ClusterPartition struct {
	Prefix     uint16 `json:"p"`
	Master     int    `json:"m"`
	Replicas   []int  `json:"r"`
	Surrogates []int  `json:"s"`
}

type ClusterMigration struct {
	Type       string         `json:"type"`
	Status     string         `json:"status"`
	Version    int            `json:"version"`
	Progress   int            `json:"progress"`
	StartAfter time.Time      `json:"start_after"`
	From       ClusterCatalog `json:"from"`
	To         ClusterCatalog `json:"to"`
}

// Allocate is called during cluster initialization to prescribe vnodes, partition configuration and replica placement.
// Current allocation algorithm is naive.
// - Distributes partitions evenly across nodes rather than vnodes
// - Does not implement Vary semantics to prevent duplicate partition replica distibution within a boundary (ie. aws_zone)
// - Only allocates initial state and has no concept of shard redistribution or data locality
func (e *ClusterManifest) Allocate(partitionCount int) (err error) {
	if e.Status != CLUSTER_STATUS_INITIALIZING {
		err = fmt.Errorf("Cluster is not in initialization state")
		return
	}
	var nodeCount = len(e.Catalog.Nodes)
	if nodeCount < 1 {
		err = fmt.Errorf("Cluster nodes not yet defined")
		return
	}
	if e.Catalog.VNodes != nil {
		err = fmt.Errorf("Cluster vnodes already allocated")
		return
	}
	if e.Catalog.Partitions != nil {
		err = fmt.Errorf("Cluster partitions already allocated")
		return
	}
	var log2pc = math.Log2(float64(partitionCount))
	if math.Trunc(log2pc) != log2pc {
		err = fmt.Errorf("Cluster partition count must be power of 2")
		return
	}
	// Limit replica count to node count
	// Distributing 3 replicas across 2 nodes is nonsensical.
	var k = e.Catalog.Replicas
	if k > nodeCount {
		k = nodeCount
	}
	var nodeMasters = make([][]int, nodeCount)
	var nodeReplicas = make([][]int, nodeCount)
	var n int
	var lastMaster int
	for i := 0; i < partitionCount; i++ {
		for j := 0; j < k; j++ {
			if j == 0 {
				if i != 0 && n%nodeCount == lastMaster {
					n++
				}
				nodeMasters[n%nodeCount] = append(nodeMasters[n%nodeCount], i)
				lastMaster = n % nodeCount
			} else {
				nodeReplicas[n%nodeCount] = append(nodeReplicas[n%nodeCount], i)
			}
			n++
		}
	}
	var nodeVNodes = make([][]ClusterVNode, nodeCount)
	for i, n := range e.Catalog.Nodes {
		if n.VNodeCount == 0 {
			err = fmt.Errorf("VNode count cannot be 0 for node %s", n.ID)
			return
		}
		nodeVNodes[i] = []ClusterVNode{}
		for j := 0; j < n.VNodeCount; j++ {
			nodeVNodes[i] = append(nodeVNodes[i], ClusterVNode{
				ID:   uuid.New().String(),
				Node: n.ID,
			})
		}
	}
	/*
		Bitwise Key Prefix Partitioning Scheme

		https://play.golang.org/p/Lm_Qtq8z06h

		partitions := 4096
		bits := int(math.Log2(float64(partitions)))
		mask := uint16(math.MaxUint16 >> (16 - bits) << (16 - bits))

		key := []byte("test")

		partition := binary.BigEndian.Uint16(key[:2]) & mask

		fmt.Printf("Key Prefix: %x, Partition: %04x, Number: %d\n", key[:2], partition, partition >> (16 - bits))
	*/
	var bits = int(math.Log2(float64(partitionCount)))
	var partitions = make([]ClusterPartition, partitionCount)
	for i := 0; i < partitionCount; i++ {
		partitions[i] = ClusterPartition{
			Prefix: uint16(i << (16 - bits)),
		}
	}
	for i, n := range e.Catalog.Nodes {
		var masters int
		var vnn = len(nodeVNodes[i])
		for j, p := range nodeMasters[i] {
			partitions[p].Master = i*n.VNodeCount + j%vnn
			masters++
		}
		for j, p := range nodeReplicas[i] {
			partitions[p].Replicas = append(partitions[p].Replicas, i*n.VNodeCount+(masters+j)%vnn)
		}
	}
	for i := range e.Catalog.Nodes {
		e.Catalog.VNodes = append(e.Catalog.VNodes, nodeVNodes[i]...)
	}
	e.Catalog.Partitions = partitions
	return
}
