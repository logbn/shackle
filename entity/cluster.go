package entity

import (
	"encoding/json"
	"time"
)

const (
	CLUSTER_MIGRATION_TYPE_SCALE_UP            = "scale_up"
	CLUSTER_MIGRATION_TYPE_NODE_REPLACEMENT    = "node_replacement"
	CLUSTER_MIGRATION_TYPE_CLUSTER_REPLACEMENT = "cluster_replacement"

	CLUSTER_MIGRATION_STATUS_PENDING    = "pending"
	CLUSTER_MIGRATION_STATUS_ACTIVE     = "active"
	CLUSTER_MIGRATION_STATUS_WAITING    = "waiting"
	CLUSTER_MIGRATION_STATUS_FINALIZING = "finalizing"
	CLUSTER_MIGRATION_STATUS_COMPLETE   = "complete"
	CLUSTER_MIGRATION_STATUS_CANCELLED  = "cancelled"

	CLUSTER_STATUS_PENDING = "pending"
	CLUSTER_STATUS_ACTIVE  = "active"
)

// ClusterManifest
type ClusterManifest struct {
	ID         string             `json:"id"`
	Status     string             `json:"status"`
	Catalog    ClusterCatalog     `json:"catalog"`
	Migrations []ClusterMigration `json:"migrations"`
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
func (e *ClusterManifest) ClusterActive() bool {
	return e.Status == CLUSTER_STATUS_ACTIVE
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
	ID       string            `json:"id"`
	AddrRaft string            `json:"addr_raft"`
	AddrData string            `json:"addr_data"`
	Meta     map[string]string `json:"meta"`
}

type ClusterVNode struct {
	ID       string `json:"id"`
	Node     int    `json:"node"`
	Capacity int    `json:"cap"`
}

type ClusterPartition struct {
	Prefix     int   `json:"p"`
	Master     int   `json:"m"`
	Replicas   []int `json:"r"`
	Surrogates []int `json:"s"`
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
