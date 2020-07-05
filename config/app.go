package config

import (
	"encoding/json"
	"time"
)

type App struct {
	Api     *Api    `yaml:"api"`
	Host    *Host   `yaml:"node_host"`
	Log     Log     `yaml:"log"`
	Monitor Monitor `yaml:"monitor"`
	Repo    Repo    `yaml:"repo"`
}

type Api struct {
	Enabled bool    `yaml:"enabled"`
	Http    ApiHttp `yaml:"http"`
}

type ApiHttp struct {
	IdleTimeout     time.Duration `yaml:"idle_timeout"`
	Keepalive       bool          `yaml:"keepalive"`
	KeepalivePeriod time.Duration `yaml:"keepalive_period"`
	MaxConnsPerIP   int           `yaml:"max_conns_per_ip"`
	Port            int           `yaml:"port"`
	ReadTimeout     time.Duration `yaml:"read_timeout"`
	WriteTimeout    time.Duration `yaml:"write_timeout"`
}

type Host struct {
	ID           uint64     `yaml:"id"`
	DeploymentID uint64     `yaml:"deployment_id"`
	KeyLength    int        `yaml:"keylength"`
	Partitions   int        `yaml:"partitions"`
	Pepper       string     `yaml:"pepper"`
	ReplicaCount int        `yaml:"replica_count"`
	WitnessCount int        `yaml:"witness_count"`
	IntApiAddr   string     `yaml:"int_api_addr"`
	RaftAddr     string     `yaml:"raft_addr"`
	RaftDir      string     `yaml:"raft_dir"`
	RaftSolo     bool       `yaml:"raft_solo"`
	Meta         HostMeta   `yaml:"meta"`
	Join         []HostJoin `yaml:"join"`
}

type Log struct {
	Level string `yaml:"level"`
}

type HostMeta map[string]string

func (n *HostMeta) ToJson() (out []byte) {
	out, _ = json.Marshal(n)
	return
}

type HostJoin struct {
	ID       uint64 `yaml:"id"`
	RaftAddr string `yaml:"raft_addr"`
}

type Repo struct {
	Hash *RepoHash `yaml:"hash"`
}

type RepoHash struct {
	CacheSize      int           `yaml:"cachesize"`
	ExpBatchSize   int           `yaml:"expiration_batch_size"`
	KeyExpiration  time.Duration `yaml:"key_expiration"`
	LockExpiration time.Duration `yaml:"lock_expiration"`
	PathIndex      string        `yaml:"path_ix"`
	PathTimeseries string        `yaml:"path_ts"`
	SweepInterval  time.Duration `yaml:"sweep_interval"`
}
