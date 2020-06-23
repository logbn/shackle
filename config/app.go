package config

import (
	"time"
)

type App struct {
	Api     *Api     `yaml:"api"`
	Cluster *Cluster `yaml:"cluster"`
	Log     Log      `yaml:"log"`
	Monitor Monitor  `yaml:"monitor"`
	Repo    Repo     `yaml:"repo"`
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

type Cluster struct {
	ID         string `yaml:"id"`
	KeyLength  int    `yaml:"keylength"`
	Partitions int    `yaml:"partitions"`
	Pepper     string `yaml:"pepper"`
	Node       Node   `yaml:"node"`
	Replicas   int    `yaml:"replicas"`
	Surrogates int    `yaml:"surrogates"`
}

type Log struct {
	Level string `yaml:"level"`
}

type Node struct {
	ID       string     `yaml:"id"`
	AddrData string     `yaml:"addr_data"`
	AddrRaft string     `yaml:"addr_raft"`
	RaftDir  string     `yaml:"raft_dir"`
	RaftSolo bool       `yaml:"raft_solo"`
	Join     []NodeJoin `yaml:"join"`
}

type NodeJoin struct {
	ID       string `yaml:"id"`
	AddrRaft string `yaml:"addr_raft"`
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
