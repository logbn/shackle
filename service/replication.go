package service

import (
	"context"
	"fmt"

	"highvolume.io/shackle/api/intapi"
	"highvolume.io/shackle/config"
	"highvolume.io/shackle/entity"
	"highvolume.io/shackle/log"
)

type Replication interface {
	Replicate(op uint32, addr string, batch entity.Batch) (res []int8, err error)
	Start()
	Stop()
}

type replication struct {
	log          log.Logger
	intApiClient intapi.ReplicationClientFinder
	keylen       int
}

// NewReplication returns a replication service
func NewReplication(
	cfg *config.App,
	log log.Logger,
	dcf intapi.ReplicationClientFinder,
) (r *replication, err error) {
	return &replication{log, dcf, cfg.Cluster.KeyLength}, nil
}

func (s *replication) Replicate(op uint32, addr string, batch entity.Batch) (res []int8, err error) {
	res = make([]int8, len(batch))
	c, err := s.intApiClient.Get(addr)
	if err != nil {
		return
	}
	batchBytes := make([]byte, len(batch)*s.keylen)
	for i, item := range batch {
		copy(batchBytes[i*s.keylen:i*s.keylen+s.keylen], item.Hash)
	}
	batchResp, err := c.Replicate(context.Background(), &intapi.BatchOp{Op: op, Items: batchBytes})
	if err != nil {
		return
	}
	for i := range batch {
		res[i] = int8(batchResp.Res[i])
	}
	if batchResp.Err != "" {
		err = fmt.Errorf(batchResp.Err)
	}
	return
}

func (s *replication) Start() {}

func (s *replication) Stop() {
	s.intApiClient.Close()
}
