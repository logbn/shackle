package service

import (
	"context"
	"fmt"

	"highvolume.io/shackle/api/intapi"
	"highvolume.io/shackle/config"
	"highvolume.io/shackle/entity"
	"highvolume.io/shackle/log"
)

type Propagation interface {
	Propagate(op uint32, addr string, batch entity.Batch) (res []int8, err error)
	Start()
	Stop()
}

type propagation struct {
	log          log.Logger
	intApiClient intapi.PropagationClientFinder
	keylen       int
}

// NewPropagation returns a propagation service
func NewPropagation(
	cfg *config.App,
	log log.Logger,
	dcf intapi.PropagationClientFinder,
) (r *propagation, err error) {
	return &propagation{log, dcf, cfg.Cluster.KeyLength}, nil
}

func (s *propagation) Propagate(op uint32, addr string, batch entity.Batch) (res []int8, err error) {
	res = make([]int8, len(batch))
	c, err := s.intApiClient.Get(addr)
	if err != nil {
		s.markError(res)
		return
	}
	batchBytes := make([]byte, len(batch)*s.keylen)
	for i, item := range batch {
		copy(batchBytes[i*s.keylen:i*s.keylen+s.keylen], item.Hash)
	}
	batchResp, err := c.Propagate(context.Background(), &intapi.BatchOp{Op: op, Items: batchBytes})
	if err != nil {
		s.markError(res)
		return
	}
	for i, item := range batch {
		res[item.N] = int8(batchResp.Res[i])
	}
	if batchResp.Err != "" {
		err = fmt.Errorf(batchResp.Err)
	}
	return
}

func (s *propagation) markError(res []int8) {
	for i := range res {
		res[i] = entity.ITEM_ERROR
	}
}

func (s *propagation) Start() {}

func (s *propagation) Stop() {}
