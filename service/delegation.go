package service

import (
	"context"
	"fmt"

	"highvolume.io/shackle/api/intapi"
	"highvolume.io/shackle/config"
	"highvolume.io/shackle/entity"
	"highvolume.io/shackle/log"
)

type Delegation interface {
	Delegate(op uint32, addr string, batch entity.Batch) (res []int8, err error)
	Start()
	Stop()
}

type delegation struct {
	log          log.Logger
	intApiClient intapi.DelegationClientFinder
	keylen       int
}

// NewDelegation returns a delegation service
func NewDelegation(
	cfg *config.App,
	log log.Logger,
	dcf intapi.DelegationClientFinder,
) (r *delegation, err error) {
	return &delegation{log, dcf, cfg.Cluster.KeyLength}, nil
}

func (s *delegation) Delegate(op uint32, addr string, batch entity.Batch) (res []int8, err error) {
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
	batchResp, err := c.Delegate(context.Background(), &intapi.BatchOp{Op: op, Items: batchBytes})
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

func (s *delegation) markError(res []int8) {
	for i := range res {
		res[i] = entity.ITEM_ERROR
	}
}

func (s *delegation) Start() {}

func (s *delegation) Stop() {
	s.intApiClient.Close()
}
