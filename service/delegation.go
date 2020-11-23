package service

import (
	"context"
	"fmt"

	"logbin.io/shackle/api/grpcint"
	"logbin.io/shackle/config"
	"logbin.io/shackle/entity"
	"logbin.io/shackle/log"
)

type Delegation interface {
	Delegate(op uint8, addr string, batch entity.Batch) (res []uint8, err error)
	Start()
	Stop()
}

type delegation struct {
	log           log.Logger
	grpcIntClient grpcint.DelegationClientFinder
	keylen        int
}

// NewDelegation returns a delegation service
func NewDelegation(
	cfg *config.App,
	log log.Logger,
	dcf grpcint.DelegationClientFinder,
) (r *delegation, err error) {
	return &delegation{log, dcf, cfg.Host.KeyLength}, nil
}

func (s *delegation) Delegate(op uint8, addr string, batch entity.Batch) (res []uint8, err error) {
	res = make([]uint8, len(batch))
	c, err := s.grpcIntClient.Get(addr)
	if err != nil {
		return
	}
	batchBytes := make([]byte, len(batch)*s.keylen)
	for i, item := range batch {
		copy(batchBytes[i*s.keylen:i*s.keylen+s.keylen], item.Hash)
	}
	batchResp, err := c.Delegate(context.Background(), &grpcint.BatchOp{Op: uint32(op), Items: batchBytes})
	if err != nil {
		return
	}
	for i := range batch {
		res[i] = uint8(batchResp.Res[i])
	}
	if batchResp.Err != "" {
		err = fmt.Errorf(batchResp.Err)
	}
	return
}

func (s *delegation) Start() {}

func (s *delegation) Stop() {
	s.grpcIntClient.Close()
}
