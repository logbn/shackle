package service

import (
	"highvolume.io/shackle/config"
)

type Delegation interface {
	Start()
	Stop()
}

type delegation struct {
}

// NewDelegation returns a hash service
func NewDelegation(cfg *config.App) (r *delegation, err error) {
	return &delegation{}, nil
}

func (s *delegation) Start() {}

func (s *delegation) Stop() {}
