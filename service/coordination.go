package service

import (
	"highvolume.io/shackle/config"
)

type Coordination interface {
	Start()
	Stop()
}

type coordination struct {
}

// NewCoordination returns a coordination service
func NewCoordination(cfg *config.App) (r *coordination, err error) {
	return &coordination{}, nil
}

func (s coordination) Start() {}

func (s coordination) Stop() {}
