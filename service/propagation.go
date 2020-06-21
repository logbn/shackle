package service

import (
	"highvolume.io/shackle/config"
)

type Propagation interface {
	Start()
	Stop()
}

type propagation struct {
}

// NewPropagation returns a propagation service
func NewPropagation(cfg *config.App) (r *propagation, err error) {
	return &propagation{}, nil
}

func (s propagation) Start() {}

func (s propagation) Stop() {}
