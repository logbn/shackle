package app

import (
	// "github.com/valyala/fasthttp"

	"highvolume.io/thunder/internal/config"
	"highvolume.io/thunder/internal/log"
)

type Api struct {
	log  log.Logger
	port string
}

func NewApi(cfg config.App, log log.Logger) *Api {
	return &Api{log, cfg.Api.Port}
}

func (a Api) Start() {
	a.log.Info("Api listening on port " + a.port)
}

func (a Api) Stop() {
}
