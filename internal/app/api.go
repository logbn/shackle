package app

import (
	"github.com/valyala/fasthttp"

	"highvolume.io/shackle/internal/api/http"
	"highvolume.io/shackle/internal/config"
	"highvolume.io/shackle/internal/log"
	"highvolume.io/shackle/internal/repo"
	"highvolume.io/shackle/internal/service"
)

type Api struct {
	log    log.Logger
	server *fasthttp.Server
	port   string
}

func NewApi(cfg config.App, log log.Logger) *Api {
	// service.NewPersistence
	svcPersistence, err := service.NewPersistence(cfg.Repo.Hash, repo.NewHash, log)
	if svcPersistence == nil || err != nil {
		log.Fatal("Persistence service misconfigured - ", err)
	}
	router := http.NewRouter(
		log,
		svcPersistence,
	)
	server := &fasthttp.Server{
		Logger:                log,
		Handler:               router.Handler,
		ReadTimeout:           cfg.Api.Http.ReadTimeout,
		WriteTimeout:          cfg.Api.Http.WriteTimeout,
		IdleTimeout:           cfg.Api.Http.IdleTimeout,
		TCPKeepalive:          cfg.Api.Http.Keepalive,
		TCPKeepalivePeriod:    cfg.Api.Http.KeepalivePeriod,
		MaxConnsPerIP:         cfg.Api.Http.MaxConnsPerIP,
		NoDefaultServerHeader: true,
	}
	return &Api{log, server, cfg.Api.Http.Port}
}

func (a Api) Start() {
	a.log.Info("Api listening on port " + a.port)
	go a.server.ListenAndServe(":" + a.port)
}

func (a Api) Stop() {
}
