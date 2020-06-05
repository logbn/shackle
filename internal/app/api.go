package app

import (
	"github.com/valyala/fasthttp"

	"highvolume.io/shackle/internal/api/http"
	"highvolume.io/shackle/internal/config"
	"highvolume.io/shackle/internal/log"
	"highvolume.io/shackle/internal/repo"
)

type Api struct {
	log    log.Logger
	server *fasthttp.Server
	port   string
}

func NewApi(cfg config.App, log log.Logger) *Api {
	// repo.Hash
	repoHash, err := repo.NewHash(cfg.Repo.Hash)
	if repoHash == nil || err != nil {
		log.Fatal("Hash repo misconfigured - ", err)
	}
	router := http.NewRouter(
		log,
		repoHash,
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
