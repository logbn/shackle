package app

import (
	"fmt"

	"github.com/valyala/fasthttp"

	"highvolume.io/shackle/api/http"
	"highvolume.io/shackle/cluster"
	"highvolume.io/shackle/config"
	"highvolume.io/shackle/log"
	"highvolume.io/shackle/repo"
	"highvolume.io/shackle/service"
)

type Cluster struct {
	log         log.Logger
	server      *fasthttp.Server
	node        cluster.Node
	apiPortHttp int
}

func NewCluster(cfg config.App, log log.Logger) *Cluster {
	// service.Hash
	svcHash, err := service.NewHash(&cfg)
	if svcHash == nil || err != nil {
		log.Fatal("Hash service misconfigured - ", err)
	}
	// service.Coordination
	svcCoordination, err := service.NewCoordination(&cfg)
	if svcCoordination == nil || err != nil {
		log.Fatal("Coordination service misconfigured - ", err)
	}
	// service.Persistence
	svcPersistence, err := service.NewPersistence(&cfg, repo.NewHash, log)
	if svcPersistence == nil || err != nil {
		log.Fatal("Persistence service misconfigured - ", err)
	}
	// service.Propagation
	svcPropagation, err := service.NewPropagation(&cfg)
	if svcPropagation == nil || err != nil {
		log.Fatal("Propagation service misconfigured - ", err)
	}
	// service.Delegation
	svcDelegation, err := service.NewDelegation(&cfg)
	if svcDelegation == nil || err != nil {
		log.Fatal("Delegation service misconfigured - ", err)
	}

	// cluster.Node
	node, err := cluster.NewNode(cfg, log, svcHash, svcCoordination, svcPersistence, svcPropagation, svcDelegation)
	if node == nil || err != nil {
		log.Fatal("Node misconfigured - ", err)
	}

	// fasthttp.Server
	httpRouter := http.NewRouter(log, node, svcHash)
	server := &fasthttp.Server{
		Logger:                log,
		Handler:               httpRouter.Handler,
		ReadTimeout:           cfg.Api.Http.ReadTimeout,
		WriteTimeout:          cfg.Api.Http.WriteTimeout,
		IdleTimeout:           cfg.Api.Http.IdleTimeout,
		TCPKeepalive:          cfg.Api.Http.Keepalive,
		TCPKeepalivePeriod:    cfg.Api.Http.KeepalivePeriod,
		MaxConnsPerIP:         cfg.Api.Http.MaxConnsPerIP,
		NoDefaultServerHeader: true,
	}

	// Create GRPC Server
	return &Cluster{log, server, node, cfg.Api.Http.Port}
}

func (a *Cluster) Start() {
	go func() {
		a.log.Infof("Cluster HTTP Api listening on port %d", a.apiPortHttp)
		err := a.server.ListenAndServe(fmt.Sprintf(":%d", a.apiPortHttp))
		if err != nil {
			a.log.Errorf("Cluster HTTP Api Startup Error: %s", err.Error())
		}
	}()
}

func (a *Cluster) Stop() {
	a.server.Shutdown()
	a.node.Stop()
}
