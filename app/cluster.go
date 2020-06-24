package app

import (
	"fmt"
	"net"

	"github.com/valyala/fasthttp"
	"google.golang.org/grpc"

	"highvolume.io/shackle/api/data"
	"highvolume.io/shackle/api/http"
	"highvolume.io/shackle/cluster"
	"highvolume.io/shackle/config"
	"highvolume.io/shackle/log"
	"highvolume.io/shackle/repo"
	"highvolume.io/shackle/service"
)

type Cluster struct {
	log         log.Logger
	dataServer  *grpc.Server
	httpServer  *fasthttp.Server
	node        cluster.Node
	apiPortHttp int
	addrData    string
}

func NewCluster(cfg config.App, log log.Logger) (*Cluster, error) {

	// cluster.CoordinationClientFinder
	coordinationClient := data.NewCoordinationClientFinder()
	if coordinationClient == nil {
		return nil, fmt.Errorf("Coordination client finder misconfigured")
	}
	// service.Hash
	svcHash, err := service.NewHash(&cfg)
	if svcHash == nil || err != nil {
		return nil, fmt.Errorf("Hash service misconfigured - %s", err.Error())
	}
	// service.Coordination
	svcCoordination, err := service.NewCoordination(&cfg, log, coordinationClient)
	if svcCoordination == nil || err != nil {
		return nil, fmt.Errorf("Coordination service misconfigured - %s", err.Error())
	}
	// service.Persistence
	svcPersistence, err := service.NewPersistence(&cfg, repo.NewHash, log)
	if svcPersistence == nil || err != nil {
		return nil, fmt.Errorf("Persistence service misconfigured - %s", err.Error())
	}
	// service.Propagation
	svcPropagation, err := service.NewPropagation(&cfg)
	if svcPropagation == nil || err != nil {
		return nil, fmt.Errorf("Propagation service misconfigured - %s", err.Error())
	}
	// service.Delegation
	svcDelegation, err := service.NewDelegation(&cfg)
	if svcDelegation == nil || err != nil {
		return nil, fmt.Errorf("Delegation service misconfigured - %s", err.Error())
	}

	// cluster.Node
	node, err := cluster.NewNode(cfg, log, svcHash, svcCoordination, svcPersistence, svcPropagation, svcDelegation)
	if node == nil || err != nil {
		return nil, fmt.Errorf("Node misconfigured - %s", err.Error())
	}

	// grpc.Server
	dataServer := grpc.NewServer()
	data.RegisterCoordinationServer(dataServer, svcCoordination)

	// fasthttp.Server
	httpRouter := http.NewRouter(log, node, svcHash)
	httpServer := &fasthttp.Server{
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
	return &Cluster{log, dataServer, httpServer, node, cfg.Api.Http.Port, cfg.Cluster.Node.AddrData}, nil
}

func (a *Cluster) Start() (err error) {
	a.node.Start()
	go func() {
		a.log.Infof("Cluster HTTP Api listening on port %d", a.apiPortHttp)
		err := a.httpServer.ListenAndServe(fmt.Sprintf(":%d", a.apiPortHttp))
		if err != nil {
			a.log.Errorf("Cluster HTTP Api Startup Error: %s", err.Error())
		}
	}()
	lis, err := net.Listen("tcp", a.addrData)
	if err != nil {
		return
	}
	go func() {
		err = a.dataServer.Serve(lis)
		if err != nil {
			a.log.Errorf(err.Error())
		}
	}()
	return
}

func (a *Cluster) Stop() {
	a.node.Stop()
	a.dataServer.Stop()
}
