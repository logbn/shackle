package app

import (
	"fmt"
	"net"

	"github.com/valyala/fasthttp"
	"google.golang.org/grpc"

	"highvolume.io/shackle/api/grpcint"
	"highvolume.io/shackle/api/http"
	"highvolume.io/shackle/cluster"
	"highvolume.io/shackle/config"
	"highvolume.io/shackle/log"
	"highvolume.io/shackle/repo"
	"highvolume.io/shackle/service"
)

type Cluster struct {
	log           log.Logger
	grpcIntServer *grpc.Server
	httpServer    *fasthttp.Server
	host          cluster.Host
	apiPortHttp   int
	addrIntApi    string
}

func NewCluster(cfg config.App, log log.Logger) (*Cluster, error) {
	// grpcint.DelegationClientFinder
	delegationClient := grpcint.NewDelegationClientFinder()
	if delegationClient == nil {
		return nil, fmt.Errorf("Delegation client finder misconfigured")
	}

	// service.Hash
	svcHash, err := service.NewHash(&cfg)
	if svcHash == nil || err != nil {
		return nil, fmt.Errorf("Hash service misconfigured - %s", err.Error())
	}
	// service.Persistence
	svcPersistence, err := service.NewPersistence(&cfg, log, repo.NewHash)
	if svcPersistence == nil || err != nil {
		return nil, fmt.Errorf("Persistence service misconfigured - %s", err.Error())
	}
	// service.Delegation
	svcDelegation, err := service.NewDelegation(&cfg, log, delegationClient)
	if svcDelegation == nil || err != nil {
		return nil, fmt.Errorf("Delegation service misconfigured - %s", err.Error())
	}

	// cluster.Host
	host, err := cluster.NewHost(*cfg.Host, log, svcHash, svcPersistence, svcDelegation)
	if host == nil || err != nil {
		return nil, fmt.Errorf("Host misconfigured - %s", err.Error())
	}

	// grpc.Server
	grpcIntServer := grpc.NewServer()
	grpcint.RegisterDelegationServer(grpcIntServer, host)

	// fasthttp.Server
	httpRouter := http.NewRouter(log, host, svcHash)
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
	return &Cluster{log, grpcIntServer, httpServer, host, cfg.Api.Http.Port, cfg.Host.IntApiAddr}, nil
}

func (a *Cluster) Start() (err error) {
	a.host.Start()
	go func() {
		a.log.Infof("Cluster HTTP Api listening on port %d", a.apiPortHttp)
		err := a.httpServer.ListenAndServe(fmt.Sprintf(":%d", a.apiPortHttp))
		if err != nil {
			a.log.Errorf("Cluster HTTP Api Startup Error: %s", err.Error())
		}
	}()
	lis, err := net.Listen("tcp", a.addrIntApi)
	if err != nil {
		return
	}
	go func() {
		err = a.grpcIntServer.Serve(lis)
		if err != nil {
			a.log.Errorf(err.Error())
		}
	}()
	return
}

func (a *Cluster) Stop() {
	a.httpServer.Shutdown()
	a.grpcIntServer.Stop()
	a.host.Stop()
}
