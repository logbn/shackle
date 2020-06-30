package app

import (
	"fmt"
	"net"

	"github.com/valyala/fasthttp"
	"google.golang.org/grpc"

	"highvolume.io/shackle/api/http"
	"highvolume.io/shackle/api/intapi"
	"highvolume.io/shackle/cluster"
	"highvolume.io/shackle/config"
	"highvolume.io/shackle/log"
	"highvolume.io/shackle/repo"
	"highvolume.io/shackle/service"
)

type Cluster struct {
	log          log.Logger
	intApiServer *grpc.Server
	httpServer   *fasthttp.Server
	node         cluster.Node
	apiPortHttp  int
	addrIntApi   string
}

func NewCluster(cfg config.App, log log.Logger) (*Cluster, error) {
	// intapi.CoordinationClientFinder
	coordinationClient := intapi.NewCoordinationClientFinder()
	if coordinationClient == nil {
		return nil, fmt.Errorf("Coordination client finder misconfigured")
	}
	// intapi.DelegationClientFinder
	delegationClient := intapi.NewDelegationClientFinder()
	if delegationClient == nil {
		return nil, fmt.Errorf("Delegation client finder misconfigured")
	}
	// intapi.ReplicationClientFinder
	replicationClient := intapi.NewReplicationClientFinder()
	if replicationClient == nil {
		return nil, fmt.Errorf("Replication client finder misconfigured")
	}

	// service.Hash
	svcHash, err := service.NewHash(&cfg)
	if svcHash == nil || err != nil {
		return nil, fmt.Errorf("Hash service misconfigured - %s", err.Error())
	}
	// service.Coordination
	svcCoordination, initChan, err := service.NewCoordination(&cfg, log, coordinationClient)
	if svcCoordination == nil || err != nil {
		return nil, fmt.Errorf("Coordination service misconfigured - %s", err.Error())
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
	// service.Replication
	svcReplication, err := service.NewReplication(&cfg, log, replicationClient)
	if svcReplication == nil || err != nil {
		return nil, fmt.Errorf("Replication service misconfigured - %s", err.Error())
	}

	// cluster.Node
	node, err := cluster.NewNode(cfg, log, svcHash, svcCoordination, svcPersistence, svcReplication, svcDelegation, initChan)
	if node == nil || err != nil {
		return nil, fmt.Errorf("Node misconfigured - %s", err.Error())
	}

	// grpc.Server
	intApiServer := grpc.NewServer()
	intapi.RegisterCoordinationServer(intApiServer, svcCoordination)
	intapi.RegisterReplicationServer(intApiServer, node)
	intapi.RegisterDelegationServer(intApiServer, node)

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
	return &Cluster{log, intApiServer, httpServer, node, cfg.Api.Http.Port, cfg.Cluster.Node.AddrIntApi}, nil
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
	lis, err := net.Listen("tcp", a.addrIntApi)
	if err != nil {
		return
	}
	go func() {
		err = a.intApiServer.Serve(lis)
		if err != nil {
			a.log.Errorf(err.Error())
		}
	}()
	return
}

func (a *Cluster) Stop() {
	a.httpServer.Shutdown()
	a.intApiServer.Stop()
	a.node.Stop()
}
