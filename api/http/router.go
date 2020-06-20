package http

import (
	"github.com/fasthttp/router"

	"highvolume.io/shackle/cluster"
	"highvolume.io/shackle/log"
	"highvolume.io/shackle/service"
)

// NewRouter returns a router
func NewRouter(
	log log.Logger,
	node cluster.Node,
	svcHash service.Hash,
) *router.Router {
	var (
		index    = Index{}
		lock     = Lock{node, svcHash}
		rollback = Rollback{node, svcHash}
		commit   = Commit{node, svcHash}
	)

	r := router.New()
	r.GET("/", index.ServeFastHTTP)
	r.POST("/lock", lock.ServeFastHTTP)
	r.POST("/rollback", rollback.ServeFastHTTP)
	r.POST("/commit", commit.ServeFastHTTP)

	return r
}
