package http

import (
	"github.com/fasthttp/router"

	"logbin.io/shackle/cluster"
	"logbin.io/shackle/log"
	"logbin.io/shackle/service"
)

// NewRouter returns a router
func NewRouter(
	log log.Logger,
	host cluster.Host,
	svcHash service.Hash,
) *router.Router {
	var (
		index    = Index{}
		lock     = Lock{host, svcHash}
		rollback = Rollback{host, svcHash}
		commit   = Commit{host, svcHash}
	)

	r := router.New()
	r.GET("/", index.ServeFastHTTP)
	r.POST("/lock", lock.ServeFastHTTP)
	r.POST("/rollback", rollback.ServeFastHTTP)
	r.POST("/commit", commit.ServeFastHTTP)

	return r
}
