package http

import (
	"github.com/fasthttp/router"

	"highvolume.io/shackle/internal/log"
	"highvolume.io/shackle/internal/repo"
)

// NewRouter returns a router
func NewRouter(
	log log.Logger,
	repoHash repo.Hash,
) *router.Router {
	lock := Lock{repoHash}

	r := router.New()
	r.GET("/", Index)
	r.POST("/lock", lock.ServeFastHTTP)

	return r
}
