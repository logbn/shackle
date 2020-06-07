package http

import (
	"github.com/fasthttp/router"

	"highvolume.io/shackle/internal/log"
	"highvolume.io/shackle/internal/service"
)

// NewRouter returns a router
func NewRouter(
	log log.Logger,
	svcPersistence service.Persistence,
) *router.Router {
	lock := Lock{svcPersistence}
	rollback := Rollback{svcPersistence}
	// commit := Commit{svcPersistence}

	r := router.New()
	r.GET("/", Index)
	r.POST("/lock", lock.ServeFastHTTP)
	r.POST("/rollback", rollback.ServeFastHTTP)
	// r.POST("/commit", commit.ServeFastHTTP)

	return r
}
