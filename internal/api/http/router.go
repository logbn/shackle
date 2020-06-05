package http

import (
	"github.com/fasthttp/router"

	"highvolume.io/shackle/internal/log"
)

// NewRouter returns a router
func NewRouter(
	log log.Logger,
) *router.Router {
	r := router.New()
	r.GET("/", Index)
	r.POST("/lock", Lock)

	return r
}
