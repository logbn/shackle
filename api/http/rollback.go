package http

import (
	"fmt"

	"github.com/valyala/fasthttp"

	"logbin.io/shackle/cluster"
	"logbin.io/shackle/entity"
	"logbin.io/shackle/service"
)

// Rollback accepts rollback requests via api
type Rollback struct {
	host    cluster.Host
	svcHash service.Hash
}

func (c *Rollback) ServeFastHTTP(ctx *fasthttp.RequestCtx) {
	// clientApp, clientID, batch, err := parseRequest(ctx, svcHash)
	_, _, batch, err := parseRequest(ctx, c.svcHash)
	if err != nil {
		return
	}
	res, err := c.host.Rollback(batch)
	if err != nil {
		ctx.Response.SetStatusCode(500)
		fmt.Fprintf(ctx, entity.ErrorToJson(err))
		return
	}
	out := entity.BatchResponseToJson(res)

	fmt.Fprintf(ctx, "%s", out)
}
