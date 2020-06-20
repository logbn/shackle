package http

import (
	"fmt"

	"github.com/valyala/fasthttp"

	"highvolume.io/shackle/cluster"
	"highvolume.io/shackle/entity"
	"highvolume.io/shackle/service"
)

// Rollback accepts rollback requests via api
type Rollback struct {
	node    cluster.Node
	svcHash service.Hash
}

func (c *Rollback) ServeFastHTTP(ctx *fasthttp.RequestCtx) {
	// clientApp, clientID, batch, err := parseRequest(ctx, svcHash)
	_, _, batch, err := parseRequest(ctx, c.svcHash)
	if err != nil {
		return
	}
	res, err := c.node.Rollback(batch)
	if err != nil {
		ctx.Response.SetStatusCode(500)
		fmt.Fprintf(ctx, entity.ErrorToJson(err))
		return
	}
	out := entity.BatchResponseToJson(res)

	fmt.Fprintf(ctx, "%s", out)
}
