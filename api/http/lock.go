package http

import (
	"fmt"

	"github.com/valyala/fasthttp"

	"highvolume.io/shackle/cluster"
	"highvolume.io/shackle/entity"
	"highvolume.io/shackle/service"
)

// Lock accepts lock requests via api
type Lock struct {
	node    cluster.Node
	svcHash service.Hash
}

func (c *Lock) ServeFastHTTP(ctx *fasthttp.RequestCtx) {
	_, _, batch, err := parseRequest(ctx, c.svcHash)
	if err != nil {
		return
	}
	res, err := c.node.Lock(batch)
	if err != nil {
		ctx.Response.SetStatusCode(500)
		fmt.Fprintf(ctx, entity.ErrorToJson(err))
		return
	}
	out := entity.BatchResponseToJson(res)

	fmt.Fprintf(ctx, "%s", out)
}