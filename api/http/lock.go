package http

import (
	"fmt"

	"github.com/valyala/fasthttp"

	"logbin.io/shackle/cluster"
	"logbin.io/shackle/entity"
	"logbin.io/shackle/service"
)

// Lock accepts lock requests via api
type Lock struct {
	host    cluster.Host
	svcHash service.Hash
}

func (c *Lock) ServeFastHTTP(ctx *fasthttp.RequestCtx) {
	_, _, batch, err := parseRequest(ctx, c.svcHash)
	if err != nil {
		return
	}
	res, err := c.host.Lock(batch)
	if err != nil {
		ctx.Response.SetStatusCode(500)
		fmt.Fprintf(ctx, entity.ErrorToJson(err))
		return
	}
	out := entity.BatchResponseToJson(res)

	fmt.Fprintf(ctx, "%s", out)
}
