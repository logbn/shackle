package http

import (
	"fmt"

	"github.com/valyala/fasthttp"

	"highvolume.io/shackle/cluster"
	"highvolume.io/shackle/entity"
	"highvolume.io/shackle/service"
)

// Commit accepts rollback requests via api
type Commit struct {
	host    cluster.Host
	svcHash service.Hash
}

func (c *Commit) ServeFastHTTP(ctx *fasthttp.RequestCtx) {
	_, _, batch, err := parseRequest(ctx, c.svcHash)
	if err != nil {
		return
	}
	res, err := c.host.Commit(batch)
	if err != nil {
		ctx.Response.SetStatusCode(500)
		fmt.Fprintf(ctx, entity.ErrorToJson(err))
		return
	}
	out := entity.BatchResponseToJson(res)

	fmt.Fprintf(ctx, "%s", out)
}
