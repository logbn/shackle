package http

import (
	"fmt"

	"github.com/valyala/fasthttp"

	"highvolume.io/shackle/internal/entity"
	"highvolume.io/shackle/internal/service"
)

// Commit accepts rollback requests via api
type Commit struct {
	svcPersistence service.Persistence
}

func (c *Commit) ServeFastHTTP(ctx *fasthttp.RequestCtx) {
	ctx.Response.Header.Set("content-type", "application/json")
	msg := ctx.Request.Body()
	// TODO: Auth and account retrieval for peppering
	batch, err := entity.BatchFromJson(msg)
	if err != nil {
		ctx.Response.SetStatusCode(500)
		fmt.Fprintf(ctx, err.Error())
		return
	}
	res, err := c.svcPersistence.Commit(batch)
	if err != nil {
		ctx.Response.SetStatusCode(500)
		fmt.Fprintf(ctx, err.Error())
		return
	}
	out := entity.BatchResponseToJson(res)

	fmt.Fprintf(ctx, "%s", out)
}
