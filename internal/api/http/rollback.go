package http

import (
	"fmt"

	"github.com/valyala/fasthttp"

	"highvolume.io/shackle/internal/entity"
	"highvolume.io/shackle/internal/service"
)

// Rollback accepts rollback requests via api
type Rollback struct {
	svcPersistence service.Persistence
}

func (c *Rollback) ServeFastHTTP(ctx *fasthttp.RequestCtx) {
	ctx.Response.Header.Set("content-type", "application/json")
	msg := ctx.Request.Body()
	// TODO: Auth and account retrieval for peppering
	batch, err := entity.BatchFromJson(msg)
	if err != nil {
		ctx.Response.SetStatusCode(500)
		fmt.Fprintf(ctx, err.Error())
		return
	}
	res, err := c.svcPersistence.Rollback(batch)
	if err != nil {
		ctx.Response.SetStatusCode(500)
		fmt.Fprintf(ctx, err.Error())
		return
	}
	out := entity.BatchResponseToJson(res)

	fmt.Fprintf(ctx, "%s", out)
}
