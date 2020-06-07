package http

import (
	"fmt"

	"github.com/valyala/fasthttp"

	"highvolume.io/shackle/internal/entity"
	"highvolume.io/shackle/internal/service"
)

// Lock accepts lock requests via api
type Lock struct {
	svcPersistence service.Persistence
}

func (c *Lock) ServeFastHTTP(ctx *fasthttp.RequestCtx) {
	ctx.Response.Header.Set("content-type", "application/json")
	msg := ctx.Request.Body()
	// TODO: Auth and account retrieval for peppering
	batch, err := entity.LockBatchFromRequest(msg)
	if err != nil {
		ctx.Response.SetStatusCode(500)
		fmt.Fprintf(ctx, err.Error())
		return
	}
	res, err := c.svcPersistence.Lock(batch)
	if err != nil {
		ctx.Response.SetStatusCode(500)
		fmt.Fprintf(ctx, err.Error())
		return
	}
	out, err := entity.BatchResponseToJson(res)
	if err != nil {
		ctx.Response.SetStatusCode(500)
		fmt.Fprintf(ctx, err.Error())
		return
	}
	fmt.Fprintf(ctx, "%s", out)
}
