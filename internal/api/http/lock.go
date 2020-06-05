package http

import (
	"fmt"

	"github.com/valyala/fasthttp"

	"highvolume.io/shackle/internal/entity"
)

// Lock accepts lock requests via api
func Lock(ctx *fasthttp.RequestCtx) {
	ctx.Response.Header.Set("content-type", "application/json")
	msg := ctx.Request.Body()
	// TODO: Auth and account retrieval for peppering
	batch, err := entity.LockBatchFromRequest(msg)
	if err != nil {
		ctx.Response.SetStatusCode(500)
		fmt.Fprintf(ctx, err.Error())
		return
	}
	fmt.Fprintf(ctx, "%x", batch)
}
