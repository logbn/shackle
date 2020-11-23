package http

import (
	"fmt"

	"github.com/valyala/fasthttp"

	"logbin.io/shackle/entity"
	"logbin.io/shackle/service"
)

func parseRequest(ctx *fasthttp.RequestCtx, svcHash service.Hash) (clientApp, clientID []byte, batch entity.Batch, err error) {
	ctx.Response.Header.Set("content-type", "application/json")
	clientApp = ctx.Request.Header.Peek("shackle-client-app")
	if len(clientApp) == 0 {
		err = fmt.Errorf("shackle-client-app header is required")
		ctx.Response.SetStatusCode(401)
		fmt.Fprintf(ctx, entity.ErrorToJson(err))
		return
	}
	clientID = ctx.Request.Header.Peek("shackle-client-id")
	if len(clientID) == 0 {
		err = fmt.Errorf("shackle-client-id header is required")
		ctx.Response.SetStatusCode(401)
		fmt.Fprintf(ctx, entity.ErrorToJson(err))
		return
	}
	var (
		contentType = ctx.Request.Header.Peek("content-type")
		bucket      = ctx.Request.Header.Peek("shackle-bucket")
		msg         = ctx.Request.Body()
	)
	batch, err = entity.BatchFromRequest(msg, contentType, bucket, svcHash)
	if err != nil {
		if entity.IsValidation(err) {
			ctx.Response.SetStatusCode(400)
			fmt.Fprintf(ctx, entity.ErrorToJson(err))
			return
		}
		ctx.Response.SetStatusCode(500)
		fmt.Fprintf(ctx, entity.ErrorToJson(err))
		return
	}
	return
}
