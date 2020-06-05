package http

import (
	"fmt"

	"github.com/valyala/fasthttp"
)

// Index serves the API's index page
func Index(ctx *fasthttp.RequestCtx) {
	fmt.Fprint(ctx, "Welcome!\n")
}
