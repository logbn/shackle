package http

import (
	"fmt"

	"github.com/valyala/fasthttp"
)

// Index serves the API's index page
type Index struct{}

func (c *Index) ServeFastHTTP(ctx *fasthttp.RequestCtx) {
	fmt.Fprint(ctx, "Welcome!")
}
