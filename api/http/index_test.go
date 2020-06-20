package http

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
)

func TestIndex(t *testing.T) {
	var ctx fasthttp.RequestCtx
	h := Index{}

	test := func() {
		ctx.Request.Reset()
		ctx.Response.Reset()
		ctx.Request.SetRequestURI("/")
		h.ServeFastHTTP(&ctx)
	}

	t.Run("Success", func(t *testing.T) {
		test()
		assert.Equal(t, 200, ctx.Response.StatusCode(), ctx.Response.Header.String())
		assert.Equal(t, "Welcome!", string(ctx.Response.Body()))
	})
}
