package http

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"

	"highvolume.io/shackle/internal/test/mock"
)

func TestCommit(t *testing.T) {
	var ctx fasthttp.RequestCtx
	svc := mock.ServicePersistence{}
	h := Commit{&svc}

	testJson := func(b []byte) {
		ctx.Request.Reset()
		ctx.Response.Reset()
		ctx.Request.SetRequestURI("/commit")
		ctx.Request.SetBodyString(string(b))
		ctx.Request.Header.SetContentType("application/json")
		h.ServeFastHTTP(&ctx)
	}

	t.Run("Success", func(t *testing.T) {
		testJson([]byte(`["a","b","c"]`))
		assert.Equal(t, 200, ctx.Response.StatusCode(), ctx.Response.Header.String())
		assert.Equal(t, "[1,1,1]", string(ctx.Response.Body()))
	})

	t.Run("Failure", func(t *testing.T) {
		testJson([]byte(`["a","b","c"`))
		assert.Equal(t, 500, ctx.Response.StatusCode(), ctx.Response.Header.String())

		testJson([]byte(``))
		assert.Equal(t, 500, ctx.Response.StatusCode(), ctx.Response.Header.String())

		// Mock returns err w/ batch size 7
		testJson([]byte(`["a","b","c","d","e","f","g"]`))
		assert.Equal(t, 500, ctx.Response.StatusCode(), ctx.Response.Header.String())
	})
}
