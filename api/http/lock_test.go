package http

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"

	"highvolume.io/shackle/entity"
	"highvolume.io/shackle/test/mock"
	"highvolume.io/shackle/test/mock/mockcluster"
)

func TestLock(t *testing.T) {
	var ctx fasthttp.RequestCtx
	svc := mock.ServicePersistence{}
	h := Lock{&mockcluster.Node{&svc}, &mock.ServiceHash{}}

	testJson := func(b []byte) {
		ctx.Request.Reset()
		ctx.Response.Reset()
		ctx.Request.Header.Set("shackle-client-app", "test")
		ctx.Request.Header.Set("shackle-client-id", "test-1")
		ctx.Request.SetRequestURI("/lock")
		ctx.Request.SetBodyString(string(b))
		ctx.Request.Header.SetContentType("application/json")
		h.ServeFastHTTP(&ctx)
	}

	t.Run("Success", func(t *testing.T) {
		testJson([]byte(`["a","b","c"]`))
		assert.Equal(t, 200, ctx.Response.StatusCode(), ctx.Response.Header.String())
		expected := fmt.Sprintf("[%d,%d,%d]", entity.ITEM_LOCKED, entity.ITEM_LOCKED, entity.ITEM_LOCKED)
		assert.Equal(t, expected, string(ctx.Response.Body()))

		testJson([]byte(`[]`))
		assert.Equal(t, 200, ctx.Response.StatusCode(), ctx.Response.Header.String())
		assert.Equal(t, "[]", string(ctx.Response.Body()))
	})

	t.Run("Failure", func(t *testing.T) {
		testJson([]byte(`["a","b","c"`))
		assert.Equal(t, 400, ctx.Response.StatusCode(), ctx.Response.Header.String())

		testJson([]byte(``))
		assert.Equal(t, 400, ctx.Response.StatusCode(), ctx.Response.Header.String())

		// Mock returns err w/ batch size 7
		testJson([]byte(`["a","b","c","d","e","f","g"]`))
		assert.Equal(t, 500, ctx.Response.StatusCode(), ctx.Response.Header.String())
	})
}
