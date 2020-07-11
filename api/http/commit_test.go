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

func TestCommit(t *testing.T) {
	var ctx fasthttp.RequestCtx
	svc := mock.ServicePersistence{}
	h := Commit{&mockcluster.Host{SvcPersistence: &svc}, &mock.ServiceHash{}}

	testJson := func(b []byte, withHdrApp, withHdrID, withHdrType bool) {
		ctx.Request.Reset()
		ctx.Response.Reset()
		if withHdrApp {
			ctx.Request.Header.Set("shackle-client-app", "test")
		}
		if withHdrID {
			ctx.Request.Header.Set("shackle-client-id", "test-1")
		}
		ctx.Request.SetRequestURI("/commit")
		ctx.Request.SetBodyString(string(b))
		ctx.Request.Header.SetContentType("application/json")
		h.ServeFastHTTP(&ctx)
	}

	t.Run("Success", func(t *testing.T) {
		testJson([]byte(`["a","b","c"]`), true, true, true)
		assert.Equal(t, 200, ctx.Response.StatusCode(), ctx.Response.Header.String())
		expected := fmt.Sprintf("[%d,%d,%d]", entity.ITEM_EXISTS, entity.ITEM_EXISTS, entity.ITEM_EXISTS)
		assert.Equal(t, expected, string(ctx.Response.Body()))
	})
	t.Run("Malformed JSON", func(t *testing.T) {
		testJson([]byte(`["a","b","c"`), true, true, true)
		assert.Equal(t, 400, ctx.Response.StatusCode(), ctx.Response.Header.String())
	})
	t.Run("Empty Json", func(t *testing.T) {
		testJson([]byte(``), true, true, true)
		assert.Equal(t, 400, ctx.Response.StatusCode(), ctx.Response.Header.String())
	})
	t.Run("No Content Type", func(t *testing.T) {
		testJson([]byte(`["a","b","c"`), true, true, false)
		assert.Equal(t, 400, ctx.Response.StatusCode(), ctx.Response.Header.String())
	})
	t.Run("No app or id header", func(t *testing.T) {
		testJson([]byte(`["a","b","c"]`), false, false, true)
		assert.Equal(t, 401, ctx.Response.StatusCode(), ctx.Response.Header.String())
	})
	t.Run("No app header", func(t *testing.T) {
		testJson([]byte(`["a","b","c"]`), false, true, true)
		assert.Equal(t, 401, ctx.Response.StatusCode(), ctx.Response.Header.String())
	})
	t.Run("No id header", func(t *testing.T) {
		testJson([]byte(`["a","b","c"]`), true, false, true)
		assert.Equal(t, 401, ctx.Response.StatusCode(), ctx.Response.Header.String())
	})
	t.Run("Persistence Error", func(t *testing.T) {
		// Mock returns err w/ batch size 7
		testJson([]byte(`["a","b","c","d","e","f","g"]`), true, true, true)
		assert.Equal(t, 500, ctx.Response.StatusCode(), ctx.Response.Header.String())
	})
}
