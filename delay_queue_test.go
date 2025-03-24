package redisdk

import (
	dgctx "github.com/darwinOrg/go-common/context"
	dgsys "github.com/darwinOrg/go-common/sys"
	dglogger "github.com/darwinOrg/go-logger"
	"testing"
	"time"
)

func TestSendDelayMsg(t *testing.T) {
	InitClient("127.0.0.1:6379")
	ctx := dgctx.SimpleDgContext()
	dq := NewDelayQueue("test")
	_ = dq.StartConsume(func(ctx *dgctx.DgContext, payload string) bool {
		dglogger.Infof(ctx, "received delay msg: %s", payload)
		return true
	})
	_ = dq.SendDelayMsg(ctx, "test msg", 3*time.Second)
	dgsys.HangupApplication()
}
