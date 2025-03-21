package redisdk

import (
	dgctx "github.com/darwinOrg/go-common/context"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/hdt3213/delayqueue"
	"time"
)

type DelayQueueCallback func(ctx *dgctx.DgContext, payload string) error

type DelayQueue struct {
	queue      *delayqueue.DelayQueue
	retryCount uint
}

func NewDelayQueue(name string, retryCount uint, callback DelayQueueCallback) *DelayQueue {
	queue := delayqueue.NewQueue(name, redisCli.(*redisV9Wrapper).inner, callback)

	return &DelayQueue{
		queue:      queue,
		retryCount: retryCount,
	}
}

func (d *DelayQueue) SendDelayMsg(ctx *dgctx.DgContext, payload string, duration time.Duration) error {
	_, err := d.queue.SendDelayMsgV2(payload, duration, d.retryCount)
	if err != nil {
		dglogger.Errorf(ctx, "send delay msg error: %v", err)
		return err
	}

	return nil
}
