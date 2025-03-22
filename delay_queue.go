package redisdk

import (
	dgctx "github.com/darwinOrg/go-common/context"
	"github.com/darwinOrg/go-common/utils"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/hdt3213/delayqueue"
	"time"
)

type delayMsg struct {
	Ctx     *dgctx.DgContext `json:"ctx"`
	Payload string           `json:"payload"`
}

type DelayQueueCallback func(ctx *dgctx.DgContext, payload string) bool

type DelayQueue struct {
	queue      *delayqueue.DelayQueue
	retryCount int
}

func NewDelayQueue(name string) *DelayQueue {
	queue := delayqueue.NewQueue(name, redisCli)

	return &DelayQueue{
		queue: queue,
	}
}

func (d *DelayQueue) SendDelayMsg(ctx *dgctx.DgContext, payload string, duration time.Duration) error {
	_, err := d.queue.SendDelayMsgV2(utils.MustConvertBeanToJsonString(&delayMsg{Ctx: ctx, Payload: payload}), duration)
	if err != nil {
		dglogger.Errorf(ctx, "send delay msg error: %v", err)
		return err
	}
	dglogger.Infof(ctx, "send delay msg success: %s", payload)
	return nil
}

func (d *DelayQueue) StartConsume(callback DelayQueueCallback) <-chan struct{} {
	d.queue.WithCallback(func(payload string) bool {
		dm := utils.MustConvertJsonStringToBean[delayMsg](payload)
		return callback(dm.Ctx, dm.Payload)
	})

	return d.queue.StartConsume()
}
