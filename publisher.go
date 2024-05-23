package redisdk

import (
	"log"
	"time"
)

// DelayQueuePublisher only publishes messages to delayqueue, it is a encapsulation of delayqueue
type DelayQueuePublisher struct {
	inner *DelayQueue
}

// NewDelayQueuePublisher0 creates a new DelayQueuePublisher by a RedisCli instance
func NewDelayQueuePublisher0(name string, opts ...any) *DelayQueuePublisher {
	return &DelayQueuePublisher{
		inner: newDelayQueue(name, opts...),
	}
}

// NewDelayQueuePublisher creates a new DelayQueuePublisher by a *redis.Client
func NewDelayQueuePublisher(name string, opts ...any) *DelayQueuePublisher {
	return NewDelayQueuePublisher0(name, opts...)
}

// WithLogger customizes logger for queue
func (p *DelayQueuePublisher) WithLogger(logger *log.Logger) *DelayQueuePublisher {
	p.inner.logger = logger
	return p
}

// SendScheduleMsg submits a message delivered at given time
func (p *DelayQueuePublisher) SendScheduleMsg(payload string, t time.Time, opts ...any) error {
	return p.inner.SendScheduleMsg(payload, t, opts...)
}

// SendDelayMsg submits a message delivered after given duration
func (p *DelayQueuePublisher) SendDelayMsg(payload string, duration time.Duration, opts ...any) error {
	return p.inner.SendDelayMsg(payload, duration, opts...)
}
