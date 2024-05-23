package redisdk

import (
	"log"
	"os"
	"strconv"
	"testing"
)

func TestMonitor_get_status(t *testing.T) {
	InitClient("127.0.0.1:6379")
	size := 1000
	cb := func(s string) bool {
		return true
	}
	logger := log.New(os.Stderr, "[DelayQueue]", log.LstdFlags)
	queue := NewDelayQueue("test", cb)
	monitor := NewDelayQueueMonitor("test").WithLogger(logger)

	for i := 0; i < size; i++ {
		err := queue.SendDelayMsg(strconv.Itoa(i), 0)
		if err != nil {
			t.Error(err)
		}
	}

	// test pengding count
	pending, err := monitor.GetPendingCount()
	if err != nil {
		t.Error(err)
		return
	}
	if int(pending) != size {
		t.Errorf("execting %d, got %d", int(pending), size)
		return
	}

	// test ready count
	err = queue.pending2Ready()
	if err != nil {
		t.Errorf("consume error: %v", err)
		return
	}
	ready, err := monitor.GetReadyCount()
	if err != nil {
		t.Error(err)
		return
	}
	if int(ready) != size {
		t.Errorf("execting %d, got %d", int(pending), size)
		return
	}

	// test processing count
	for i := 0; i < size/2; i++ {
		_, _ = queue.ready2Unack()
	}
	processing, err := monitor.GetProcessingCount()
	if err != nil {
		t.Error(err)
		return
	}
	if int(processing) != size/2 {
		t.Errorf("execting %d, got %d", int(pending), size/2)
		return
	}
}

type MyProfiler struct {
	ProduceCount int
	DeliverCount int
	ConsumeCount int
	RetryCount   int
	FailCount    int
}

func (p *MyProfiler) OnEvent(event *Event) {
	switch event.Code {
	case NewMessageEvent:
		p.ProduceCount += event.MsgCount
	case DeliveredEvent:
		p.DeliverCount += event.MsgCount
	case AckEvent:
		p.ConsumeCount += event.MsgCount
	case RetryEvent:
		p.RetryCount += event.MsgCount
	case FinalFailedEvent:
		p.FailCount += event.MsgCount
	}
}

func TestMonitor_listener1(t *testing.T) {
	InitClient("127.0.0.1:6379")
	size := 1000
	cb := func(s string) bool {
		return true
	}
	queue := NewDelayQueue("test", cb)
	queue.EnableReport()
	monitor := NewDelayQueueMonitor("test")
	profile := &MyProfiler{}
	monitor.ListenEvent(profile)

	for i := 0; i < size; i++ {
		err := queue.SendDelayMsg(strconv.Itoa(i), 0)
		if err != nil {
			t.Error(err)
		}
	}
	queue.consume()

	if profile.ProduceCount != size {
		t.Error("wrong produce count")
	}
	if profile.DeliverCount != size {
		t.Error("wrong deliver count")
	}
	if profile.ConsumeCount != size {
		t.Error("wrong consume count")
	}
}

func TestMonitor_listener2(t *testing.T) {
	InitClient("127.0.0.1:6379")
	size := 1000
	cb := func(s string) bool {
		return false
	}
	queue := NewDelayQueue("test", cb).WithDefaultRetryCount(1)
	queue.EnableReport()
	monitor := NewDelayQueueMonitor("test")
	profile := &MyProfiler{}
	monitor.ListenEvent(profile)

	for i := 0; i < size; i++ {
		err := queue.SendDelayMsg(strconv.Itoa(i), 0)
		if err != nil {
			t.Error(err)
		}
	}
	for i := 0; i < 3; i++ {
		queue.consume()
	}

	if profile.RetryCount != size {
		t.Error("wrong deliver count")
	}
	if profile.FailCount != size {
		t.Error("wrong consume count")
	}
}
