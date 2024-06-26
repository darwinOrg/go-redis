package redisdk

import (
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestPublisher(t *testing.T) {
	InitClient("127.0.0.1:6379")
	size := 1000
	retryCount := 3
	deliveryCount := make(map[string]int)
	cb := func(s string) bool {
		deliveryCount[s]++
		i, _ := strconv.ParseInt(s, 10, 64)
		return i%2 == 0
	}
	logger := log.New(os.Stderr, "[DelayQueue]", log.LstdFlags)
	queue := NewDelayQueue("test", cb).WithLogger(logger)
	publisher := NewDelayQueuePublisher("test").WithLogger(logger)

	for i := 0; i < size; i++ {
		err := publisher.SendDelayMsg(strconv.Itoa(i), 0, WithRetryCount(retryCount), WithMsgTTL(time.Hour))
		if err != nil {
			t.Error(err)
		}
	}
	for i := 0; i < 10*size; i++ {
		err := queue.consume()
		if err != nil {
			t.Errorf("consume error: %v", err)
			return
		}
	}
	for k, v := range deliveryCount {
		i, _ := strconv.ParseInt(k, 10, 64)
		if i%2 == 0 {
			if v != 1 {
				t.Errorf("expect 1 delivery, actual %d", v)
			}
		} else {
			if v != retryCount+1 {
				t.Errorf("expect %d delivery, actual %d", retryCount+1, v)
			}
		}
	}
}
