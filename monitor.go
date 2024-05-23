package redisdk

import (
	"log"
)

// DelayQueueMonitor can get running status and events of DelayQueue
type DelayQueueMonitor struct {
	inner *DelayQueue
}

// NewDelayQueueMonitor creates a new DelayQueue DelayQueueMonitor
func NewDelayQueueMonitor(name string, opts ...interface{}) *DelayQueueMonitor {
	return &DelayQueueMonitor{
		inner: NewDelayQueue(name, opts...),
	}
}

// WithLogger customizes logger for queue
func (m *DelayQueueMonitor) WithLogger(logger *log.Logger) *DelayQueueMonitor {
	m.inner.logger = logger
	return m
}

// GetPendingCount returns the number of messages which delivery time has not arrived
func (m *DelayQueueMonitor) GetPendingCount() (int64, error) {
	return m.inner.GetPendingCount()
}

// GetReadyCount returns the number of messages which have arrived delivery time but but have not been delivered yet
func (m *DelayQueueMonitor) GetReadyCount() (int64, error) {
	return m.inner.GetReadyCount()
}

// GetProcessingCount returns the number of messages which are being processed
func (m *DelayQueueMonitor) GetProcessingCount() (int64, error) {
	return m.inner.GetProcessingCount()
}

// ListenEvent register a listener which will be called when events occured in this queue
// so it can be used to monitor running status
// returns: close function, error
func (m *DelayQueueMonitor) ListenEvent(listener EventListener) (func(), error) {
	reportChan := genReportChannel(m.inner.name)
	sub, closer, err := redisCli.Subscribe(reportChan)
	if err != nil {
		return nil, err
	}
	go func() {
		for payload := range sub {
			event, err := decodeEvent(payload)
			if err != nil {
				m.inner.logger.Printf("[listen event] %v\n", event)
			} else {
				listener.OnEvent(event)
			}
		}
	}()
	return closer, nil
}
