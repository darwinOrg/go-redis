package redisdk

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"log"
	"strconv"
	"sync"
	"time"
)

// DelayQueue is a message queue supporting delayed/scheduled delivery based on redis
type DelayQueue struct {
	// name for this Queue. Make sure the name is unique in redis database
	name          string
	cb            func(string) bool
	pendingKey    string // sorted set: message id -> delivery time
	readyKey      string // list
	unAckKey      string // sorted set: message id -> retry time
	retryKey      string // list
	retryCountKey string // hash: message id -> remain retry count
	garbageKey    string // set: message id
	useHashTag    bool
	ticker        *time.Ticker
	logger        *log.Logger
	close         chan struct{}

	maxConsumeDuration time.Duration // default 5 seconds
	msgTTL             time.Duration // default 1 hour
	defaultRetryCount  uint          // default 3
	fetchInterval      time.Duration // default 1 second
	fetchLimit         uint          // default no limit
	concurrent         uint          // default 1, executed serially

	eventListener EventListener
}

// NewDelayQueue creates a new queue, use DelayQueue.StartConsume to consume or DelayQueue.SendScheduleMsg to publish message
//
//	 queue := delayqueue.NewQueue("example", redisCli, func(payload string) bool {
//	     // callback returns true to confirm successful consumption.
//	     // If callback returns false or not return within maxConsumeDuration, DelayQueue will re-deliver this message
//			return true
//		})
func NewDelayQueue(name string, opts ...any) *DelayQueue {
	return newDelayQueue(name, opts...)
}

func NewDelayQueueOnCluster(name string, opts ...any) *DelayQueue {
	opts = append(opts, UseHashTagKey())
	return newDelayQueue(name, opts...)
}

// newDelayQueue creates a new queue, use DelayQueue.StartConsume to consume or DelayQueue.SendScheduleMsg to publish message
// callback returns true to confirm successful consumption. If callback returns false or not return within maxConsumeDuration, DelayQueue will re-deliver this message
func newDelayQueue(name string, opts ...any) *DelayQueue {
	if name == "" {
		panic("name is required")
	}
	if redisCli == nil {
		panic("cli is required")
	}
	useHashTag := false
	var callback CallbackFunc = nil
	for _, opt := range opts {
		switch o := opt.(type) {
		case hashTagKeyOpt:
			useHashTag = true
		case CallbackFunc:
			callback = o
		}
	}
	var keyPrefix string
	if useHashTag {
		keyPrefix = "{dq:" + name + "}"
	} else {
		keyPrefix = "dq:" + name
	}
	return &DelayQueue{
		name:               name,
		cb:                 callback,
		pendingKey:         keyPrefix + ":pending",
		readyKey:           keyPrefix + ":ready",
		unAckKey:           keyPrefix + ":unack",
		retryKey:           keyPrefix + ":retry",
		retryCountKey:      keyPrefix + ":retry:cnt",
		garbageKey:         keyPrefix + ":garbage",
		useHashTag:         useHashTag,
		close:              nil,
		maxConsumeDuration: 5 * time.Second,
		msgTTL:             time.Hour,
		logger:             log.Default(),
		defaultRetryCount:  3,
		fetchInterval:      time.Second,
		concurrent:         1,
	}
}

// WithCallback set callback for queue to receives and consumes messages
// callback returns true to confirm successfully consumed, false to re-deliver this message
func (q *DelayQueue) WithCallback(callback CallbackFunc) *DelayQueue {
	q.cb = callback
	return q
}

// WithLogger customizes logger for queue
func (q *DelayQueue) WithLogger(logger *log.Logger) *DelayQueue {
	q.logger = logger
	return q
}

// WithFetchInterval customizes the interval at which consumer fetch message from redis
func (q *DelayQueue) WithFetchInterval(d time.Duration) *DelayQueue {
	q.fetchInterval = d
	return q
}

// WithMaxConsumeDuration customizes max consume duration
// If no acknowledge received within WithMaxConsumeDuration after message delivery, DelayQueue will try to deliver this message again
func (q *DelayQueue) WithMaxConsumeDuration(d time.Duration) *DelayQueue {
	q.maxConsumeDuration = d
	return q
}

// WithFetchLimit limits the max number of processing messages, 0 means no limit
func (q *DelayQueue) WithFetchLimit(limit uint) *DelayQueue {
	q.fetchLimit = limit
	return q
}

// WithConcurrent sets the number of concurrent consumers
func (q *DelayQueue) WithConcurrent(c uint) *DelayQueue {
	if c == 0 {
		return q
	}
	q.concurrent = c
	return q
}

// WithDefaultRetryCount customizes the max number of retry, it effects of messages in this queue
// use WithRetryCount during DelayQueue.SendScheduleMsg or DelayQueue.SendDelayMsg to specific retry count of particular message
func (q *DelayQueue) WithDefaultRetryCount(count uint) *DelayQueue {
	q.defaultRetryCount = count
	return q
}

func (q *DelayQueue) genMsgKey(idStr string) string {
	if q.useHashTag {
		return "{dq:" + q.name + "}" + ":msg:" + idStr
	}
	return "dq:" + q.name + ":msg:" + idStr
}

type retryCountOpt int

// WithRetryCount set retry count for a msg
// example: queue.SendDelayMsg(payload, duration, delayqueue.WithRetryCount(3))
func WithRetryCount(count int) any {
	return retryCountOpt(count)
}

type msgTTLOpt time.Duration

// WithMsgTTL set ttl for a msg
// example: queue.SendDelayMsg(payload, duration, delayqueue.WithMsgTTL(Hour))
func WithMsgTTL(d time.Duration) any {
	return msgTTLOpt(d)
}

// SendScheduleMsg submits a message delivered at given time
func (q *DelayQueue) SendScheduleMsg(payload string, t time.Time, opts ...any) error {
	// parse options
	retryCount := q.defaultRetryCount
	for _, opt := range opts {
		switch o := opt.(type) {
		case retryCountOpt:
			retryCount = uint(o)
		case msgTTLOpt:
			q.msgTTL = time.Duration(o)
		}
	}
	// generate id
	idStr := uuid.Must(uuid.NewRandom()).String()
	now := time.Now()
	// store msg
	msgTTL := t.Sub(now) + q.msgTTL // delivery + q.msgTTL
	_, err := redisCli.Set(q.genMsgKey(idStr), payload, msgTTL)
	if err != nil {
		return fmt.Errorf("store msg failed: %v", err)
	}
	// store retry count
	err = redisCli.HSet(q.retryCountKey, idStr, strconv.Itoa(int(retryCount)))
	if err != nil {
		return fmt.Errorf("store retry count failed: %v", err)
	}
	// put to pending
	err = redisCli.ZAdd(q.pendingKey, map[string]float64{idStr: float64(t.Unix())})
	if err != nil {
		return fmt.Errorf("push to pending failed: %v", err)
	}
	q.reportEvent(NewMessageEvent, 1)
	return nil
}

// SendDelayMsg submits a message delivered after given duration
func (q *DelayQueue) SendDelayMsg(payload string, duration time.Duration, opts ...any) error {
	t := time.Now().Add(duration)
	return q.SendScheduleMsg(payload, t, opts...)
}

// pending2ReadyScript atomically moves messages from pending to ready
// keys: pendingKey, readyKey
// argv: currentTime
// returns: ready message number
const pending2ReadyScript = `
local msgs = redis.call('ZRangeByScore', KEYS[1], '0', ARGV[1])  -- get ready msg
if (#msgs == 0) then return end
local args2 = {} -- keys to push into ready
for _,v in ipairs(msgs) do
	table.insert(args2, v) 
    if (#args2 == 4000) then
		redis.call('LPush', KEYS[2], unpack(args2))
		args2 = {}
	end
end
if (#args2 > 0) then 
	redis.call('LPush', KEYS[2], unpack(args2))
end
redis.call('ZRemRangeByScore', KEYS[1], '0', ARGV[1])  -- remove msgs from pending
return #msgs
`

func (q *DelayQueue) pending2Ready() error {
	now := time.Now().Unix()
	keys := []string{q.pendingKey, q.readyKey}
	raw, err := redisCli.Eval(pending2ReadyScript, keys, []any{now})
	if err != nil && err != NilErr {
		return fmt.Errorf("pending2ReadyScript failed: %v", err)
	}
	count, ok := raw.(int64)
	if ok {
		q.reportEvent(ReadyEvent, int(count))
	}
	return nil
}

// ready2UnackScript atomically moves messages from ready to unack
// keys: readyKey/retryKey, unackKey
// argv: retryTime
const ready2UnackScript = `
local msg = redis.call('RPop', KEYS[1])
if (not msg) then return end
redis.call('ZAdd', KEYS[2], ARGV[1], msg)
return msg
`

func (q *DelayQueue) ready2Unack() (string, error) {
	retryTime := time.Now().Add(q.maxConsumeDuration).Unix()
	keys := []string{q.readyKey, q.unAckKey}
	ret, err := redisCli.Eval(ready2UnackScript, keys, []any{retryTime})
	if errors.Is(err, NilErr) {
		return "", err
	}
	if err != nil {
		return "", fmt.Errorf("ready2UnackScript failed: %v", err)
	}
	str, ok := ret.(string)
	if !ok {
		return "", fmt.Errorf("illegal result: %#v", ret)
	}
	q.reportEvent(DeliveredEvent, 1)
	return str, nil
}

func (q *DelayQueue) retry2Unack() (string, error) {
	retryTime := time.Now().Add(q.maxConsumeDuration).Unix()
	keys := []string{q.retryKey, q.unAckKey}
	ret, err := redisCli.Eval(ready2UnackScript, keys, []any{retryTime, q.retryKey, q.unAckKey})
	if errors.Is(err, NilErr) {
		return "", NilErr
	}
	if err != nil {
		return "", fmt.Errorf("ready2UnackScript failed: %v", err)
	}
	str, ok := ret.(string)
	if !ok {
		return "", fmt.Errorf("illegal result: %#v", ret)
	}
	return str, nil
}

func (q *DelayQueue) callback(idStr string) error {
	payload, err := redisCli.Get(q.genMsgKey(idStr))
	if errors.Is(err, NilErr) {
		return nil
	}
	if err != nil {
		// Is an IO error?
		return fmt.Errorf("get message payload failed: %v", err)
	}
	ack := q.cb(payload)
	if ack {
		err = q.ack(idStr)
	} else {
		err = q.nack(idStr)
	}
	return err
}

// batchCallback calls DelayQueue.callback in batch. callback is executed concurrently according to property DelayQueue.concurrent
// batchCallback must wait all callback finished, otherwise the actual number of processing messages may beyond DelayQueue.FetchLimit
func (q *DelayQueue) batchCallback(ids []string) {
	if len(ids) == 1 || q.concurrent == 1 {
		for _, id := range ids {
			err := q.callback(id)
			if err != nil {
				q.logger.Printf("consume msg %s failed: %v", id, err)
			}
		}
		return
	}
	ch := make(chan string, len(ids))
	for _, id := range ids {
		ch <- id
	}
	close(ch)
	wg := sync.WaitGroup{}
	concurrent := int(q.concurrent)
	if concurrent > len(ids) { // too many goroutines is no use
		concurrent = len(ids)
	}
	wg.Add(concurrent)
	for i := 0; i < concurrent; i++ {
		go func() {
			defer wg.Done()
			for id := range ch {
				err := q.callback(id)
				if err != nil {
					q.logger.Printf("consume msg %s failed: %v", id, err)
				}
			}
		}()
	}
	wg.Wait()
}

func (q *DelayQueue) ack(idStr string) error {
	err := redisCli.ZRem(q.unAckKey, idStr)
	if err != nil {
		return fmt.Errorf("remove from unack failed: %v", err)
	}
	// msg key has ttl, ignore result of delete
	_ = redisCli.Del(q.genMsgKey(idStr))
	_ = redisCli.HDel(q.retryCountKey, idStr)
	q.reportEvent(AckEvent, 1)
	return nil
}

func (q *DelayQueue) nack(idStr string) error {
	// update retry time as now, unack2Retry will move it to retry immediately
	err := redisCli.ZAdd(q.unAckKey, map[string]float64{
		idStr: float64(time.Now().Unix()),
	})
	if err != nil {
		return fmt.Errorf("negative ack failed: %v", err)
	}
	q.reportEvent(NackEvent, 1)
	return nil
}

// unack2RetryScript atomically moves messages from unack to retry which remaining retry count greater than 0,
// and moves messages from unack to garbage which  retry count is 0
// Because DelayQueue cannot determine garbage message before eval unack2RetryScript, so it cannot pass keys parameter to redisCli.Eval
// Therefore unack2RetryScript moves garbage message to garbageKey instead of deleting directly
// keys: unackKey, retryCountKey, retryKey, garbageKey
// argv: currentTime
// returns: {retryMsgs, failMsgs}
const unack2RetryScript = `
local unack2retry = function(msgs)
	local retryCounts = redis.call('HMGet', KEYS[2], unpack(msgs)) -- get retry count
	local retryMsgs = 0
	local failMsgs = 0
	for i,v in ipairs(retryCounts) do
		local k = msgs[i]
		if v ~= false and v ~= nil and v ~= '' and tonumber(v) > 0 then
			redis.call("HIncrBy", KEYS[2], k, -1) -- reduce retry count
			redis.call("LPush", KEYS[3], k) -- add to retry
			retryMsgs = retryMsgs + 1
		else
			redis.call("HDel", KEYS[2], k) -- del retry count
			redis.call("SAdd", KEYS[4], k) -- add to garbage
			failMsgs = failMsgs + 1
		end
	end
	return retryMsgs, failMsgs
end

local retryMsgs = 0
local failMsgs = 0
local msgs = redis.call('ZRangeByScore', KEYS[1], '0', ARGV[1])  -- get retry msg
if (#msgs == 0) then return end
if #msgs < 4000 then
	local d1, d2 = unack2retry(msgs)
	retryMsgs = retryMsgs + d1
	failMsgs = failMsgs + d2
else
	local buf = {}
	for _,v in ipairs(msgs) do
		table.insert(buf, v)
		if #buf == 4000 then
		    local d1, d2 = unack2retry(buf)
			retryMsgs = retryMsgs + d1
			failMsgs = failMsgs + d2
			buf = {}
		end
	end
	if (#buf > 0) then
		local d1, d2 = unack2retry(buf)
		retryMsgs = retryMsgs + d1
		failMsgs = failMsgs + d2
	end
end
redis.call('ZRemRangeByScore', KEYS[1], '0', ARGV[1])  -- remove msgs from unack
return {retryMsgs, failMsgs}
`

func (q *DelayQueue) unack2Retry() error {
	keys := []string{q.unAckKey, q.retryCountKey, q.retryKey, q.garbageKey}
	now := time.Now()
	raw, err := redisCli.Eval(unack2RetryScript, keys, []any{now.Unix()})
	if err != nil && err != NilErr {
		return fmt.Errorf("unack to retry script failed: %v", err)
	}
	infos, ok := raw.([]any)
	if ok && len(infos) == 2 {
		retryCount, ok := infos[0].(int64)
		if ok {
			q.reportEvent(RetryEvent, int(retryCount))
		}
		failCount, ok := infos[1].(int64)
		if ok {
			q.reportEvent(FinalFailedEvent, int(failCount))
		}
	}
	return nil
}

func (q *DelayQueue) garbageCollect() error {
	msgIds, err := redisCli.SMembers(q.garbageKey)
	if err != nil {
		return fmt.Errorf("smembers failed: %v", err)
	}
	if len(msgIds) == 0 {
		return nil
	}
	// allow concurrent clean
	msgKeys := make([]string, 0, len(msgIds))
	for _, idStr := range msgIds {
		msgKeys = append(msgKeys, q.genMsgKey(idStr))
	}
	err = redisCli.Del(msgKeys...)
	if err != nil && err != NilErr {
		return fmt.Errorf("del msgs failed: %v", err)
	}
	err = redisCli.SRem(q.garbageKey, msgIds...)
	if err != nil && err != NilErr {
		return fmt.Errorf("remove from garbage key failed: %v", err)
	}
	return nil
}

func (q *DelayQueue) consume() error {
	// pending to ready
	err := q.pending2Ready()
	if err != nil {
		return err
	}
	// consume
	ids := make([]string, 0, q.fetchLimit)
	for {
		idStr, err := q.ready2Unack()
		if errors.Is(err, NilErr) { // consumed all
			break
		}
		if err != nil {
			return err
		}
		ids = append(ids, idStr)
		if q.fetchLimit > 0 && len(ids) >= int(q.fetchLimit) {
			break
		}
	}
	if len(ids) > 0 {
		q.batchCallback(ids)
	}
	// unack to retry
	err = q.unack2Retry()
	if err != nil {
		return err
	}
	err = q.garbageCollect()
	if err != nil {
		return err
	}
	// retry
	ids = make([]string, 0, q.fetchLimit)
	for {
		idStr, err := q.retry2Unack()
		if errors.Is(err, NilErr) { // consumed all
			break
		}
		if err != nil {
			return err
		}
		ids = append(ids, idStr)
		if q.fetchLimit > 0 && len(ids) >= int(q.fetchLimit) {
			break
		}
	}
	if len(ids) > 0 {
		q.batchCallback(ids)
	}
	return nil
}

// StartConsume creates a goroutine to consume message from DelayQueue
// use `<-done` to wait consumer stopping
// If there is no callback set, StartConsume will panic
func (q *DelayQueue) StartConsume() (done <-chan struct{}) {
	if q.cb == nil {
		panic("this instance has no callback")
	}
	q.close = make(chan struct{}, 1)
	q.ticker = time.NewTicker(q.fetchInterval)
	done0 := make(chan struct{})
	go func() {
	tickerLoop:
		for {
			select {
			case <-q.ticker.C:
				err := q.consume()
				if err != nil {
					log.Printf("consume error: %v", err)
				}
			case <-q.close:
				break tickerLoop
			}
		}
		close(done0)
	}()
	return done0
}

// StopConsume stops consumer goroutine
func (q *DelayQueue) StopConsume() {
	close(q.close)
	if q.ticker != nil {
		q.ticker.Stop()
	}
}

// GetPendingCount returns the number of pending messages
func (q *DelayQueue) GetPendingCount() (int64, error) {
	return redisCli.ZCard(q.pendingKey)
}

// GetReadyCount returns the number of messages which have arrived delivery time but but have not been delivered
func (q *DelayQueue) GetReadyCount() (int64, error) {
	return redisCli.LLen(q.readyKey)
}

// GetProcessingCount returns the number of messages which are being processed
func (q *DelayQueue) GetProcessingCount() (int64, error) {
	return redisCli.ZCard(q.unAckKey)
}

// EventListener which will be called when events occur
// This Listener can be used to monitor running status
type EventListener interface {
	// OnEvent will be called when events occur
	OnEvent(*Event)
}

// ListenEvent register a listener which will be called when events occur,
// so it can be used to monitor running status
//
// But It can ONLY receive events from the CURRENT INSTANCE,
// if you want to listen to all events in queue, just use DelayQueueMonitor.ListenEvent
//
// There can be AT MOST ONE EventListener in an DelayQueue instance.
// If you are using customized listener, DelayQueueMonitor will stop working
func (q *DelayQueue) ListenEvent(listener EventListener) {
	q.eventListener = listener
}

// RemoveListener stops reporting events to EventListener
func (q *DelayQueue) DisableListener() {
	q.eventListener = nil
}

func (q *DelayQueue) reportEvent(code int, count int) {
	listener := q.eventListener // eventListener may be changed during running
	if listener != nil && count > 0 {
		event := &Event{
			Code:      code,
			Timestamp: time.Now().Unix(),
			MsgCount:  count,
		}
		listener.OnEvent(event)
	}
}

// pubsubListener receives events and reports them through redis pubsub for monitoring
type pubsubListener struct {
	redisCli   RedisCli
	reportChan string
}

func genReportChannel(name string) string {
	return "dq:" + name + ":reportEvents"
}

// EnableReport enables reporting to monitor
func (q *DelayQueue) EnableReport() {
	reportChan := genReportChannel(q.name)
	q.ListenEvent(&pubsubListener{
		redisCli:   redisCli,
		reportChan: reportChan,
	})
}

// DisableReport stops reporting to monitor
func (q *DelayQueue) DisableReport() {
	q.DisableListener()
}

func (l *pubsubListener) OnEvent(event *Event) {
	payload := encodeEvent(event)
	_ = l.redisCli.Publish(l.reportChan, payload)
}

type hashTagKeyOpt int

// CallbackFunc receives and consumes messages
// returns true to confirm successfully consumed, false to re-deliver this message
type CallbackFunc = func(string) bool

// UseHashTagKey add hashtags to redis keys to ensure all keys of this queue are allocated in the same hash slot.
// If you are using Codis/AliyunRedisCluster/TencentCloudRedisCluster, add this option to NewQueue
// WARNING! Changing (add or remove) this option will cause DelayQueue failing to read existed data in redis
// see more:  https://redis.io/docs/reference/cluster-spec/#hash-tags
func UseHashTagKey() any {
	return hashTagKeyOpt(1)
}
