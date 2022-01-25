package nsqd

import (
	"container/heap"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-diskqueue"

	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/pqueue"
	"github.com/nsqio/nsq/internal/quantile"
)

type Consumer interface {
	UnPause()
	Pause()
	Close() error
	TimedOutMessage()
	Stats(string) ClientStats
	Empty()
}

// Channel represents the concrete type for a NSQ channel (and also
// implements the Queue interface)
//
// There can be multiple channels per topic, each with there own unique set
// of subscribers (clients).
//
// Channels maintain all client and message metadata, orchestrating in-flight
// messages, timeouts, requeuing, etc.

// Channel 表示 NSQ 通道的具体类型（也实现了 Queue 接口）
// 每个主题可以有多个频道，每个频道都有自己独特的订阅者（客户端）集。
// 通道维护所有客户端和消息元数据，编排正在运行的消息、超时、重新排队等。
type Channel struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	requeueCount uint64
	messageCount uint64
	timeoutCount uint64

	sync.RWMutex

	topicName string // Topic的名称
	name      string // Channel的名称
	nsqd      *NSQD

	backend BackendQueue // 消息优先存入这个内存chan

	memoryMsgChan chan *Message // 磁盘队列，当内存memoryMsgChan满时，写入硬盘队列
	exitFlag      int32         // 指示此通道是否关闭/退出
	exitMutex     sync.RWMutex  // 退出锁

	// state tracking
	clients        map[int64]Consumer // 存储消费者clients切片
	paused         int32              // 当前 channel 是否处于暂停状态
	ephemeral      bool               // channel 是否为临时的
	deleteCallback func(*Channel)     // 删除回调
	deleter        sync.Once          // 用于 clients map 为空时候，临时 channel 的删除

	// Stats tracking
	e2eProcessingLatencyStream *quantile.Quantile

	// TODO: these can be DRYd up
	deferredMessages map[MessageID]*pqueue.Item // 保存尚未到时间的延迟消费消息
	deferredPQ       pqueue.PriorityQueue       // 保存尚未到时间的延迟消费消息，最小堆。
	deferredMutex    sync.Mutex
	inFlightMessages map[MessageID]*Message // 保存已推送尚未收到FIN的消息
	inFlightPQ       inFlightPqueue         // 保存已推送尚未收到FIN的消息，最小堆(时间越小的排在越前面，用于处理超时的消息)
	inFlightMutex    sync.Mutex
}

// NewChannel creates a new instance of the Channel type and returns a pointer
func NewChannel(topicName string, channelName string, nsqd *NSQD,
	deleteCallback func(*Channel)) *Channel {

	c := &Channel{
		topicName:      topicName,
		name:           channelName,
		memoryMsgChan:  nil,
		clients:        make(map[int64]Consumer),
		deleteCallback: deleteCallback,
		nsqd:           nsqd,
	}
	// create mem-queue only if size > 0 (do not use unbuffered chan)
	if nsqd.getOpts().MemQueueSize > 0 {
		c.memoryMsgChan = make(chan *Message, nsqd.getOpts().MemQueueSize)
	}
	if len(nsqd.getOpts().E2EProcessingLatencyPercentiles) > 0 { // TODO
		c.e2eProcessingLatencyStream = quantile.New(
			nsqd.getOpts().E2EProcessingLatencyWindowTime,
			nsqd.getOpts().E2EProcessingLatencyPercentiles,
		)
	}

	c.initPQ() // 初始化队列

	if strings.HasSuffix(channelName, "#ephemeral") { // 临时
		c.ephemeral = true
		c.backend = newDummyBackendQueue()
	} else {
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := nsqd.getOpts()
			lg.Logf(opts.Logger, opts.LogLevel, lg.LogLevel(level), f, args...)
		}

		// Channel持久化队列
		// backend names, for uniqueness, automatically include the topic...
		backendName := getBackendName(topicName, channelName)
		c.backend = diskqueue.New( // 持久化队列
			backendName,
			nsqd.getOpts().DataPath,
			nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			nsqd.getOpts().SyncEvery,
			nsqd.getOpts().SyncTimeout,
			dqLogf,
		)
	}

	c.nsqd.Notify(c, !c.ephemeral) // 发送Channel通知

	return c
}

// 初始化in-flight和deferred的Map和PQ
func (c *Channel) initPQ() {
	pqSize := int(math.Max(1, float64(c.nsqd.getOpts().MemQueueSize)/10))

	c.inFlightMutex.Lock() // 初始化 已推送尚未收到FIN的消息 队列
	c.inFlightMessages = make(map[MessageID]*Message)
	c.inFlightPQ = newInFlightPqueue(pqSize)
	c.inFlightMutex.Unlock()

	c.deferredMutex.Lock() // 初始化 延迟消费 队列
	c.deferredMessages = make(map[MessageID]*pqueue.Item)
	c.deferredPQ = pqueue.New(pqSize)
	c.deferredMutex.Unlock()
}

// Exiting returns a boolean indicating if this channel is closed/exiting
func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitFlag) == 1
}

// Delete empties the channel and closes
// 删除清空通道并关闭(会将所有消息清空不保存)
func (c *Channel) Delete() error {
	return c.exit(true)
}

// Close cleanly closes the Channel
func (c *Channel) Close() error {
	return c.exit(false)
}

// deleted 为true时会删除数据
func (c *Channel) exit(deleted bool) error {
	c.exitMutex.Lock()
	defer c.exitMutex.Unlock()

	if !atomic.CompareAndSwapInt32(&c.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		c.nsqd.logf(LOG_INFO, "CHANNEL(%s): deleting", c.name)

		// since we are explicitly deleting a channel (not just at system exit time)
		// de-register this from the lookupd
		// 持久化元数据
		c.nsqd.Notify(c, !c.ephemeral)
	} else {
		c.nsqd.logf(LOG_INFO, "CHANNEL(%s): closing", c.name)
	}

	// this forceably closes client connections
	c.RLock()
	for _, client := range c.clients {
		client.Close()
	}
	c.RUnlock()

	if deleted {
		// empty the queue (deletes the backend files, too)
		c.Empty() // 清空队列（也删除后端文件）
		return c.backend.Delete()
	}

	// write anything leftover to disk
	c.flush() // 将剩余的任何内容写入磁盘
	return c.backend.Close()
}

// Empty 丢弃memoryMsgChan和backend,覆盖Map和PQ
func (c *Channel) Empty() error {
	c.Lock()
	defer c.Unlock()

	c.initPQ()
	for _, client := range c.clients {
		client.Empty()
	}

	for {
		select {
		case <-c.memoryMsgChan: // 清空内存msg
		default:
			goto finish
		}
	}

finish:
	return c.backend.Empty() // 清空持久化msg
}

// flush persists all the messages in internal memory buffers to the backend
// it does not drain inflight/deferred because it is only called in Close()
func (c *Channel) flush() error {
	if len(c.memoryMsgChan) > 0 || len(c.inFlightMessages) > 0 || len(c.deferredMessages) > 0 {
		c.nsqd.logf(LOG_INFO, "CHANNEL(%s): flushing %d memory %d in-flight %d deferred messages to backend",
			c.name, len(c.memoryMsgChan), len(c.inFlightMessages), len(c.deferredMessages))
	}

	for {
		select { // 将内存中的消息持久化
		case msg := <-c.memoryMsgChan:
			err := writeMessageToBackend(msg, c.backend)
			if err != nil {
				c.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}

finish:
	c.inFlightMutex.Lock() // 将发生还未确认的消息持久化
	for _, msg := range c.inFlightMessages {
		err := writeMessageToBackend(msg, c.backend)
		if err != nil {
			c.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		}
	}
	c.inFlightMutex.Unlock()

	c.deferredMutex.Lock() // 将延迟消息持久化
	for _, item := range c.deferredMessages {
		msg := item.Value.(*Message)
		err := writeMessageToBackend(msg, c.backend)
		if err != nil {
			c.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		}
	}
	c.deferredMutex.Unlock()

	return nil
}

func (c *Channel) Depth() int64 {
	return int64(len(c.memoryMsgChan)) + c.backend.Depth()
}

func (c *Channel) Pause() error {
	return c.doPause(true)
}

func (c *Channel) UnPause() error {
	return c.doPause(false)
}

func (c *Channel) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&c.paused, 1)
	} else {
		atomic.StoreInt32(&c.paused, 0)
	}

	c.RLock()
	for _, client := range c.clients {
		if pause {
			client.Pause()
		} else {
			client.UnPause()
		}
	}
	c.RUnlock()
	return nil
}

func (c *Channel) IsPaused() bool {
	return atomic.LoadInt32(&c.paused) == 1
}

// PutMessage writes a Message to the queue
func (c *Channel) PutMessage(m *Message) error {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()
	if c.Exiting() {
		return errors.New("exiting")
	}
	err := c.put(m)
	if err != nil {
		return err
	}
	atomic.AddUint64(&c.messageCount, 1)
	return nil
}

// 将消息发送到Channel.memoryMsgChan或持久化队列Channel.backend
func (c *Channel) put(m *Message) error {
	select {
	case c.memoryMsgChan <- m: // 内存Channel
	default:
		err := writeMessageToBackend(m, c.backend) // 持久化队列
		c.nsqd.SetHealth(err)
		if err != nil {
			c.nsqd.logf(LOG_ERROR, "CHANNEL(%s): failed to write message to backend - %s",
				c.name, err)
			return err
		}
	}
	return nil
}

func (c *Channel) PutMessageDeferred(msg *Message, timeout time.Duration) {
	atomic.AddUint64(&c.messageCount, 1) // 消息数+1
	c.StartDeferredTimeout(msg, timeout)
}

// TouchMessage resets the timeout for an in-flight message
// TouchMessage 重置飞行中消息的超时
func (c *Channel) TouchMessage(clientID int64, id MessageID, clientMsgTimeout time.Duration) error {
	msg, err := c.popInFlightMessage(clientID, id) // 先从in-flightMap移除
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg) // 先从in-flightPQ移除

	newTimeout := time.Now().Add(clientMsgTimeout)
	if newTimeout.Sub(msg.deliveryTS) >=
		c.nsqd.getOpts().MaxMsgTimeout { // 调整过期时间
		// we would have gone over, set to the max
		newTimeout = msg.deliveryTS.Add(c.nsqd.getOpts().MaxMsgTimeout)
	}

	msg.pri = newTimeout.UnixNano()
	err = c.pushInFlightMessage(msg) // 从新添加到in-flightMap
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg) // 从新添加到in-flightPQ
	return nil
}

// FinishMessage successfully discards an in-flight message
func (c *Channel) FinishMessage(clientID int64, id MessageID) error {
	msg, err := c.popInFlightMessage(clientID, id) // 以原子方式从in -flight 字典中删除一条消息
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)              // 从优先队列移除该消息
	if c.e2eProcessingLatencyStream != nil { // TODO 统计跟踪
		c.e2eProcessingLatencyStream.Insert(msg.Timestamp)
	}
	return nil
}

// RequeueMessage requeues a message based on `time.Duration`, ie:
//
// `timeoutMs` == 0 - requeue a message immediately
// `timeoutMs`  > 0 - asynchronously wait for the specified timeout
//     and requeue a message (aka "deferred requeue")
// 先将Msg从in-flightMap和in-flightPQ移除，再添加到重排deferredMessagesMap和延时重排队列DeferredPQ
func (c *Channel) RequeueMessage(clientID int64, id MessageID, timeout time.Duration) error {
	// remove from inflight first
	msg, err := c.popInFlightMessage(clientID, id) // 先从in-flightMap移除
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)          // 先从in-flightPQ移除
	atomic.AddUint64(&c.requeueCount, 1) // 重新排队+1

	if timeout == 0 { // 立刻重排
		c.exitMutex.RLock()
		if c.Exiting() {
			c.exitMutex.RUnlock()
			return errors.New("exiting")
		}
		err := c.put(msg)
		c.exitMutex.RUnlock()
		return err
	}

	// deferred requeue 延迟重新排队
	return c.StartDeferredTimeout(msg, timeout)
}

// AddClient adds a client to the Channel's client list
// AddClient 将客户端添加到Channel的客户端列表
func (c *Channel) AddClient(clientID int64, client Consumer) error {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return errors.New("exiting")
	}

	c.RLock()
	_, ok := c.clients[clientID]
	numClients := len(c.clients)
	c.RUnlock()
	if ok {
		return nil
	}

	maxChannelConsumers := c.nsqd.getOpts().MaxChannelConsumers
	if maxChannelConsumers != 0 && numClients >= maxChannelConsumers {
		return fmt.Errorf("consumers for %s:%s exceeds limit of %d",
			c.topicName, c.name, maxChannelConsumers)
	}

	c.Lock()
	c.clients[clientID] = client
	c.Unlock()
	return nil
}

// RemoveClient removes a client from the Channel's client list
// RemoveClient 从Channel的客户端列表中删除一个客户端
func (c *Channel) RemoveClient(clientID int64) {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return
	}

	c.RLock()
	_, ok := c.clients[clientID]
	c.RUnlock()
	if !ok {
		return
	}

	c.Lock()
	delete(c.clients, clientID)
	c.Unlock()

	if len(c.clients) == 0 && c.ephemeral == true {
		go c.deleter.Do(func() { c.deleteCallback(c) })
	}
}

// StartInFlightTimeout 将一条消息写入 inFlight 队列
func (c *Channel) StartInFlightTimeout(msg *Message, clientID int64, timeout time.Duration) error {
	now := time.Now()
	msg.clientID = clientID
	msg.deliveryTS = now
	msg.pri = now.Add(timeout).UnixNano()
	err := c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg) // 添加到In-Flight的优先队列
	return nil
}

// StartDeferredTimeout 加入延时deferredMessages Map和延时队列
func (c *Channel) StartDeferredTimeout(msg *Message, timeout time.Duration) error {
	absTs := time.Now().Add(timeout).UnixNano()
	item := &pqueue.Item{Value: msg, Priority: absTs}
	err := c.pushDeferredMessage(item) // 加入延时deferredMessages Map
	if err != nil {
		return err
	}
	c.addToDeferredPQ(item) //  加入延时队列
	return nil
}

// pushInFlightMessage atomically adds a message to the in-flight dictionary
func (c *Channel) pushInFlightMessage(msg *Message) error {
	c.inFlightMutex.Lock()
	_, ok := c.inFlightMessages[msg.ID]
	if ok {
		c.inFlightMutex.Unlock()
		return errors.New("ID already in flight")
	}
	c.inFlightMessages[msg.ID] = msg
	c.inFlightMutex.Unlock()
	return nil
}

// popInFlightMessage atomically removes a message from the in-flight dictionary
func (c *Channel) popInFlightMessage(clientID int64, id MessageID) (*Message, error) {
	c.inFlightMutex.Lock()
	msg, ok := c.inFlightMessages[id]
	if !ok {
		c.inFlightMutex.Unlock()
		return nil, errors.New("ID not in flight")
	}
	if msg.clientID != clientID {
		c.inFlightMutex.Unlock()
		return nil, errors.New("client does not own message")
	}
	delete(c.inFlightMessages, id)
	c.inFlightMutex.Unlock()
	return msg, nil
}

func (c *Channel) addToInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	c.inFlightPQ.Push(msg)
	c.inFlightMutex.Unlock()
}

func (c *Channel) removeFromInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	if msg.index == -1 {
		// this item has already been popped off the pqueue
		c.inFlightMutex.Unlock()
		return
	}
	c.inFlightPQ.Remove(msg.index)
	c.inFlightMutex.Unlock()
}

func (c *Channel) pushDeferredMessage(item *pqueue.Item) error {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	id := item.Value.(*Message).ID
	_, ok := c.deferredMessages[id]
	if ok {
		c.deferredMutex.Unlock()
		return errors.New("ID already deferred")
	}
	c.deferredMessages[id] = item
	c.deferredMutex.Unlock()
	return nil
}

func (c *Channel) popDeferredMessage(id MessageID) (*pqueue.Item, error) {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	item, ok := c.deferredMessages[id]
	if !ok {
		c.deferredMutex.Unlock()
		return nil, errors.New("ID not deferred")
	}
	delete(c.deferredMessages, id)
	c.deferredMutex.Unlock()
	return item, nil
}

func (c *Channel) addToDeferredPQ(item *pqueue.Item) {
	c.deferredMutex.Lock()
	heap.Push(&c.deferredPQ, item)
	c.deferredMutex.Unlock()
}

// 用于 nsqd.go 中的 queueScanLoop，用于 put 延迟发送的消息
func (c *Channel) processDeferredQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.deferredMutex.Lock() // TODO
		item, _ := c.deferredPQ.PeekAndShift(t)
		c.deferredMutex.Unlock()

		if item == nil {
			goto exit
		}
		dirty = true

		msg := item.Value.(*Message)
		_, err := c.popDeferredMessage(msg.ID) // 从deferredMessagesMap 删除
		if err != nil {
			goto exit
		}
		c.put(msg) // 发送
	}

exit:
	return dirty
}

// 用于 nsqd.go 中的 queueScanLoop，用于重发超时的消息
func (c *Channel) processInFlightQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.inFlightMutex.Lock()
		// 没有超时，则返回nil, 然后goto exit->return dirty
		// 超时了，inFlightPQ弹出并返回msg
		msg, _ := c.inFlightPQ.PeekAndShift(t)
		c.inFlightMutex.Unlock()

		if msg == nil {
			goto exit
		}
		dirty = true

		_, err := c.popInFlightMessage(msg.clientID, msg.ID) // 从inFlightMessageMap 删除
		if err != nil {
			goto exit
		}
		atomic.AddUint64(&c.timeoutCount, 1)
		c.RLock()
		client, ok := c.clients[msg.clientID]
		c.RUnlock()
		if ok {
			client.TimedOutMessage()
		}
		// 将消息重新写入channel
		c.put(msg)
	}

exit:
	return dirty
}
