package nsqd

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-diskqueue"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/quantile"
	"github.com/nsqio/nsq/internal/util"
)

type Topic struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messageCount uint64 // 累计消息数
	messageBytes uint64 // 累计消息体的字节数

	sync.RWMutex

	name              string                // topic名，生产和消费时需要指定此名称
	channelMap        map[string]*Channel   // 保存每个channel name和channel指针的映射
	backend           BackendQueue          // 磁盘队列，当内存memoryMsgChan满时，写入硬盘队列
	memoryMsgChan     chan *Message         // 消息优先存入这个内存chan
	startChan         chan int              // 启动 chan
	exitChan          chan int              // 终止 chan
	channelUpdateChan chan int              // topic 内的 channel map 增加/删除的更新通知
	waitGroup         util.WaitGroupWrapper // 包裹一层 waitGroup，当执行 Exit 的时候，需要等待部分函数执行完毕
	exitFlag          int32                 // topic 是否处于退出状态
	idFactory         *guidFactory          // id生成器工厂

	ephemeral      bool         // topic 是否为临时的，临时的 topic 不会存入磁盘
	deleteCallback func(*Topic) // 删除的回调函数
	deleter        sync.Once

	paused    int32    // topic 是否处于暂停状态
	pauseChan chan int // pause 事件的回调 chan

	nsqd *NSQD // nsqd 上下文
}

// NewTopic Topic constructor
func NewTopic(topicName string, nsqd *NSQD, deleteCallback func(*Topic)) *Topic {
	t := &Topic{
		name:              topicName,
		channelMap:        make(map[string]*Channel),
		memoryMsgChan:     nil,
		startChan:         make(chan int, 1),
		exitChan:          make(chan int),
		channelUpdateChan: make(chan int),
		nsqd:              nsqd,
		paused:            0,
		pauseChan:         make(chan int),
		deleteCallback:    deleteCallback,
		idFactory:         NewGUIDFactory(nsqd.getOpts().ID), // 全局id生成工厂
	}
	// create mem-queue only if size > 0 (do not use unbuffered chan)
	if nsqd.getOpts().MemQueueSize > 0 { // 创建 mem-queue
		t.memoryMsgChan = make(chan *Message, nsqd.getOpts().MemQueueSize)
	}
	if strings.HasSuffix(topicName, "#ephemeral") { // 临时 topic，消息超出内存限制不落磁盘
		t.ephemeral = true
		t.backend = newDummyBackendQueue()
	} else {
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := nsqd.getOpts()
			lg.Logf(opts.Logger, opts.LogLevel, lg.LogLevel(level), f, args...)
		}
		// 持久化队列
		t.backend = diskqueue.New(
			topicName,
			nsqd.getOpts().DataPath,
			nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			nsqd.getOpts().SyncEvery,
			nsqd.getOpts().SyncTimeout,
			dqLogf,
		)
	}

	t.waitGroup.Wrap(t.messagePump) // 通过go协程启动消息泵

	t.nsqd.Notify(t, !t.ephemeral) // 通知 lookupLoop，新增了一个 Topic，进行 Register 操作

	return t
}

// Start 发送Topic开始通知，开始Topic.messagePump()
func (t *Topic) Start() {
	select {
	case t.startChan <- 1:
	default:
	}
}

// Exiting returns a boolean indicating if this topic is closed/exiting
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
// 获取 topic 中 name 为 channelName 的 GetChannel
// 如果不存在，新建一个 channel
func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	channel, isNew := t.getOrCreateChannel(channelName)
	t.Unlock()

	if isNew {
		// update messagePump state
		select {
		case t.channelUpdateChan <- 1: // Channel 更新通知
		case <-t.exitChan:
		}
	}

	return channel
}

// this expects the caller to handle locking
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	channel, ok := t.channelMap[channelName]
	if !ok {
		deleteCallback := func(c *Channel) { // 删除Channel回调
			t.DeleteExistingChannel(c.name)
		}
		channel = NewChannel(t.name, channelName, t.nsqd, deleteCallback) // 创建一个Channel
		t.channelMap[channelName] = channel                               // 保存到Topic.channelMap
		t.nsqd.logf(LOG_INFO, "TOPIC(%s): new channel(%s)", t.name, channel.name)
		return channel, true
	}
	return channel, false
}

// GetExistingChannel 获取 topic 中 name 为 channelName 的 channel，如果不存在，返回 nil
func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.RLock()
	defer t.RUnlock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel, nil
}

// DeleteExistingChannel removes a channel from the topic only if it exists
// 将 client 从 nsqd 的 clientMap 中删除
func (t *Topic) DeleteExistingChannel(channelName string) error {
	t.RLock()
	channel, ok := t.channelMap[channelName]
	t.RUnlock()
	if !ok {
		return errors.New("channel does not exist")
	}

	t.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting channel %s", t.name, channel.name)

	// delete empties the channel before closing
	// (so that we dont leave any messages around)
	//
	// we do this before removing the channel from map below (with no lock)
	// so that any incoming subs will error and not create a new channel
	// to enforce ordering
	channel.Delete()

	t.Lock()
	delete(t.channelMap, channelName)
	numChannels := len(t.channelMap)
	t.Unlock()

	// update messagePump state
	select {
	case t.channelUpdateChan <- 1:
	case <-t.exitChan:
	}

	if numChannels == 0 && t.ephemeral == true {
		go t.deleter.Do(func() { t.deleteCallback(t) })
	}

	return nil
}

// PutMessage writes a Message to the queue
// PutMessage 向 topic 写一条消息，如果 memoryMsgQueue 满了，则写入 backendQueue
func (t *Topic) PutMessage(m *Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	err := t.put(m) // 添加消息到内存队列或持久化队列
	if err != nil {
		return err
	}
	atomic.AddUint64(&t.messageCount, 1)
	atomic.AddUint64(&t.messageBytes, uint64(len(m.Body)))
	return nil
}

// PutMessages writes multiple Messages to the queue
// PutMessages 一次性写入多条 message
func (t *Topic) PutMessages(msgs []*Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}

	messageTotalBytes := 0

	for i, m := range msgs {
		err := t.put(m)
		if err != nil {
			atomic.AddUint64(&t.messageCount, uint64(i))
			atomic.AddUint64(&t.messageBytes, uint64(messageTotalBytes))
			return err
		}
		messageTotalBytes += len(m.Body)
	}

	atomic.AddUint64(&t.messageBytes, uint64(messageTotalBytes))
	atomic.AddUint64(&t.messageCount, uint64(len(msgs)))
	return nil
}

// 添加消息到内存队列或持久化队列（写入 memoryMsgChan 或者 BackendQueue）
func (t *Topic) put(m *Message) error {
	select {
	case t.memoryMsgChan <- m: // 当 chan 中还能写入时，写入 memoryMsgChan
	default: // 否则，写入 BackendQueue，落入磁盘
		err := writeMessageToBackend(m, t.backend) // 添加消息到持久化队列
		t.nsqd.SetHealth(err)
		if err != nil {
			t.nsqd.logf(LOG_ERROR,
				"TOPIC(%s) ERROR: failed to write message to backend - %s",
				t.name, err)
			return err
		}
	}
	return nil
}

func (t *Topic) Depth() int64 {
	return int64(len(t.memoryMsgChan)) + t.backend.Depth()
}

// messagePump selects over the in-memory and backend queue and
// writes messages to every channel for this topic
// topic 启动后，执行 messagePump
// messagePump 从内存中的 memoryMsgChan 以及 BackendQueue 中获取message
// 并将 message 放入全部的 channel
func (t *Topic) messagePump() {
	var msg *Message
	var buf []byte
	var err error
	var chans []*Channel
	var memoryMsgChan chan *Message
	var backendChan <-chan []byte

	// do not pass messages before Start(), but avoid blocking Pause() or GetChannel()
	for { // TODO
		select {
		case <-t.channelUpdateChan: // 接收 Channel 更新通知 （创建或删除）
			continue
		case <-t.pauseChan: // 接收暂停通知
			continue
		case <-t.exitChan: // 接收退出通知
			goto exit
		case <-t.startChan: // 接收开始通知，开始接收msg
		}
		break
	}
	t.RLock()
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.RUnlock()
	if len(chans) > 0 && !t.IsPaused() { // TODO Topic是没有暂停
		memoryMsgChan = t.memoryMsgChan
		backendChan = t.backend.ReadChan()
	}

	// main message loop
	for {
		select { // 从这里可以看到，如果消息已经被写入磁盘的话，nsq 消费消息就是无序的
		case msg = <-memoryMsgChan: // 从内存channel获取msg
		case buf = <-backendChan: // 从持久化队列获取msg
			msg, err = decodeMessage(buf) // 解码[]byte
			if err != nil {
				t.nsqd.logf(LOG_ERROR, "failed to decode message - %s", err)
				continue
			}
		case <-t.channelUpdateChan: // 接收 Channel 更新通知 （创建或删除）
			chans = chans[:0]
			t.RLock()
			for _, c := range t.channelMap {
				chans = append(chans, c)
			}
			t.RUnlock()
			if len(chans) == 0 || t.IsPaused() {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case <-t.pauseChan: // 接收暂停通知
			if len(chans) == 0 || t.IsPaused() {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case <-t.exitChan: // 接收退出通知
			goto exit
		}

		for i, channel := range chans { // 将msg发送到当前Topic下的每一个Channel
			chanMsg := msg
			// copy the message because each channel
			// needs a unique instance but...
			// fastpath to avoid copy if its the first channel
			// (the topic already created the first copy)
			// 这里 msg 实例要进行深拷贝，因为每个 channel 需要自己的实例
			// 为了重发/延迟发送等
			if i > 0 {
				chanMsg = NewMessage(msg.ID, msg.Body) // 封装msg
				chanMsg.Timestamp = msg.Timestamp
				chanMsg.deferred = msg.deferred
			}
			if chanMsg.deferred != 0 { // 延时消息
				channel.PutMessageDeferred(chanMsg, chanMsg.deferred)
				continue
			}
			err := channel.PutMessage(chanMsg) // 发送msg到内存Channel或持久化队列
			if err != nil {
				t.nsqd.logf(LOG_ERROR,
					"TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s",
					t.name, msg.ID, channel.name, err)
			}
		}
	}

exit:
	t.nsqd.logf(LOG_INFO, "TOPIC(%s): closing ... messagePump", t.name)
}

// Delete empties the topic and all its channels and closes
func (t *Topic) Delete() error {
	return t.exit(true)
}

// Close persists all outstanding topic data and closes all its channels
func (t *Topic) Close() error {
	return t.exit(false)
}

func (t *Topic) exit(deleted bool) error {
	if !atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		t.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting", t.name)

		// since we are explicitly deleting a topic (not just at system exit time)
		// de-register this from the lookupd
		t.nsqd.Notify(t, !t.ephemeral)
	} else {
		t.nsqd.logf(LOG_INFO, "TOPIC(%s): closing", t.name)
	}

	close(t.exitChan)

	// synchronize the close of messagePump()
	t.waitGroup.Wait()

	if deleted {
		// topic 删除，topic 下的 channel 同样执行 Delete 操作
		t.Lock()
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		t.Unlock()

		// empty the queue (deletes the backend files, too)
		// 清空 topic 的消息 chan，删除落在磁盘上的消息
		t.Empty()
		return t.backend.Delete()
	}

	// close all the channels
	t.RLock()
	for _, channel := range t.channelMap {
		err := channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			t.nsqd.logf(LOG_ERROR, "channel(%s) close - %s", channel.name, err)
		}
	}
	t.RUnlock()

	// 将当前内存消息 chan 中的 msg 写入磁盘
	// write anything leftover to disk
	t.flush()
	return t.backend.Close()
}

func (t *Topic) Empty() error {
	for {
		select {
		case <-t.memoryMsgChan:
		default:
			goto finish
		}
	}

finish:
	return t.backend.Empty()
}

func (t *Topic) flush() error {
	if len(t.memoryMsgChan) > 0 {
		t.nsqd.logf(LOG_INFO,
			"TOPIC(%s): flushing %d memory messages to backend",
			t.name, len(t.memoryMsgChan))
	}

	for {
		select {
		case msg := <-t.memoryMsgChan:
			err := writeMessageToBackend(msg, t.backend)
			if err != nil {
				t.nsqd.logf(LOG_ERROR,
					"ERROR: failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}

finish:
	return nil
}

func (t *Topic) AggregateChannelE2eProcessingLatency() *quantile.Quantile {
	var latencyStream *quantile.Quantile
	t.RLock()
	realChannels := make([]*Channel, 0, len(t.channelMap))
	for _, c := range t.channelMap {
		realChannels = append(realChannels, c)
	}
	t.RUnlock()
	for _, c := range realChannels {
		if c.e2eProcessingLatencyStream == nil {
			continue
		}
		if latencyStream == nil {
			latencyStream = quantile.New(
				t.nsqd.getOpts().E2EProcessingLatencyWindowTime,
				t.nsqd.getOpts().E2EProcessingLatencyPercentiles)
		}
		latencyStream.Merge(c.e2eProcessingLatencyStream)
	}
	return latencyStream
}

// Pause 暂停 topic
func (t *Topic) Pause() error {
	return t.doPause(true)
}

// UnPause 取消暂停
func (t *Topic) UnPause() error {
	return t.doPause(false)
}

func (t *Topic) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&t.paused, 1)
	} else {
		atomic.StoreInt32(&t.paused, 0)
	}

	select {
	case t.pauseChan <- 1:
	case <-t.exitChan:
	}

	return nil
}

func (t *Topic) IsPaused() bool {
	return atomic.LoadInt32(&t.paused) == 1
}

func (t *Topic) GenerateID() MessageID {
	var i int64 = 0
	for {
		id, err := t.idFactory.NewGUID()
		if err == nil {
			return id.Hex()
		}
		if i%10000 == 0 {
			t.nsqd.logf(LOG_ERROR, "TOPIC(%s): failed to create guid - %s", t.name, err)
		}
		time.Sleep(time.Millisecond)
		i++
	}
}
