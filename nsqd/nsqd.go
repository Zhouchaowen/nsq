package nsqd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/nsq/internal/clusterinfo"
	"github.com/nsqio/nsq/internal/dirlock"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/statsd"
	"github.com/nsqio/nsq/internal/util"
	"github.com/nsqio/nsq/internal/version"
)

const (
	TLSNotRequired = iota
	TLSRequiredExceptHTTP
	TLSRequired
)

type errStore struct {
	err error
}

type NSQD struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	clientIDSequence int64 // 连接 nsqd tcp server 的 client 数目

	sync.RWMutex
	ctx context.Context
	// ctxCancel cancels a context that main() is waiting on
	ctxCancel context.CancelFunc

	opts atomic.Value // nsqd 的配置文件

	dl        *dirlock.DirLock // 文件锁
	isLoading int32            // nsqd 是否在读取存储在 datapath 中的 metadata 文件
	isExiting int32
	errValue  atomic.Value // 错误信息
	startTime time.Time    // nsqd 开始运行的时间

	topicMap map[string]*Topic // 保存当前所有的topic

	lookupPeers atomic.Value // 保存lookupd

	tcpServer     *tcpServer
	tcpListener   net.Listener
	httpListener  net.Listener
	httpsListener net.Listener
	tlsConfig     *tls.Config

	poolSize int // nsqd 执行 queueScan 的 worker 数目

	// 通知 chan，当 增加/删除 topic/channel 的时候，通知 lookupLoop 进行 Register/Unregister
	notifyChan           chan interface{}
	optsNotificationChan chan struct{}         // 配置文件 chan
	exitChan             chan int              // 退出 chan
	waitGroup            util.WaitGroupWrapper // 包裹一层 waitGroup，当执行 Exit 的时候，需要等待部分函数执行完毕

	ci *clusterinfo.ClusterInfo
}

func New(opts *Options) (*NSQD, error) {
	var err error

	dataPath := opts.DataPath
	if opts.DataPath == "" {
		cwd, _ := os.Getwd()
		dataPath = cwd
	}
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}

	n := &NSQD{
		startTime:            time.Now(),
		topicMap:             make(map[string]*Topic),
		exitChan:             make(chan int),
		notifyChan:           make(chan interface{}),
		optsNotificationChan: make(chan struct{}, 1),
		dl:                   dirlock.New(dataPath),
	}
	n.ctx, n.ctxCancel = context.WithCancel(context.Background())
	// 封装一个带超时的client
	httpcli := http_api.NewClient(nil, opts.HTTPClientConnectTimeout, opts.HTTPClientRequestTimeout)
	n.ci = clusterinfo.New(n.logf, httpcli)

	n.lookupPeers.Store([]*lookupPeer{})

	n.swapOpts(opts)
	n.errValue.Store(errStore{})

	err = n.dl.Lock() // 目录锁
	if err != nil {
		return nil, fmt.Errorf("failed to lock data-path: %v", err)
	}

	if opts.MaxDeflateLevel < 1 || opts.MaxDeflateLevel > 9 {
		return nil, errors.New("--max-deflate-level must be [1,9]")
	}

	if opts.ID < 0 || opts.ID >= 1024 {
		return nil, errors.New("--node-id must be [0,1024)")
	}

	if opts.TLSClientAuthPolicy != "" && opts.TLSRequired == TLSNotRequired {
		opts.TLSRequired = TLSRequired
	}

	// 通过opts构建TLS
	tlsConfig, err := buildTLSConfig(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config - %s", err)
	}
	if tlsConfig == nil && opts.TLSRequired != TLSNotRequired {
		return nil, errors.New("cannot require TLS client connections without TLS key and cert")
	}
	n.tlsConfig = tlsConfig

	for _, v := range opts.E2EProcessingLatencyPercentiles {
		if v <= 0 || v > 1 {
			return nil, fmt.Errorf("invalid E2E processing latency percentile: %v", v)
		}
	}

	n.logf(LOG_INFO, version.String("nsqd"))
	n.logf(LOG_INFO, "ID: %d", opts.ID)

	// 构建 TCP HTTP HTTPS Listen
	n.tcpServer = &tcpServer{nsqd: n}
	n.tcpListener, err = net.Listen("tcp", opts.TCPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.TCPAddress, err)
	}
	if opts.HTTPAddress != "" {
		n.httpListener, err = net.Listen("tcp", opts.HTTPAddress)
		if err != nil {
			return nil, fmt.Errorf("listen (%s) failed - %s", opts.HTTPAddress, err)
		}
	}
	if n.tlsConfig != nil && opts.HTTPSAddress != "" {
		n.httpsListener, err = tls.Listen("tcp", opts.HTTPSAddress, n.tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("listen (%s) failed - %s", opts.HTTPSAddress, err)
		}
	}
	if opts.BroadcastHTTPPort == 0 {
		opts.BroadcastHTTPPort = n.RealHTTPAddr().Port
	}

	if opts.BroadcastTCPPort == 0 {
		opts.BroadcastTCPPort = n.RealTCPAddr().Port
	}

	if opts.StatsdPrefix != "" {
		var port string = fmt.Sprint(opts.BroadcastHTTPPort)
		statsdHostKey := statsd.HostKey(net.JoinHostPort(opts.BroadcastAddress, port))
		prefixWithHost := strings.Replace(opts.StatsdPrefix, "%s", statsdHostKey, -1)
		if prefixWithHost[len(prefixWithHost)-1] != '.' {
			prefixWithHost += "."
		}
		opts.StatsdPrefix = prefixWithHost // 带主机的前缀
	}

	return n, nil
}

func (n *NSQD) getOpts() *Options {
	return n.opts.Load().(*Options)
}

func (n *NSQD) swapOpts(opts *Options) {
	n.opts.Store(opts)
}

func (n *NSQD) triggerOptsNotification() {
	select {
	case n.optsNotificationChan <- struct{}{}:
	default:
	}
}

func (n *NSQD) RealTCPAddr() *net.TCPAddr {
	if n.tcpListener == nil {
		return &net.TCPAddr{}
	}
	return n.tcpListener.Addr().(*net.TCPAddr)

}

func (n *NSQD) RealHTTPAddr() *net.TCPAddr {
	if n.httpListener == nil {
		return &net.TCPAddr{}
	}
	return n.httpListener.Addr().(*net.TCPAddr)
}

func (n *NSQD) RealHTTPSAddr() *net.TCPAddr {
	if n.httpsListener == nil {
		return &net.TCPAddr{}
	}
	return n.httpsListener.Addr().(*net.TCPAddr)
}

func (n *NSQD) SetHealth(err error) {
	n.errValue.Store(errStore{err: err})
}

func (n *NSQD) IsHealthy() bool {
	return n.GetError() == nil
}

func (n *NSQD) GetError() error {
	errValue := n.errValue.Load()
	return errValue.(errStore).err
}

func (n *NSQD) GetHealth() string {
	err := n.GetError()
	if err != nil {
		return fmt.Sprintf("NOK - %s", err)
	}
	return "OK"
}

func (n *NSQD) GetStartTime() time.Time {
	return n.startTime
}

func (n *NSQD) Main() error {
	exitCh := make(chan error)
	var once sync.Once
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				n.logf(LOG_FATAL, "%s", err)
			}
			exitCh <- err
		})
	}

	n.waitGroup.Wrap(func() {
		exitFunc(protocol.TCPServer(n.tcpListener, n.tcpServer, n.logf))
	})
	if n.httpListener != nil {
		httpServer := newHTTPServer(n, false, n.getOpts().TLSRequired == TLSRequired)
		n.waitGroup.Wrap(func() {
			exitFunc(http_api.Serve(n.httpListener, httpServer, "HTTP", n.logf))
		})
	}
	if n.httpsListener != nil {
		httpsServer := newHTTPServer(n, true, true)
		n.waitGroup.Wrap(func() {
			exitFunc(http_api.Serve(n.httpsListener, httpsServer, "HTTPS", n.logf))
		})
	}

	n.waitGroup.Wrap(n.queueScanLoop) //
	n.waitGroup.Wrap(n.lookupLoop)
	if n.getOpts().StatsdAddress != "" {
		n.waitGroup.Wrap(n.statsdLoop)
	}

	err := <-exitCh
	return err
}

type meta struct {
	Topics []struct {
		Name     string `json:"name"`
		Paused   bool   `json:"paused"`
		Channels []struct {
			Name   string `json:"name"`
			Paused bool   `json:"paused"`
		} `json:"channels"`
	} `json:"topics"`
}

func newMetadataFile(opts *Options) string {
	return path.Join(opts.DataPath, "nsqd.dat")
}

func readOrEmpty(fn string) ([]byte, error) {
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read metadata from %s - %s", fn, err)
		}
	}
	return data, nil
}

func writeSyncFile(fn string, data []byte) error {
	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	f.Close()
	return err
}

// LoadMetadata 加载元数据，其中保存了 Topics Channels
func (n *NSQD) LoadMetadata() error {
	atomic.StoreInt32(&n.isLoading, 1) // 设置正在加载元数据状态
	defer atomic.StoreInt32(&n.isLoading, 0)

	fn := newMetadataFile(n.getOpts()) // 获取元数据文件Path /xx/x/nsqd.dat

	data, err := readOrEmpty(fn) // 空或读取数据
	if err != nil {
		return err
	}
	if data == nil {
		return nil // fresh start
	}

	var m meta // 持久化数据结构
	err = json.Unmarshal(data, &m)
	if err != nil {
		return fmt.Errorf("failed to parse metadata in %s - %s", fn, err)
	}

	for _, t := range m.Topics {
		if !protocol.IsValidTopicName(t.Name) {
			n.logf(LOG_WARN, "skipping creation of invalid topic %s", t.Name)
			continue
		}
		topic := n.GetTopic(t.Name) // 获取或创建一个 *Topic
		if t.Paused {
			topic.Pause()
		}
		for _, c := range t.Channels {
			if !protocol.IsValidChannelName(c.Name) {
				n.logf(LOG_WARN, "skipping creation of invalid channel %s", c.Name)
				continue
			}
			channel := topic.GetChannel(c.Name) // 获取或创建一个 *Channel
			if c.Paused {                       // 设置Pause
				channel.Pause()
			}
		}
		topic.Start()
	}
	return nil
}

// PersistMetadata 持久化元数据
func (n *NSQD) PersistMetadata() error {
	// persist metadata about what topics/channels we have, across restarts
	fileName := newMetadataFile(n.getOpts())

	n.logf(LOG_INFO, "NSQ: persisting topic/channel metadata to %s", fileName)

	// 封装持久化数据
	js := make(map[string]interface{})
	topics := []interface{}{}
	for _, topic := range n.topicMap {
		if topic.ephemeral {
			continue
		}
		topicData := make(map[string]interface{})
		topicData["name"] = topic.name
		topicData["paused"] = topic.IsPaused()
		channels := []interface{}{}
		topic.Lock()
		for _, channel := range topic.channelMap {
			if channel.ephemeral {
				continue
			}
			channel.Lock()
			channelData := make(map[string]interface{})
			channelData["name"] = channel.name
			channelData["paused"] = channel.IsPaused()
			channel.Unlock()
			channels = append(channels, channelData)
		}
		topic.Unlock()
		topicData["channels"] = channels
		topics = append(topics, topicData)
	}
	js["version"] = version.Binary
	js["topics"] = topics

	data, err := json.Marshal(&js)
	if err != nil {
		return err
	}

	// 创建新文件
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	err = writeSyncFile(tmpFileName, data) // 写入数据
	if err != nil {
		return err
	}
	err = os.Rename(tmpFileName, fileName) // 新文件替换旧文件
	if err != nil {
		return err
	}
	// technically should fsync DataPath here

	return nil
}

func (n *NSQD) Exit() {
	if !atomic.CompareAndSwapInt32(&n.isExiting, 0, 1) {
		// avoid double call
		return
	}
	if n.tcpListener != nil {
		n.tcpListener.Close()
	}

	if n.tcpServer != nil {
		n.tcpServer.Close()
	}

	if n.httpListener != nil {
		n.httpListener.Close()
	}

	if n.httpsListener != nil {
		n.httpsListener.Close()
	}

	n.Lock()
	err := n.PersistMetadata()
	if err != nil {
		n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
	}
	n.logf(LOG_INFO, "NSQ: closing topics")
	for _, topic := range n.topicMap {
		topic.Close()
	}
	n.Unlock()

	n.logf(LOG_INFO, "NSQ: stopping subsystems")
	close(n.exitChan)
	n.waitGroup.Wait()
	n.dl.Unlock()
	n.logf(LOG_INFO, "NSQ: bye")
	n.ctxCancel()
}

// GetTopic performs a thread safe operation
// to return a pointer to a Topic object (potentially new)
func (n *NSQD) GetTopic(topicName string) *Topic {
	// most likely we already have this topic, so try read lock first
	// 很可能我们已经有了这个主题，所以先尝试读锁
	n.RLock()
	t, ok := n.topicMap[topicName]
	n.RUnlock()
	if ok {
		return t
	}

	n.Lock() // TODO 有效避免竞争

	t, ok = n.topicMap[topicName]
	if ok {
		n.Unlock()
		return t
	}
	deleteCallback := func(t *Topic) { // 删除-回调函数
		n.DeleteExistingTopic(t.name)
	}
	t = NewTopic(topicName, n, deleteCallback) // 创建一个Topic，开启messagePump，等待start。
	n.topicMap[topicName] = t                  // 存到nsqd.topicMap中

	n.Unlock()

	n.logf(LOG_INFO, "TOPIC(%s): created", t.name)
	// topic is created but messagePump not yet started

	// if this topic was created while loading metadata at startup don't do any further initialization
	// (topic will be "started" after loading completes)
	// 如果当前 nsqd 处于 loading metadata 的状态，在 load 完毕，会进行 topic.Start
	if atomic.LoadInt32(&n.isLoading) == 1 {
		return t
	}

	// if using lookupd, make a blocking call to get channels and immediately create them
	// to ensure that all channels receive published messages
	// 如果不处于 loading metadata 的状态，同时使用 nsqlookupd，那么请求 nsqlookupd，获取 topic 对应的全部 channel
	lookupdHTTPAddrs := n.lookupdHTTPAddrs() // 获取Lookupd的地址
	if len(lookupdHTTPAddrs) > 0 {
		channelNames, err := n.ci.GetLookupdTopicChannels(t.name, lookupdHTTPAddrs)
		if err != nil {
			n.logf(LOG_WARN, "failed to query nsqlookupd for channels to pre-create for topic %s - %s", t.name, err)
		}
		for _, channelName := range channelNames {
			if strings.HasSuffix(channelName, "#ephemeral") { // 是否是临时通道
				continue // do not create ephemeral channel with no consumer client
			}
			t.GetChannel(channelName) // 获取或创建一个 *Channel
		}
	} else if len(n.getOpts().NSQLookupdTCPAddresses) > 0 {
		n.logf(LOG_ERROR, "no available nsqlookupd to query for channels to pre-create for topic %s", t.name)
	}

	// now that all channels are added, start topic messagePump
	t.Start() // 现在所有频道都已添加，开始主题 messagePump
	return t
}

// GetExistingTopic gets a topic only if it exists
func (n *NSQD) GetExistingTopic(topicName string) (*Topic, error) {
	n.RLock()
	defer n.RUnlock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	return topic, nil
}

// DeleteExistingTopic removes a topic only if it exists
func (n *NSQD) DeleteExistingTopic(topicName string) error {
	n.RLock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		n.RUnlock()
		return errors.New("topic does not exist")
	}
	n.RUnlock()

	// delete empties all channels and the topic itself before closing
	// (so that we dont leave any messages around)
	// delete 在关闭之前清空所有频道和主题本身
	// we do this before removing the topic from map below (with no lock)
	// so that any incoming writes will error and not create a new topic
	// to enforce ordering
	topic.Delete()

	n.Lock()
	delete(n.topicMap, topicName)
	n.Unlock()

	return nil
}

// Notify 持久化元数据，告知 lookupLoop，topic 被删除了，执行 UNREGISTER 告知 nsqlookupd
func (n *NSQD) Notify(v interface{}, persist bool) {
	// since the in-memory metadata is incomplete,
	// should not persist metadata while loading it.
	// nsqd will call `PersistMetadata` it after loading
	loading := atomic.LoadInt32(&n.isLoading) == 1
	n.waitGroup.Wrap(func() {
		// by selecting on exitChan we guarantee that
		// we do not block exit, see issue #123
		select {
		case <-n.exitChan:
		case n.notifyChan <- v: // 持久化元数据
			if loading || !persist {
				return
			}
			n.Lock()
			err := n.PersistMetadata()
			if err != nil {
				n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
			}
			n.Unlock()
		}
	})
}

// channels returns a flat slice of all channels in all topics
func (n *NSQD) channels() []*Channel {
	var channels []*Channel
	n.RLock()
	for _, t := range n.topicMap {
		t.RLock()
		for _, c := range t.channelMap {
			channels = append(channels, c)
		}
		t.RUnlock()
	}
	n.RUnlock()
	return channels
}

// resizePool adjusts the size of the pool of queueScanWorker goroutines
//
// 	1 <= pool <= min(num * 0.25, QueueScanWorkerPoolMax)
// 调整queueScanWorker goroutine 池的大小
func (n *NSQD) resizePool(num int, workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	idealPoolSize := int(float64(num) * 0.25) // 理想线程池数量,默认最多是 4
	if idealPoolSize < 1 {
		idealPoolSize = 1
	} else if idealPoolSize > n.getOpts().QueueScanWorkerPoolMax {
		idealPoolSize = n.getOpts().QueueScanWorkerPoolMax
	}
	for {
		if idealPoolSize == n.poolSize {
			break
		} else if idealPoolSize < n.poolSize {
			// contract
			// 协程池协程容量 < 当前已经开启的协程数量
			// 说明开启的协程过多，需要关闭协程
			// closeCh queueScanWorker会中断“守护”协程
			// 关闭后，将当前开启的协程数量-1
			closeCh <- 1 // 随机关闭一个
			n.poolSize--
		} else {
			// expand
			// 协程池协程容量 > 当前已经开启的协程数量
			// 开启新的协程
			n.waitGroup.Wrap(func() {
				n.queueScanWorker(workCh, responseCh, closeCh)
			})
			n.poolSize++
		}
	}
}

// queueScanWorker receives work (in the form of a channel) from queueScanLoop
// and processes the deferred and in-flight queues
// worker从queueScanLoop接收需要处理的channel，处理该channel的in-flight数据与deferred数据。
// processInFlightQueue与processDeferredQueue函数都会调用c.put(msg)，将数据发送到Channel的memoryMsgChan，进而重新被push到消费者。
func (n *NSQD) queueScanWorker(workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	for {
		select {
		case c := <-workCh: // workCh就是从nsqd.topicMap.channelMap中 随机 选择的channels
			now := time.Now().UnixNano()
			dirty := false
			if c.processInFlightQueue(now) { // 消费确认优先级队列
				dirty = true
			}
			if c.processDeferredQueue(now) { // 消费延时优先级队列
				dirty = true
			}
			responseCh <- dirty
		case <-closeCh: // 关闭工作线程
			return
		}
	}
}

// queueScanLoop runs in a single goroutine to process in-flight and deferred
// priority queues. It manages a pool of queueScanWorker (configurable max of
// QueueScanWorkerPoolMax (default: 4)) that process channels concurrently.
//
// It copies Redis's probabilistic expiration algorithm: it wakes up every
// QueueScanInterval (default: 100ms) to select a random QueueScanSelectionCount
// (default: 20) channels from a locally cached list (refreshed every
// QueueScanRefreshInterval (default: 5s)).
//
// If either of the queues had work to do the channel is considered "dirty".
//
// If QueueScanDirtyPercent (default: 25%) of the selected channels were dirty,
// the loop continues without sleep.
// 维护并管理 goroutine 池的数量， 这些 goroutine 主要用于处理 channel 中 延时优先级队列和等待消费确认优先级队列。
// 同时 queueScanLoop 循环随机选择 channel 并交给工作线程池进行处理。
// queueScanLoop的处理方法模仿了Redis的概率到期算法(probabilistic expiration algorithm)：每过一个QueueScanInterval(默认100ms)间隔，
// 进行一次概率选择，从所有的channel缓存中随机选择QueueScanSelectionCount(默认20)个channel，如果某个被选中channel的任何一个queue有事可做，
// 则认为该channel为“脏”channel。如果被选中channel中“脏”channel的比例大于QueueScanDirtyPercent(默认25%)，则不投入睡眠，直接进行下一次概率选择。
// channel缓存每QueueScanRefreshInterval(默认5s)刷新一次。
func (n *NSQD) queueScanLoop() {
	// 发送Channel用
	workCh := make(chan *Channel, n.getOpts().QueueScanSelectionCount)
	responseCh := make(chan bool, n.getOpts().QueueScanSelectionCount)
	closeCh := make(chan int)

	// worker 定时器，用于定时选取一定数量的 channel 默认100s
	workTicker := time.NewTicker(n.getOpts().QueueScanInterval)
	// refersh 定时器，用于定时更新刷新channel数量，重新调整协程池，默认时间是5s刷新一次
	refreshTicker := time.NewTicker(n.getOpts().QueueScanRefreshInterval)

	channels := n.channels()
	n.resizePool(len(channels), workCh, responseCh, closeCh)

	for {
		select {
		case <-workTicker.C:
			if len(channels) == 0 {
				continue
			}
		case <-refreshTicker.C: // 每5s刷新一次，获取Channel，调整queueScanWorker goroutine 池的大小
			channels = n.channels()
			n.resizePool(len(channels), workCh, responseCh, closeCh)
			continue
		case <-n.exitChan:
			goto exit
		}

		// 每次扫描最大的channel数量，默认是20，如果channel的数量小于这个值，则以channel的数量为准。
		num := n.getOpts().QueueScanSelectionCount
		if num > len(channels) {
			num = len(channels)
		}

	loop:
		// UniqRands 是作用是，从 channels 中随机选择 num 个 channel
		for _, i := range util.UniqRands(num, len(channels)) {
			workCh <- channels[i]
		}

		// 接收worker结果,等待这num个channel的处理结果(是否为“脏”channel)， 统计有多少channel是"脏"的，
		numDirty := 0
		for i := 0; i < num; i++ {
			if <-responseCh {
				numDirty++
			}
		}

		// 如果dirty的数量超过配置直接进行下一轮
		// 如果“脏”channel达到一定比例，直接进行下次处理
		if float64(numDirty)/float64(num) > n.getOpts().QueueScanDirtyPercent {
			goto loop
		}
	}

exit:
	n.logf(LOG_INFO, "QUEUESCAN: closing")
	close(closeCh)
	workTicker.Stop()
	refreshTicker.Stop()
}

func buildTLSConfig(opts *Options) (*tls.Config, error) {
	var tlsConfig *tls.Config

	if opts.TLSCert == "" && opts.TLSKey == "" {
		return nil, nil
	}

	tlsClientAuthPolicy := tls.VerifyClientCertIfGiven

	cert, err := tls.LoadX509KeyPair(opts.TLSCert, opts.TLSKey)
	if err != nil {
		return nil, err
	}
	switch opts.TLSClientAuthPolicy {
	case "require":
		tlsClientAuthPolicy = tls.RequireAnyClientCert
	case "require-verify":
		tlsClientAuthPolicy = tls.RequireAndVerifyClientCert
	default:
		tlsClientAuthPolicy = tls.NoClientCert
	}

	tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tlsClientAuthPolicy,
		MinVersion:   opts.TLSMinVersion,
	}

	if opts.TLSRootCAFile != "" {
		tlsCertPool := x509.NewCertPool()
		caCertFile, err := ioutil.ReadFile(opts.TLSRootCAFile)
		if err != nil {
			return nil, err
		}
		if !tlsCertPool.AppendCertsFromPEM(caCertFile) {
			return nil, errors.New("failed to append certificate to pool")
		}
		tlsConfig.ClientCAs = tlsCertPool
	}

	tlsConfig.BuildNameToCertificate()

	return tlsConfig, nil
}

func (n *NSQD) IsAuthEnabled() bool {
	return len(n.getOpts().AuthHTTPAddresses) != 0
}

// Context returns a context that will be canceled when nsqd initiates the shutdown
func (n *NSQD) Context() context.Context {
	return n.ctx
}
