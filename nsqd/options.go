package nsqd

import (
	"crypto/md5"
	"crypto/tls"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"

	"github.com/nsqio/nsq/internal/lg"
)

type Options struct {
	// basic options
	ID        int64       `flag:"node-id" cfg:"id"` // 消息 ID 的唯一部分，（int）范围 [0,1024）（默认是主机名的哈希）（默认 248）
	LogLevel  lg.LogLevel `flag:"log-level"`
	LogPrefix string      `flag:"log-prefix"` // 日志消息前缀
	Logger    Logger

	TCPAddress               string        `flag:"tcp-address"`                                        //
	HTTPAddress              string        `flag:"http-address"`                                       // 监听 HTTP 客户端（默认“0.0.0.0:4151”）
	HTTPSAddress             string        `flag:"https-address"`                                      // 监听 HTTPS 客户端（默认“0.0.0.0:4152”）
	BroadcastAddress         string        `flag:"broadcast-address"`                                  // 将使用lookupd 注册的地址（默认为操作系统主机名）（默认为“yourhost.local”）
	BroadcastTCPPort         int           `flag:"broadcast-tcp-port"`                                 // 将向lookupd 注册的TCP 端口（默认为该nsqd 正在侦听的TCP 端口）
	BroadcastHTTPPort        int           `flag:"broadcast-http-port"`                                // 将向lookupd 注册的HTTP 端口（默认为该nsqd 正在侦听的HTTP 端口）
	NSQLookupdTCPAddresses   []string      `flag:"lookupd-tcp-address" cfg:"nsqlookupd_tcp_addresses"` // 查找 TCP 地址
	AuthHTTPAddresses        []string      `flag:"auth-http-address" cfg:"auth_http_addresses"`        // 查询认证服务器
	HTTPClientConnectTimeout time.Duration `flag:"http-client-connect-timeout" cfg:"http_client_connect_timeout"`
	HTTPClientRequestTimeout time.Duration `flag:"http-client-request-timeout" cfg:"http_client_request_timeout"`

	// diskqueue options
	DataPath        string        `flag:"data-path"`          // 存储磁盘支持的消息的路径
	MemQueueSize    int64         `flag:"mem-queue-size"`     // 保留在内存中的消息数（每个主题/频道）（默认 10000）
	MaxBytesPerFile int64         `flag:"max-bytes-per-file"` // 滚动前每个磁盘队列文件的字节数（默认 104857600）
	SyncEvery       int64         `flag:"sync-every"`         // 每个磁盘队列 fsync 的消息数（默认 2500）
	SyncTimeout     time.Duration `flag:"sync-timeout"`       // 每个磁盘队列 fsync 的持续时间（默认 2 秒）

	QueueScanInterval        time.Duration // 扫描channel的时间间隔
	QueueScanRefreshInterval time.Duration // 刷新扫描的时间间隔
	QueueScanSelectionCount  int           `flag:"queue-scan-selection-count"` //
	QueueScanWorkerPoolMax   int           `flag:"queue-scan-worker-pool-max"` // 最大的扫描池数量
	QueueScanDirtyPercent    float64       // 标识百分比

	// msg and command options
	MsgTimeout    time.Duration `flag:"msg-timeout"`     // 在自动请求消息之前等待的默认持续时间（默认 1m0s）
	MaxMsgTimeout time.Duration `flag:"max-msg-timeout"` // 消息超时前的最长持续时间（默认 15m0s）
	MaxMsgSize    int64         `flag:"max-msg-size"`    // 单个消息的最大大小（以字节为单位）（默认 1048576）
	MaxBodySize   int64         `flag:"max-body-size"`   // 单个命令体的最大大小（默认 5242880）
	MaxReqTimeout time.Duration `flag:"max-req-timeout"` // 消息的最大重新排队超时（默认 1h0m0s）
	ClientTimeout time.Duration //

	// client overridable configuration options
	MaxHeartbeatInterval   time.Duration `flag:"max-heartbeat-interval"`    // 客户端心跳之间的最大客户端可配置持续时间（默认 1m0s）
	MaxRdyCount            int64         `flag:"max-rdy-count"`             // 客户端的最大 RDY 计数（默认 2500）
	MaxOutputBufferSize    int64         `flag:"max-output-buffer-size"`    // 客户端输出缓冲区的最大客户端可配置大小（以字节为单位）（默认 65536）
	MaxOutputBufferTimeout time.Duration `flag:"max-output-buffer-timeout"` // 刷新到客户端之间的最大客户端可配置持续时间（默认 30 秒）
	MinOutputBufferTimeout time.Duration `flag:"min-output-buffer-timeout"` // 刷新到客户端之间的最小客户端可配置持续时间（默认 25 毫秒）
	OutputBufferTimeout    time.Duration `flag:"output-buffer-timeout"`     // 将数据刷新到客户端之间的默认持续时间（默认 250 毫秒）
	MaxChannelConsumers    int           `flag:"max-channel-consumers"`     // 每个 nsqd 实例的最大通道消费者连接数（默认 0，即无限制）

	// statsd integration
	StatsdAddress          string        `flag:"statsd-address"`           // 用于推送统计信息的 statsd 守护进程的 UDP <addr>:<port>
	StatsdPrefix           string        `flag:"statsd-prefix"`            // 用于发送到 statsd 的密钥的前缀（用于主机替换的 %s）（默认为“nsq.%s”）
	StatsdInterval         time.Duration `flag:"statsd-interval"`          // 推送到 statsd 之间的持续时间（默认 1m0s）
	StatsdMemStats         bool          `flag:"statsd-mem-stats"`         // 将发送内存和 GC 统计信息切换到 statsd（默认为 true）
	StatsdUDPPacketSize    int           `flag:"statsd-udp-packet-size"`   // statsd UDP 数据包的字节大小（默认 508）
	StatsdExcludeEphemeral bool          `flag:"statsd-exclude-ephemeral"` //

	// e2e message latency
	// 计算这段时间的端到端延迟分位数
	E2EProcessingLatencyWindowTime time.Duration `flag:"e2e-processing-latency-window-time"`
	// 消息处理时间百分位数
	E2EProcessingLatencyPercentiles []float64 `flag:"e2e-processing-latency-percentile" cfg:"e2e_processing_latency_percentiles"`

	// TLS config
	TLSCert             string `flag:"tls-cert"`               // 证书文件路径
	TLSKey              string `flag:"tls-key"`                //
	TLSClientAuthPolicy string `flag:"tls-client-auth-policy"` //
	TLSRootCAFile       string `flag:"tls-root-ca-file"`       //
	TLSRequired         int    `flag:"tls-required"`           //
	TLSMinVersion       uint16 `flag:"tls-min-version"`        //

	// compression
	DeflateEnabled  bool `flag:"deflate"`           //
	MaxDeflateLevel int  `flag:"max-deflate-level"` // 客户机可以协商的最大 deflate 压缩级别(> values = = > nsqd CPU 使用量)(默认值6)
	SnappyEnabled   bool `flag:"snappy"`            // 启用 snappy 功能协商（客户端压缩）（默认为 true）
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	h := md5.New()
	io.WriteString(h, hostname)
	defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	return &Options{
		ID:        defaultID,
		LogPrefix: "[nsqd] ",
		LogLevel:  lg.INFO,

		TCPAddress:        "0.0.0.0:4150",
		HTTPAddress:       "0.0.0.0:4151",
		HTTPSAddress:      "0.0.0.0:4152",
		BroadcastAddress:  hostname,
		BroadcastTCPPort:  0,
		BroadcastHTTPPort: 0,

		NSQLookupdTCPAddresses: make([]string, 0),
		AuthHTTPAddresses:      make([]string, 0),

		HTTPClientConnectTimeout: 2 * time.Second,
		HTTPClientRequestTimeout: 5 * time.Second,

		MemQueueSize:    10000,
		MaxBytesPerFile: 100 * 1024 * 1024,
		SyncEvery:       2500,
		SyncTimeout:     2 * time.Second,

		QueueScanInterval:        100 * time.Millisecond,
		QueueScanRefreshInterval: 5 * time.Second,
		QueueScanSelectionCount:  20,
		QueueScanWorkerPoolMax:   4,
		QueueScanDirtyPercent:    0.25,

		MsgTimeout:    60 * time.Second,
		MaxMsgTimeout: 15 * time.Minute,
		MaxMsgSize:    1024 * 1024,
		MaxBodySize:   5 * 1024 * 1024,
		MaxReqTimeout: 1 * time.Hour,
		ClientTimeout: 60 * time.Second,

		MaxHeartbeatInterval:   60 * time.Second,
		MaxRdyCount:            2500,
		MaxOutputBufferSize:    64 * 1024,
		MaxOutputBufferTimeout: 30 * time.Second,
		MinOutputBufferTimeout: 25 * time.Millisecond,
		OutputBufferTimeout:    250 * time.Millisecond,
		MaxChannelConsumers:    0,

		StatsdPrefix:        "nsq.%s",
		StatsdInterval:      60 * time.Second,
		StatsdMemStats:      true,
		StatsdUDPPacketSize: 508,

		E2EProcessingLatencyWindowTime: time.Duration(10 * time.Minute),

		DeflateEnabled:  true,
		MaxDeflateLevel: 6,
		SnappyEnabled:   true,

		TLSMinVersion: tls.VersionTLS10,
	}
}
