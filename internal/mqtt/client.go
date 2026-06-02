// Package mqtt provides an MQTT client implementing collector.Publisher and writeback.Transport.
package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"go-opcua-connector/internal/collector"
	"go-opcua-connector/internal/config"
	"go-opcua-connector/internal/model"
	"go-opcua-connector/internal/writeback"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
)

var _ collector.Publisher = (*Client)(nil)
var _ writeback.Transport = (*Client)(nil)

// Client MQTT客户端，同时实现 collector.Publisher 和 writeback.Transport。
type Client struct {
	config    *config.MQTTConfig
	client    mqtt.Client
	logger    *zap.Logger
	connected atomic.Bool
}

// NewClient 创建新的MQTT客户端。
func NewClient(cfg *config.MQTTConfig, logger *zap.Logger) *Client {
	return &Client{config: cfg, logger: logger}
}

// Connect 连接到MQTT Broker。
func (c *Client) Connect(ctx context.Context) error {
	brokerList := splitBrokers(c.config.Brokers)
	if len(brokerList) == 0 {
		return fmt.Errorf("no MQTT brokers configured")
	}

	clientID := c.config.ClientID
	if clientID == "" {
		clientID = fmt.Sprintf("go-opcua-connector-%d", time.Now().UnixNano())
	}

	opts := mqtt.NewClientOptions()
	for _, broker := range brokerList {
		opts.AddBroker(broker)
	}
	opts.SetClientID(clientID)
	opts.SetCleanSession(c.config.CleanSession)
	opts.SetKeepAlive(time.Duration(c.config.KeepAliveSec) * time.Second)
	opts.SetConnectTimeout(time.Duration(c.config.ConnectTimeoutSec) * time.Second)
	opts.SetAutoReconnect(c.config.AutoReconnect)
	opts.SetMaxReconnectInterval(time.Duration(c.config.MaxReconnectDelaySec) * time.Second)
	if c.config.Username != "" {
		opts.SetUsername(c.config.Username)
	}
	if c.config.Password != "" {
		opts.SetPassword(c.config.Password)
	}

	opts.OnConnect = func(client mqtt.Client) {
		c.logger.Info("MQTT connected")
		c.connected.Store(true)
	}
	opts.OnConnectionLost = func(client mqtt.Client, err error) {
		c.logger.Warn("MQTT connection lost", zap.Error(err))
		c.connected.Store(false)
	}
	opts.OnReconnecting = func(client mqtt.Client, opts *mqtt.ClientOptions) {
		c.logger.Info("MQTT reconnecting")
	}

	if c.config.TLSCertFile != "" || c.config.TLSCAFile != "" {
		tlsCfg, err := buildTLSConfig(c.config)
		if err != nil {
			return fmt.Errorf("failed to build TLS config: %w", err)
		}
		opts.SetTLSConfig(tlsCfg)
	}

	c.client = mqtt.NewClient(opts)
	token := c.client.Connect()
	if !token.WaitTimeout(time.Duration(c.config.ConnectTimeoutSec) * time.Second) {
		return fmt.Errorf("MQTT connect timeout")
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("failed to connect to MQTT broker: %w", err)
	}

	c.connected.Store(true)
	c.logger.Info("Connected to MQTT broker",
		zap.String("brokers", c.config.Brokers),
		zap.String("client_id", clientID))
	return nil
}

// PublishBatch 批量发布数据点，实现 collector.Publisher。
func (c *Client) PublishBatch(ctx context.Context, topic string, points []model.DataPoint) error {
	if !c.connected.Load() {
		return fmt.Errorf("not connected to MQTT")
	}
	if len(points) == 0 {
		return nil
	}

	actualTopic := c.config.Topic
	if actualTopic == "" {
		actualTopic = topic
	}

	batchMsg := model.NewBatchMessage(points)

	data, err := json.Marshal(batchMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal batch message: %w", err)
	}

	done := make(chan error, 1)
	go func() {
		token := c.client.Publish(actualTopic, byte(c.config.Qos), c.config.Retained, data)
		token.Wait()
		done <- token.Error()
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// IsConnected 检查连接状态。
func (c *Client) IsConnected() bool {
	return c.connected.Load()
}

// Subscribe 订阅命令消息，实现 writeback.Transport。
func (c *Client) Subscribe(ctx context.Context, subject string, handler func(data []byte)) (writeback.Subscription, error) {
	qos := byte(c.config.Qos)
	token := c.client.Subscribe(subject, qos, func(client mqtt.Client, msg mqtt.Message) {
		handler(msg.Payload())
	})

	done := make(chan error, 1)
	go func() {
		token.Wait()
		done <- token.Error()
	}()

	select {
	case err := <-done:
		if err != nil {
			return nil, fmt.Errorf("failed to subscribe to MQTT topic %s: %w", subject, err)
		}
		return &mqttSubscription{
			client: c.client,
			topic:  subject,
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// PublishResult 发布回写结果，实现 writeback.Transport。
func (c *Client) PublishResult(ctx context.Context, subject string, data []byte) error {
	if !c.connected.Load() {
		return fmt.Errorf("not connected to MQTT")
	}

	done := make(chan error, 1)
	go func() {
		token := c.client.Publish(subject, byte(c.config.Qos), false, data)
		token.Wait()
		done <- token.Error()
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close 断开MQTT连接。
func (c *Client) Close() {
	if c.client != nil && c.client.IsConnected() {
		c.client.Disconnect(250)
	}
	c.connected.Store(false)
	c.logger.Info("MQTT client closed")
}

type mqttSubscription struct {
	client mqtt.Client
	topic  string
}

func (s *mqttSubscription) Unsubscribe() error {
	token := s.client.Unsubscribe(s.topic)
	token.Wait()
	return token.Error()
}

func splitBrokers(s string) []string {
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		t := strings.TrimSpace(p)
		if t != "" {
			result = append(result, t)
		}
	}
	return result
}

func buildTLSConfig(cfg *config.MQTTConfig) (*tls.Config, error) {
	tlsCfg := &tls.Config{InsecureSkipVerify: cfg.InsecureSkipVerify}
	if cfg.TLSCAFile != "" {
		caCert, err := os.ReadFile(cfg.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA cert: %w", err)
		}
		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsCfg.RootCAs = certPool
	}
	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client cert/key pair: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return tlsCfg, nil
}
