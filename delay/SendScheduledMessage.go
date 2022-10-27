package main

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
	"time"
)

/**
 * 发送定时消息
 */
func main() {
	// 创建pulsar客户端
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		// 服务接入地址
		URL: "http://pulsar-xxx.tdmq-pulsar.ap-sh.public.tencenttdmq.com:8080",
		// 授权角色密钥
		Authentication:    pulsar.NewAuthenticationToken("eyJrZXlJZC......"),
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	defer client.Close()

	// 使用客户端创建生产者
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		// topic完整路径，格式为persistent://集群（租户）ID/命名空间/Topic名称
		Topic: "persistent://pulsar-xxx/sdk_go/topic1",
	})

	if err != nil {
		log.Fatal(err)
	}

	// 发送消息
	_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		// 消息内容
		Payload: []byte("hello go client, this is a scheduled message."),
		// 业务参数
		Properties: map[string]string{"key": "value"},
		// 可设置发送的具体时间
		DeliverAt: time.Now().Add(time.Second * 10),
	})

	defer producer.Close()

	if err != nil {
		fmt.Println("Failed to publish message", err)
	}
	fmt.Println("Published message")
}
