package main

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
	"time"
)

/**
 * 普通消费者
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

	// 使用客户端创建消费者
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		// topic完整路径，格式为persistent://集群（租户）ID/命名空间/Topic名称
		Topic: "persistent://pulsar-xxx/sdk_go/topic1",
		// 订阅名称
		SubscriptionName: "topic1_sub",
		// 订阅模式
		Type: pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	for i := 0; i < 10; i++ {
		// 获取消息
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		// 模拟业务处理
		fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
			msg.ID(), string(msg.Payload()))

		// 消费成功，回复ack，消费失败根据业务需要选择回复nack或ReconsumeLater
		consumer.Ack(msg)

	}

	if err := consumer.Unsubscribe(); err != nil {
		log.Fatal(err)
	}
}
