package main

import (
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
	"time"
)

/**
 * Key_Shared 订阅模式：如果key相同，消息会分配到同一个消费者
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

	// 使用channel
	channel := make(chan pulsar.ConsumerMessage, 100)

	// 订阅信息
	options := pulsar.ConsumerOptions{
		// topic完整路径，格式为persistent://集群（租户）ID/命名空间/Topic名称
		Topic: "persistent://pulsar-xxx/sdk_go/topic1",
		// 订阅名称
		SubscriptionName: "topic1_sub",
		// 订阅模式
		Type: pulsar.KeyShared,
	}

	options.MessageChannel = channel

	// 订阅消息
	consumer, err := client.Subscribe(options)
	if err != nil {
		log.Fatal(err)
	}

	defer consumer.Close()

	// 消费信息
	for cm := range channel {
		msg := cm.Message
		key := msg.Key()
		fmt.Printf("Received message  msgId: %v -- content: '%s' -- key: '%s'\n",
			msg.ID(), string(msg.Payload()), key)
		// 回复ack
		consumer.Ack(msg)
	}
}
