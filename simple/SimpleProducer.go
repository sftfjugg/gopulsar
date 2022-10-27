package main

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
	"strconv"
	"time"
)

/**
 * 同步生产者
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
	defer producer.Close()

	for i := 0; i < 10; i++ {
		// 发送消息
		messageID, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			// 消息内容
			Payload: []byte("hello go client, this is a message."),
			// 业务key
			Key: "yourKey" + strconv.Itoa(i%4),
			// 业务参数
			Properties: map[string]string{"key": "value"},
		})
		if err != nil {
			fmt.Println("Failed to publish message", err)
		}
		fmt.Println("Published messageId", messageID)
	}

	if err != nil {
		fmt.Println("Failed to publish message", err)
	}
	fmt.Println("Published message")
}
