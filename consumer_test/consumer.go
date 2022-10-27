package main

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sftfjugg/gopulsar/logger"
	log "github.com/sirupsen/logrus"
	"time"
)

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://10.250.xx.xx:6650", //支持："pulsar://localhost:6650,localhost:6651,localhost:6652"
		OperationTimeout:  60 * time.Second,
		ConnectionTimeout: 60 * time.Second,
		Logger:            logger.NewLoggerWithLogrus(log.StandardLogger(), "test.log"),
	})

	defer client.Close()

	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            "my-topic",
		SubscriptionName: "my-sub",
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
			msg.ID(), string(msg.Payload()))

		consumer.Ack(msg)
	}

	if err := consumer.Unsubscribe(); err != nil {
		log.Fatal(err)
	}
}
