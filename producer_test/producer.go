package main

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sftfjugg/gopulsar/logger"
	"log"
	"time"
)

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://10.250.11.33:6650", //支持："pulsar://localhost:6650,localhost:6651,localhost:6652"
		OperationTimeout:  60 * time.Second,
		ConnectionTimeout: 60 * time.Second,
		Logger:            logger.NewLoggerWithLogrus(log.StandardLogger(), "test.log"),
	})

	defer client.Close()

	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "my-topic",
	})

	if err != nil {
		log.Fatal(err)
	}

	_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: []byte("hello"),
	})

	defer producer.Close()

	if err != nil {
		fmt.Println("Failed to publish message", err)
	}
	fmt.Println("Published message")

}
