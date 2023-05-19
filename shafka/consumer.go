package shafka

import (
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

func Temporary() {

	conf := sarama.NewConfig()
	conf.Producer.Return.Successes = true
	connectionStringArr := []string{"localhost:9092"}

	consumer, err := sarama.NewConsumer(connectionStringArr, conf)

	if err != nil {
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition("local-access-log", 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumed := 0
ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("Consumed message offset %d\n", msg.Offset)
			consumed++
		case <-signals:
			break ConsumerLoop
		}
	}

	log.Printf("Consumed: %d\n", consumed)

}
