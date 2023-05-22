package main

import (
	"log"
	"sync"

	"github.com/ShinGyeongseon/practicekafka/kafka"
)

var wg sync.WaitGroup

func main() {

	topic := "temporary-topic2"
	brokers := []string{"localhost:9092"}

	for idx := 0; idx < 5; idx++ {
		log.Println("======================================", idx, "======================================")
		kafka.ReadToLastOffset(topic, brokers, int32(idx))
	}
	// kafka.Consumer(topic)

	// CreateMsg()

	// wg.Add(10)
	// for i := 1; i <= 10; i++ {
	// 	go func() {
	// 		defer wg.Done()
	// 		podName := "access-to-pod-B"
	// 		kafka.CreateMsgOne("temporary-topic2", podName, 0, "key001")
	// 	}()
	// }
	// wg.Wait()

	// kafka.AddPartition("temporary-topic2", 4)

	// CreateMsgByPartition()

	// ConsumerByOffset("temporary-topic2", 0, -2)
	// ConsumerByOffset("temporary-topic2", 1, -2)
	// ConsumerByOffset("temporary-topic2", 2, -2)
	// ConsumerByOffset("temporary-topic2", 3, -2)

	// Consumer("temporary-topic2")

	// manageConsumer()
}
