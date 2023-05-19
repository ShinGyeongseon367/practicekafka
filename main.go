package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
)

var wg sync.WaitGroup

func main() {
	// CreateMsg()

	wg.Add(10)
	for i := 1; i <= 10; i++ {
		go func() {
			defer wg.Done()
			podName := "access-to-pod-CCCCCCCC"
			CreateMsgOne("temporary-topic2", podName, 0, "key001")
		}()
	}
	wg.Wait()

	// addPartition("temporary-topic2", 4)

	// CreateMsgByPartition()
}

func addPartition(topic string, newPartitionCount int) {
	// Kafka 어드민 클라이언트 생성
	config := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatal("Failed to create Kafka admin client: ", err)
	}
	defer admin.Close()

	// 토픽의 현재 파티션 개수 확인
	topicMetadata, err := admin.DescribeTopics([]string{topic})
	if err != nil {
		log.Fatal("Failed to describe topic: ", err)
	}
	currentPartitionCount := len(topicMetadata[0].Partitions)

	// 추가할 파티션 개수 계산
	totalPartitionCount := currentPartitionCount + newPartitionCount

	// 파티션 추가
	err = admin.CreatePartitions(topic, int32(totalPartitionCount), nil, false)
	if err != nil {
		log.Fatal("Failed to create partitions: ", err)
	}

	log.Printf("Successfully added %d partitions to topic %s", newPartitionCount, topic)

}

// partition은 어떻게 사용해야하는건지 모르니까 일단 이거는 나중 문제로 사용합시다.
func CreateMsgOne(topic string, msg string, partition int, key string) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	existedErr(err)

	defer func() {
		existedErr(producer.Close())
	}()

	kafkaMessage := &sarama.ProducerMessage{
		Topic: topic,
		// Partition: int32(partition),
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(msg),
	}

	_, _, errr := producer.SendMessage(kafkaMessage)
	existedErr(errr)
}

func CreateMsgByPartition() {

	kafkaConn := "localhost:9092"
	topic := "temporary-topic2"

	// Sarama 설정 초기화
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	// Kafka 브로커에 연결
	client, err := sarama.NewClient([]string{kafkaConn}, config)
	if err != nil {
		log.Println("Unable to connect to Kafka:", err)
		os.Exit(1)
	}
	defer client.Close()

	// Kafka 프로듀서 생성
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		log.Println("Unable to create producer:", err)
		os.Exit(1)
	}
	defer producer.Close()

	// 파티션 번호 지정
	partitions := []int32{1, 2}

	// 각 파티션에 메시지 전송
	for _, p := range partitions {
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Value:     sarama.StringEncoder(fmt.Sprintf("Hello, from partition %d!", p)),
			Partition: p,
		}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("Failed to send message at partition %d: %s", p, err)
			continue
		}
		fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)
	}

}

func CreateMsg() {
	// Kafka 설정
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	// Kafka Producer 생성
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalln("Failed to create Kafka producer:", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln("Failed to close Kafka producer:", err)
		}
	}()

	// 콘솔 입력 대기
	fmt.Println("Enter a message to send to Kafka (Ctrl+C to exit):")
	reader := bufio.NewReader(os.Stdin)

	// Ctrl+C 시그널 처리
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt) // 이게 무슨 말인지 아는게 중요 .

	// 메시지 입력 및 Kafka 전송 루프
	for {
		select {
		case <-signals:
			fmt.Println("Exiting...")
			return
		default:
			// 콘솔에서 메시지 입력
			message, err := reader.ReadString('\n')
			if err != nil {
				log.Println("Failed to read input:", err)
				continue
			}

			// 입력한 메시지를 Kafka에 전송
			message = strings.TrimSpace(message)
			if message != "" {
				// Kafka 메시지 생성
				kafkaMessage := &sarama.ProducerMessage{
					Topic: "temporary-topic2", // 실제 사용하는 Kafka Topic 이름으로 변경해야 합니다.
					Value: sarama.StringEncoder(message),
				}

				// Kafka에 메시지 전송
				_, _, err := producer.SendMessage(kafkaMessage)
				if err != nil {
					log.Println("Failed to send message to Kafka:", err)
					continue
				}

				fmt.Println("Message sent to Kafka!")
			}
		}
	}
}

func Consumer() {

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

func existedErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
