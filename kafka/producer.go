package kafka

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
)

func CreateMsgOne(topic string, msg string, partition int, key string) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	ExistedErrThenFatal(err)

	defer func() {
		ExistedErrThenFatal(producer.Close())
	}()

	kafkaMessage := &sarama.ProducerMessage{
		Topic: topic,
		// Partition: int32(partition),
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(msg),
	}

	_, _, errr := producer.SendMessage(kafkaMessage)
	ExistedErrThenFatal(errr)
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
