package kafka

import (
	"log"

	"github.com/Shopify/sarama"
)

func AddPartition(topic string, newPartitionCount int) {
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
