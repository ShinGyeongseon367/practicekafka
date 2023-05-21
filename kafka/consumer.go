package kafka

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

func Consumer(topic string) {

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

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, int64(-2))
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
			log.Printf("Consumed message value %s\n", string(msg.Value))
			consumed++
		case <-signals:
			break ConsumerLoop
		}
	}

	log.Printf("Consumed: %d\n", consumed)

}

func ConsumerByOffset(topic string, partition int, offset int) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	connertionStringArr := []string{"localhost:9092"}

	consumer, err := sarama.NewConsumer(connertionStringArr, config)
	ExistedErrThenPanic(err)
	// panic 은 consumer 를 생성하는 과정에서 사용하고
	// Fatal 은 close 과정에서 에러가 터질 때 사용한다 ?! 이게 옳은것은 아니지만 ... 일단은 이렇게 생각을 해보자고
	defer func() { ExistedErrThenFatal(consumer.Close()) }()

	partitionConsumer, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetOldest)
	ExistedErrThenPanic(err)
	defer func() { ExistedErrThenFatal(err) }()

	// EXIT:
	// 	for {
	// 		select {
	// 		case msg := <-partitionConsumer.Messages():
	// 			log.Printf("Received message: topic=%s, partition=%d, offset=%d, key=%s, value=%s\n",
	// 				msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
	// 		default:
	// 			break EXIT
	// 		}
	// 	}

	// for i := range partitionConsumer.Messages() {
	// 	log.Println(i)
	// }
	select {
	case msg := <-partitionConsumer.Messages():
		log.Println(msg.Offset, " :=====: ", string(msg.Value))
	default:
		log.Println("이거는 defualt 입니다. 그런데 이게 먼저 타는 일은 없을 것 같은데  ??!")
		// 먼저 탈 수 있습니다. 메서드가 실행되자마자 채널이 바로 준비되는게 아니라서
		// default 가 먼저실행되서 데이터가 제대로 실행이 안됐었습니다.
	}
}

func ExistedErrThenFatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func ExistedErrThenPanic(err error) {
	if err != nil {
		panic(err)
	}
}

func ReadToLastOffset(topic string, brokers []string) {
	config := sarama.NewConfig()
	partition := int32(0)

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	// 여기서도 sarama.offsetNewest 로 설정하면 가장 최근에 들어오는 데이터가 없으면
	// 채널에 그냥 머물러 있기때문에 데이터가 있는지 없는지 확인이 필요
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, 20)

	if err != nil {
		panic(err)
	}
	defer partitionConsumer.Close()
	log.Println("이거는 마지막 오프셋 확인을 위한 로그: ", partitionConsumer.HighWaterMarkOffset())
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("Received message: topic=%s, partition=%d, offset=%d, key=%s, value=%s\n",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

			// 마지막 오프셋까지 읽은 경우 종료
			if msg.Offset == partitionConsumer.HighWaterMarkOffset()-1 {
				return
			}
		}
	}
}

func PrintlnTestForPkg() {
	fmt.Println("print가 정상적으로 되고 있습니다.")
}
