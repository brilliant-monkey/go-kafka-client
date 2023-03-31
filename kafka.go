package kafka

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/brilliant-monkey/go-kafka-client/types"
	"github.com/segmentio/kafka-go"
)

type KafkaClient struct {
	config       types.KafkaClientConfig
	writerConfig kafka.WriterConfig
	readerConfig kafka.ReaderConfig
}

func NewKafkaClient(config types.KafkaClientConfig) *KafkaClient {
	var writerConfig kafka.WriterConfig
	if config.GetProducerTopic() == nil {
		log.Println("Kafka producer config not set.")
	} else {
		writerConfig = kafka.WriterConfig{
			Brokers: config.GetBrokers(),
			Topic:   *config.GetProducerTopic(),
		}
	}

	var readerConfig kafka.ReaderConfig
	if config.GetConsumerTopic() == nil {
		log.Println("Kafka consumer config not set.")
	} else {
		readerConfig = kafka.ReaderConfig{
			Brokers: config.GetBrokers(),
			Topic:   *config.GetConsumerTopic(),
			GroupID: config.GetGroupId(),
		}
	}

	return &KafkaClient{
		config,
		writerConfig,
		readerConfig,
	}
}

func (client *KafkaClient) TestConnection() (err error) {
	if len(client.config.GetBrokers()) < 1 {
		err = errors.New("missing brokers in config")
		return
	}
	broker := client.config.GetBrokers()[0]
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		defer conn.Close()
	}
	return
}

func (client *KafkaClient) Produce(message []byte) (err error) {
	log.Printf("Producing a message on %s Kafka topic", client.writerConfig.Topic)
	writer := kafka.NewWriter(client.writerConfig)
	writer.AllowAutoTopicCreation = true

	defer writer.Close()

	const retries = 3
	for i := 0; i < retries; i++ {
		err = writer.WriteMessages(context.Background(), kafka.Message{
			Value: message,
		})
		if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) {
			log.Println("Failed to publish message. Leader not available yet. Retrying...")
			time.Sleep(time.Millisecond * 250)
			continue
		}
		break
	}
	return
}

func (client *KafkaClient) Consume(callback func(message []byte) error) (err error) {
	log.Println("Starting Kafka consumer...")
	r := kafka.NewReader(client.readerConfig)
	defer func() {
		log.Println("Closing connection to Kafka...")
		if err = r.Close(); err != nil {
			log.Printf("Failed to close Kafka reader: %s", err)
			return
		}
		log.Println("Kafka connection closed.")
	}()

	log.Println("Listening for Kafka messages...")
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Println("An error has occurred reading a Kafka message.", err)
			time.Sleep(time.Millisecond * 250)
			continue
		}
		if err = callback(m.Value); err != nil {
			log.Println("An error has occurred processing a Kafka message.", err)
		}
	}
}
