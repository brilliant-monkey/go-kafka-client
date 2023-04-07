package kafka

import (
	"context"
	"errors"
	"io"
	"log"
	"time"

	"github.com/brilliant-monkey/go-kafka-client/types"
	"github.com/segmentio/kafka-go"
)

type KafkaClient struct {
	config       types.KafkaClientConfig
	writerConfig kafka.WriterConfig
	readerConfig kafka.ReaderConfig
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewKafkaClient(config types.KafkaClientConfig) *KafkaClient {
	var writerConfig kafka.WriterConfig
	if config.GetProducerTopic() == nil {
		log.Println("Kafka producer config not set.")
	} else {
		writerConfig = kafka.WriterConfig{
			Brokers:  config.GetBrokers(),
			Topic:    *config.GetProducerTopic(),
			Balancer: &kafka.RoundRobin{},
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

	ctx, cancel := context.WithCancel(context.Background())
	return &KafkaClient{
		config,
		writerConfig,
		readerConfig,
		ctx,
		cancel,
	}
}

func (client *KafkaClient) TestConnection() (err error) {
	if len(client.config.GetBrokers()) < 1 {
		err = errors.New("missing brokers in config")
		return
	}
	broker := client.config.GetBrokers()[0]
	conn, err := kafka.Dial("tcp", broker)
	if err == nil {
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
	err = client.TestConnection()
	if err != nil {
		return
	}

	go func() {
		log.Println("Waiting for close")
		<-client.ctx.Done()
		log.Println("closed")
	}()

	go func() {
		defer func() {
			log.Println("Closing Kafka connection...")
			closeErr := r.Close()
			if closeErr != nil {
				log.Println("An error occurred while closing the Kafka connection: ", err)
			}
			log.Println("Kafka connection closed.")
		}()

		log.Printf("Listening for Kafka messages on %s...", r.Config().Topic)
		for {
			select {
			case <-client.ctx.Done():
				log.Println("Hmmm... Context was completed.")
				return
			default:
				log.Println("Waiting for message...")
				m, err := r.ReadMessage(client.ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						log.Println("Kafka received shutdown.")
						return
					} else if err == io.EOF {
						log.Println("Kafka:", err)
						continue
					}
					log.Println("An error occurred reading message from Kafka: ", err)

					client.cancel()
					return
				}

				if err = callback(m.Value); err != nil {
					log.Println("An error has occurred processing a Kafka message.", err)
				}
			}
		}
	}()
	return nil
}

func (client *KafkaClient) Stop() (err error) {
	log.Println("Shutting down Kafka connection...")
	client.cancel()
	<-client.ctx.Done()
	err = client.ctx.Err()
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Println("An error occurred while shutting down the Kafka connection:", err)
		return
	}
	log.Println("Kafka connection is shutdown.")
	return
}
