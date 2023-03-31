package types

type KafkaClientConfig interface {
	GetBrokers() []string
	GetGroupId() string
	GetProducerTopic() *string
	GetConsumerTopic() *string
}
