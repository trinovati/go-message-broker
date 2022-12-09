package dto

import (
	"sync"

	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/config"
)

type BehaviourDto struct {
	BehaviourType             string
	ExchangeName              string
	ExchangeType              string
	QueueName                 string
	AccessKey                 string
	Qos                       int
	PurgeBeforeStarting       bool
	OutgoingDeliveryChannel   chan interface{}
	UnacknowledgedDeliveryMap *sync.Map
}

func NewBehaviourDto() *BehaviourDto {
	return &BehaviourDto{}
}

func (dto *BehaviourDto) FillPublisherData(exchangeName string, exchangeType string, queueName string, accessKey string) *BehaviourDto {
	dto.BehaviourType = config.RABBITMQ_PUBLISHER_BEHAVIOUR

	dto.ExchangeName = exchangeName
	dto.ExchangeType = exchangeType
	dto.QueueName = queueName
	dto.AccessKey = accessKey

	return dto
}

func (dto *BehaviourDto) FillConsumerData(exchangeName string, exchangeType string, queueName string, accessKey string, qos int, purgeBeforeStarting bool, outgoingDeliveryChannel chan interface{}, unacknowledgedDeliveryMap *sync.Map) *BehaviourDto {
	dto.BehaviourType = config.RABBITMQ_CONSUMER_BEHAVIOUR

	dto.ExchangeName = exchangeName
	dto.ExchangeType = exchangeType
	dto.QueueName = queueName
	dto.AccessKey = accessKey
	dto.Qos = qos
	dto.PurgeBeforeStarting = purgeBeforeStarting
	dto.OutgoingDeliveryChannel = outgoingDeliveryChannel
	dto.UnacknowledgedDeliveryMap = unacknowledgedDeliveryMap

	return dto
}
