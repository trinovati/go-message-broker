package rabbitmq

import "gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/config"

func (rmq *RabbitMQ) GetDeliveryChannel() (gobMessageChannel chan []byte, err error) {
	if rmq.Consumer == nil {
		return nil, config.Error.New("rabbitmq don't have a staged consumer")
	}

	return rmq.Consumer.GetDeliveryChannel()
}

/*
Return the channel that will be used to send messages into the service.
*/
func (consumer *Consumer) GetDeliveryChannel() (gobMessageChannel chan []byte, err error) {
	return consumer.DeliveryChannel, nil
}
