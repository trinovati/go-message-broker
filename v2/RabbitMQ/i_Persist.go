package rabbitmq

import "gitlab.com/aplicacao/trinovati-connector-message-brokers/v2/RabbitMQ/config"

func (rmq *RabbitMQ) Persist(message []byte, gobTarget []byte) (err error) {
	if rmq.Publisher == nil {
		return config.Error.New("rabbitmq don't have a staged published")
	}

	return rmq.Publisher.Persist(message, gobTarget)
}

func (publisher *Publisher) Persist(message []byte, gobTarget []byte) (err error) {
	return publisher.Publish(message, gobTarget)
}
