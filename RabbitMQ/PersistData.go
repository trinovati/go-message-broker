package rabbitmq

import (
	"errors"
)

/*
Choose the right method for publish data to the queue linked to RabbitMQ.PublishData object.
*/
func (r *RabbitMQ) PersistData(transformedMessage interface{}, persistFormatFlag string) (err error) {
	errorFileIdentification := "RabbitMQ.go at PersistData()"

	switch message := transformedMessage.(type) {
	case string:
		err := r.Publish(message)
		if err != nil {
			return errors.New("error publishing in " + errorFileIdentification + ": " + err.Error())
		}

	default:
		return errors.New("in " + errorFileIdentification + ": message have come with untreatable interface{}")
	}

	return nil
}
