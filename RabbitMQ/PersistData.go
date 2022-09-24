package rabbitmq

import (
	"errors"
	"fmt"
)

/*
Choose the right method for publish data to the queue linked to RabbitMQ.PublishData object.
*/
func (r *RabbitMQ) PersistData(transformedMessage interface{}, newTarget string, persistFormatFlag string) (err error) {
	errorFileIdentification := "RabbitMQ.go at PersistData()"

	switch message := transformedMessage.(type) {
	case string:
		err := r.Publish(message, newTarget)
		if err != nil {
			return errors.New("error publishing in " + errorFileIdentification + ": " + err.Error())
		}

	default:
		completeError := fmt.Sprintf("in %s: message have reached PersistData without implemented '%T' format", errorFileIdentification, transformedMessage)
		return errors.New(completeError)
	}

	return nil
}
