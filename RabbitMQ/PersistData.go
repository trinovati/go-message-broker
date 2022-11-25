package rabbitmq

import (
	"errors"
	"fmt"
	"strings"
)

/*
Choose the right method for publish data to the queue linked to RabbitMQ.PublishData object.

Implementing interface for Message Handle service.
*/
func (r *RabbitMQ) PersistData(transformedMessage interface{}, newTarget string, persistFormatFlag string) (err error) {
	errorFileIdentification := "RabbitMQ.go at PersistData()"

	targets := strings.Split(newTarget, "@")

	exchange := targets[0]
	queue := ""
	if len(targets) > 1 {
		queue = targets[1]
	}

	switch message := transformedMessage.(type) {
	case string:
		err := r.Publish(message, exchange, queue)
		if err != nil {
			return errors.New("error publishing in " + errorFileIdentification + ": " + err.Error())
		}

	default:
		completeError := fmt.Sprintf("in %s: message have reached PersistData without implemented '%T' format", errorFileIdentification, transformedMessage)
		return errors.New(completeError)
	}

	return nil
}
