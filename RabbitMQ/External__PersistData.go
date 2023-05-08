package rabbitmq

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
)

/*
Choose the right method for publish data to the queue linked to RabbitMQ.PublishData object.

Implementing interface for Message Handle service.
*/
func (r *RabbitMQ) PersistData(transformedMessage interface{}, newTarget string, persistFormatFlag string) (err error) {
	errorFileIdentification := "RabbitMQ at PersistData()"

	var message string
	targets := strings.Split(newTarget, "@")

	exchange := targets[0]
	queue := ""
	if len(targets) > 1 {
		queue = targets[1]
	}

	switch messageTyping := transformedMessage.(type) {
	case string:
		message = messageTyping

	case map[string]interface{}:
		var jsonDestiny []byte

		jsonDestiny, err = json.Marshal(messageTyping)
		if err != nil {
			return errors.New("error unamrshalling json in : " + errorFileIdentification + "" + err.Error())
		}

		message = string(jsonDestiny)

	default:
		completeError := fmt.Sprintf("in %s: message have reached PersistData without implemented '%T' format", errorFileIdentification, transformedMessage)
		return errors.New(completeError)
	}

	isPublisherUnexistent := true
	for _, behaviour := range r.Behaviour {
		switch publisher := behaviour.(type) {
		case *Publisher:
			isPublisherUnexistent = false

			err := publisher.Publish(message, exchange, queue)
			if err != nil {
				log.Println("error publishing in " + errorFileIdentification + ": " + err.Error())
				continue
			}

			return nil
		}
	}

	if isPublisherUnexistent {
		return errors.New("in " + errorFileIdentification + ": no publisher was found at RabbitMQ object")
	}

	return nil
}
