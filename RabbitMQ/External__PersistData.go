package rabbitmq

import (
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

	targets := strings.Split(newTarget, "@")

	exchange := targets[0]
	queue := ""
	if len(targets) > 1 {
		queue = targets[1]
	}

	for {
		switch message := transformedMessage.(type) {
		case string:

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
				return errors.New("in " + errorFileIdentification + ": no publisher was found")
			}

		default:
			completeError := fmt.Sprintf("in %s: message have reached PersistData without implemented '%T' format", errorFileIdentification, transformedMessage)
			return errors.New(completeError)
		}
	}
}
