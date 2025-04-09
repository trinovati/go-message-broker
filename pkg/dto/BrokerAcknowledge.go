// Package contains data transfer objects (DTOs) used for processing and handling broker deliveries and publishing.
// It includes structures for create publishing, receiving deliveries and acknowledging deliveries.
package dto_broker

import (
	"context"

	constant_broker "github.com/trinovati/go-message-broker/v3/pkg/constant"
)

/*
Carry relevant info to acknowledge a delivery.

MessageId is the id given by the broker to the delivery you're referencing.

Action is a selector of the treatment of the acknowledge like success, requeue and deadletter.

Report is a publish object that will be used to post on the deadletter queue should Action asks for it.

LoggingCtx is used to enrich the logs information the acknowledge may want to do. Use a context.WithValue,
the existing key-value pairs will appear at the log if the adapter have received a logger that supports
logging with contexts.
*/
type BrokerAcknowledge struct {
	MessageId  string
	Action     constant_broker.AcknowledgeAction
	Report     BrokerPublishing
	LoggingCtx context.Context
}
