// The dto_pkg package contains data transfer objects (DTOs) used for processing broker messages and handling document errors. It includes structures for acknowledging deliveries and reporting failed document processing.
package dto_pkg

import "github.com/trinovati/go-message-broker/constants" // Internal imports

/*
Carry relevant info to acknowledge a delivery.

MessageId is the id given by the broker to the delivery you're referencing.

Action is a selector of the treatment of the acknowledge like success, requeue and deadletter.

Report is a publish object that will be used to post on the deadletter queue should Action asks for it.
*/
type BrokerAcknowledge struct {
	MessageId string
	Action    constants.AcknowledgeAction
	Report    BrokerPublishing
}
