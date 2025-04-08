package interfaces

import (
	"context"

	dto_pkg "github.com/trinovati/go-message-broker/v3/dto"
)

/*
Consumer must implement the basic Behavior for dealing with internal control and implement the
interface that represents the port this adapter is solving.
*/
type Consumer interface {
	Behavior
	ConsumeForever(ctx context.Context)
	BreakConsume()
	Deliveries() (deliveries chan dto_pkg.BrokerDelivery)
	Acknowledge(acknowledge dto_pkg.BrokerAcknowledge) error
}
