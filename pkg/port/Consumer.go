package port

import (
	"context"

	dto_broker "github.com/trinovati/go-message-broker/v3/pkg/dto"
)

/*
Enforce a generic form of consume for any simple consumer service.
All adapters of this library must support this methods to keep concise behavior.
All methods must work together somehow.

ConsumeForever must be a worker of infinite loop of consumptions from the message broker.
It must enforce a way of keep alive in case of connection failures.
It must have a intern worker to acknowledge deliveries via BrokerDelivery.Acknowledger channel.

BreakConsume is a cancellation of ConsumeForever.

Deliveries is a getter for the channel that ConsumeForever is sending its deliveries.

Acknowledge is a method to manually acknowledge a delivery that have come from ConsumeForever.
*/
type Consumer interface {
	ConsumeForever(ctx context.Context)
	BreakConsume(ctx context.Context)
	Deliveries(ctx context.Context) (deliveries chan dto_broker.BrokerDelivery)
	Acknowledge(acknowledge dto_broker.BrokerAcknowledge) error
}
