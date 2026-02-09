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

BreakConsume is a cancellation of ConsumeForever.

Deliveries is a getter for the channel that ConsumeForever is sending its deliveries.
*/
type Consumer interface {
	Acknowledger
	ConsumeForever(ctx context.Context)
	BreakConsume(ctx context.Context)
	IsConsuming() bool
	IsRunning() bool
	Deliveries() (deliveries chan dto_broker.BrokerDelivery)
}
