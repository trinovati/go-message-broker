package port

import (
	"context"

	dto_broker "github.com/trinovati/go-message-broker/v3/pkg/dto"
)

/*
Enforce a generic form of publish for any simple publisher service.
All adapters of this library must support this methods to keep concise behavior.

Publish is a simple message publishing action.
*/
type Publisher interface {
	Publish(ctx context.Context, publishing dto_broker.BrokerPublishing) error
}
