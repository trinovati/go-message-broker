package port

import (
	"context"

	constant_broker "github.com/trinovati/go-message-broker/v3/pkg/constant"
)

type Acknowledger interface {
	Acknowledge(
		ctx context.Context,
		id string,
		action constant_broker.AcknowledgeAction,
		header map[string]any,
		body []byte,
	) error
}
