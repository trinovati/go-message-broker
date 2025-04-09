package interfaces

import (
	"github.com/trinovati/go-message-broker/v3/pkg/port"
)

type Consumer interface {
	Behavior
	port.Consumer
}
