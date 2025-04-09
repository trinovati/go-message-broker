package interfaces

import (
	"github.com/trinovati/go-message-broker/v3/port"
)

type Consumer interface {
	Behavior
	port.Consumer
}
