package interfaces

import (
	"github.com/trinovati/go-message-broker/v3/pkg/port"
)

type Acknowledger interface {
	port.Acknowledger
}
