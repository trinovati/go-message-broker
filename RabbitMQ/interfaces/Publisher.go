package interfaces

import "github.com/trinovati/go-message-broker/v3/pkg/port"

/*
Publisher must implement the basic Behavior for dealing with internal control and implement the
interface that represents the port this adapter is solving.
*/
type Publisher interface {
	Behavior
	port.Publisher
}
