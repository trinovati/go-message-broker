package interfaces

import dto_pkg "github.com/trinovati/go-message-broker/dto"

/*
Publisher must implement the basic Behavior for dealing with internal control and implement the
interface that represents the port this adapter is solving.
*/
type Publisher interface {
	Behavior
	Publish(publishing dto_pkg.BrokerPublishing) error
}
