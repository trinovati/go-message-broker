package config

import (
	"github.com/trinovati/go-message-broker/RabbitMQ/errors"
	"github.com/trinovati/go-message-broker/RabbitMQ/interfaces"
)

/*
Error handler that should controll stack flow by creating/wraping errors.
*/
var Error interfaces.Error = errors.NewError()
