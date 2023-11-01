package config

import (
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/errors"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/interfaces"
)

/*
Error handler that should controll stack flow by creating/wraping errors.
*/
var Error interfaces.Error = errors.NewError()
