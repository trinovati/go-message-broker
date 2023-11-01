package config

import (
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/v2/RabbitMQ/errors"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/v2/RabbitMQ/interfaces"
)

/*
Error handler that should controll stack flow by creating/wraping errors.
*/
var Error interfaces.Error = errors.NewError()
