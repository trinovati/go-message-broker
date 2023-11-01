package config

import "os"

/*
Environmental variable used by RabbitMQ service for connection with server.
*/
var (
	RABBITMQ_PROTOCOL = GetEnvOrDefault("RABBITMQ_PROTOCOL", "amqp")
	RABBITMQ_HOST     = GetEnvOrDefault("RABBITMQ_HOST", "localhost")
	RABBITMQ_PORT     = GetEnvOrDefault("RABBITMQ_PORT", "5672")
	RABBITMQ_USERNAME = GetEnvOrDefault("RABBITMQ_USERNAME", "guest")
	RABBITMQ_PASSWORD = GetEnvOrDefault("RABBITMQ_PASSWORD", "guest")
)

/*
Value for identification of object behavior.
*/
var (
	RABBITMQ_PUBLISHER_BEHAVIOUR = "consumer"
	RABBITMQ_CONSUMER_BEHAVIOUR  = "publisher"
)

func GetEnvOrDefault(environmentVariableName string, defaultValue string) string {
	environmentVariable := os.Getenv(environmentVariableName)
	if environmentVariable == "" {
		return defaultValue
	}

	return environmentVariable
}
