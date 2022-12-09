package config

import "os"

/*
Returns the environment variable value if it's available, otherwise returns the given default value.
*/
func GetEnvOrDefault(environmentVariableName string, defaultValue string) string {
	environmentVariable := os.Getenv(environmentVariableName)
	if environmentVariable == "" {
		return defaultValue
	}

	return environmentVariable
}

/*
Environmental variable used by RabbitMQ service for connection with server.
*/
var (
	RABBITMQ_PROTOCOL = GetEnvOrDefault("RABBITMQ_PROTOCOL", "amqp")
	RABBITMQ_SERVER   = GetEnvOrDefault("RABBITMQ_SERVER", "guest:guest@localhost:5672")
)

/*
Value for identification of object behavior.
*/
var (
	RABBITMQ_PUBLISHER_BEHAVIOUR = "consumer"
	RABBITMQ_CONSUMER_BEHAVIOUR  = "publisher"
)

/*
Value for identification of server or client behavior.
*/
var (
	RABBITMQ_RPC_CLIENT = "rpc_client"
	RABBITMQ_RPC_SERVER = "rpc_server"
	RABBITMQ_CLIENT     = "client"

	/*
		Environmental variable that indicates if the pod is a client or a server.
	*/
	RABBITMQ_SERVICE = GetEnvOrDefault("RABBITMQ_SERVICE", RABBITMQ_CLIENT)
)
