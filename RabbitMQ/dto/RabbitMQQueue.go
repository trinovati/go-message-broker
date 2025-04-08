package dto

type RabbitMQQueue struct {
	Exchange     string
	ExchangeType string
	Name         string
	AccessKey    string
	Qos          int
	Purge        bool
}
