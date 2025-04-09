package dto_rabbitmq

type RabbitMQQueue struct {
	Exchange     string `json:"exchange"`
	ExchangeType string `json:"exchange_type"`
	Name         string `json:"name"`
	AccessKey    string `json:"access_key"`
	Qos          int    `json:"qos"`
	Purge        bool   `json:"purge"`
}
