package dto

/*
Generic object that message broker for optional temporary configuration.
*/
type Target struct {
	Exchange     string
	ExchangeType string
	Queue        string
	AccessKey    string
	Qos          int
	Purge        bool
}
