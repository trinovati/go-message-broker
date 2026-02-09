
# Go Message Broker
## Description
This library have the objective of abstracting the complex logic of keep alive connections for continuous publishing and consumptions over a message broker.
It also abstracts publishing, consumptions and acknowledges, to be goroutine friendly and standardizing its structure for easy data transport.

## License
This project is released under the MIT license (see LICENSE file).

## **Technologies Used**  
- **Golang**
- **Docker**
- **RabbitMQ**

## **Setup**
go get github.com/trinovati/go-message-broker/v3

## Contact
* Roni:       roni.gasparetto@trinovati.com.br    Co-owner and CTO at Trinovati
* Fabio Boni: fabioboni96@hotmail.com             Maintainer and developer

## Authors and acknowledgment
* Fabio Boni

## **TODO**
* RabbitMQ module:
  - Connection may leak in memory if there is a goroutine keeping it alive forever and it becomes impossible to close it in case all its child channels are closed without calling CloseConnection() first.
  The connection must keep being independent of the channels, not being closed when a channel is closed, however, if all channels are closed, there will be no references left to close the connection. A solution must be found for this, perhaps adding the connection to the publisher and consumer objects, though the complexity this may introduce should be analyzed.
