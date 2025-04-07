package errors

import (
	"fmt"
	"log"

	"github.com/pkg/errors"
	"github.com/trinovati/go-message-broker/RabbitMQ/interfaces"
)

type Error struct {
	HttpStatus int
	HttpBody   string
	err        error
	stack      error
}

func NewError() *Error {
	return &Error{}
}

func (e *Error) SetStatus(status int) interfaces.Error {
	e.HttpStatus = status

	return e
}

func (e *Error) SetBody(body string) interfaces.Error {
	e.HttpBody = body

	return e
}

func (e Error) GetStatus() (status int) {
	return e.HttpStatus
}

func (e Error) GetBody() (body string) {
	return e.HttpBody
}

func (e Error) GetStack() (stack error) {
	return e.stack
}

func (e *Error) New(message string) interfaces.Error {
	return &Error{
		err:      errors.New(message),
		HttpBody: message,
		stack:    errors.New(" "),
	}
}

func (e *Error) Merge(err error) interfaces.Error {
	if err == nil {
		return e
	}

	if e.err == nil {
		e.err = errors.New(err.Error())
	} else if e.err.Error() == "" && err.Error() != "" {
		e.err = err
	}

	a, ok := err.(interfaces.Error)
	if ok {
		if e.HttpBody == "" && a.GetBody() != "" {
			e.SetBody(a.GetBody())
		}

		if e.HttpStatus == 0 && a.GetStatus() != 0 {
			e.SetStatus(a.GetStatus())
		}
	}

	aux, ok := err.(interfaces.Error)
	if ok {
		if aux.GetStack() != nil {
			e.stack = aux.GetStack()
		}
	}

	return e
}

func (e *Error) Wrap(err error, message string) interfaces.Error {
	var stack error
	var wrap = errors.Wrap(err, message)
	var body string
	var status int = 0

	if wrap == nil {
		wrap = errors.New(message)
	}

	body = wrap.Error()

	a, ok := err.(interfaces.Error)
	if ok {
		status = a.GetStatus()
	}

	aux, ok := err.(interfaces.Error)
	if ok && aux.GetStack() != nil {
		stack = aux.GetStack()
	} else {
		stack = errors.New(" ")
	}

	return &Error{
		err:        wrap,
		HttpBody:   body,
		HttpStatus: status,
		stack:      stack,
	}
}

// Incomplete
func (e *Error) Unwrap() error {
	return &Error{
		err: errors.Unwrap(e.err),
	}
}

func (e *Error) Error() string {
	return e.err.Error()
}

func (e *Error) String() string {
	return fmt.Sprintf("%s\n%+v", e.err.Error(), e.stack)
}

func (e *Error) Print() {
	log.Printf("\n------------------------------------------ ERROR ------------------------------------------\n%s\n%+v\n-------------------------------------------------------------------------------------------\n", e.Error(), e.stack)
}
