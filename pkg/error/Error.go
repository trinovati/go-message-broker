// Package defines helper errors that are wrapped to usage in combination with errors.Is.
package error_broker

import "errors"

var (
	ErrRetryPossible    error = errors.New("retry possible")
	ErrClosedConnection error = errors.New("closed connection")
)
