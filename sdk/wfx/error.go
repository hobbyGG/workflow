package wfx

import (
	"fmt"
	"strings"
)

// ServiceError is an error reported in a workflow service response.
type ServiceError struct {
	Operation       string
	Code            int32
	Message         string
	InternalCode    int32
	InternalMessage string
}

func (e *ServiceError) Error() string {
	message := strings.TrimSpace(e.Message)
	if message == "" {
		message = strings.TrimSpace(e.InternalMessage)
	}
	if message == "" {
		message = "request failed"
	}
	if e.Code != 0 {
		return fmt.Sprintf("wfx: %s: %s (code %d)", e.Operation, message, e.Code)
	}
	return fmt.Sprintf("wfx: %s: %s (internal code %d)", e.Operation, message, e.InternalCode)
}

func responseError(operation string, code int32, message string, internalCode int32, internalMessage string) error {
	if code == 0 && internalCode == 0 {
		return nil
	}
	return &ServiceError{
		Operation:       operation,
		Code:            code,
		Message:         message,
		InternalCode:    internalCode,
		InternalMessage: internalMessage,
	}
}
