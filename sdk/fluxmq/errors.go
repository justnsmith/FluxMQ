package fluxmq

import "fmt"

// BrokerError represents an error code returned by the broker.
type BrokerError struct {
	Code    int16
	Message string
}

func (e *BrokerError) Error() string {
	return fmt.Sprintf("broker error %d: %s", e.Code, e.Message)
}

// Known broker error codes matching errors.h
var (
	ErrOffsetOutOfRange    = &BrokerError{Code: 1, Message: "offset out of range"}
	ErrUnknownTopic        = &BrokerError{Code: 3, Message: "unknown topic"}
	ErrGroupNotFound       = &BrokerError{Code: 16, Message: "group not found"}
	ErrIllegalGeneration   = &BrokerError{Code: 22, Message: "illegal generation"}
	ErrUnknownMemberId     = &BrokerError{Code: 25, Message: "unknown member id"}
	ErrRebalanceInProgress = &BrokerError{Code: 27, Message: "rebalance in progress"}
	ErrTopicAlreadyExists  = &BrokerError{Code: 36, Message: "topic already exists"}
	ErrInvalidPartition    = &BrokerError{Code: 10, Message: "invalid partition"}
	ErrInvalidRequest      = &BrokerError{Code: 42, Message: "invalid request"}
)

// codeToError maps an int16 error code to the corresponding typed error.
// Returns nil for code 0 (kOk).
func codeToError(code int16) error {
	switch code {
	case 0:
		return nil
	case 1:
		return ErrOffsetOutOfRange
	case 3:
		return ErrUnknownTopic
	case 10:
		return ErrInvalidPartition
	case 16:
		return ErrGroupNotFound
	case 22:
		return ErrIllegalGeneration
	case 25:
		return ErrUnknownMemberId
	case 27:
		return ErrRebalanceInProgress
	case 36:
		return ErrTopicAlreadyExists
	case 42:
		return ErrInvalidRequest
	default:
		return &BrokerError{Code: code, Message: "unknown error"}
	}
}
