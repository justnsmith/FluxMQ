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
	ErrNotLeader           = &BrokerError{Code: 6, Message: "not the leader for this partition"}
	ErrBrokerNotAvailable  = &BrokerError{Code: 8, Message: "broker not available"}
	ErrInvalidPartition    = &BrokerError{Code: 10, Message: "invalid partition"}
	ErrGroupNotFound       = &BrokerError{Code: 16, Message: "group not found"}
	ErrIllegalGeneration   = &BrokerError{Code: 22, Message: "illegal generation"}
	ErrUnknownMemberId     = &BrokerError{Code: 25, Message: "unknown member id"}
	ErrRebalanceInProgress = &BrokerError{Code: 27, Message: "rebalance in progress"}
	ErrTopicAlreadyExists  = &BrokerError{Code: 36, Message: "topic already exists"}
	ErrInvalidRequest      = &BrokerError{Code: 42, Message: "invalid request"}
	ErrFencedLeaderEpoch   = &BrokerError{Code: 74, Message: "fenced leader epoch"}
	ErrDuplicateSequence   = &BrokerError{Code: 46, Message: "duplicate sequence number"}
	ErrOutOfOrderSequence  = &BrokerError{Code: 47, Message: "out of order sequence number"}
	ErrUnknownProducerId   = &BrokerError{Code: 48, Message: "unknown producer id"}
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
	case 6:
		return ErrNotLeader
	case 8:
		return ErrBrokerNotAvailable
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
	case 46:
		return ErrDuplicateSequence
	case 47:
		return ErrOutOfOrderSequence
	case 48:
		return ErrUnknownProducerId
	case 74:
		return ErrFencedLeaderEpoch
	default:
		return &BrokerError{Code: code, Message: "unknown error"}
	}
}
