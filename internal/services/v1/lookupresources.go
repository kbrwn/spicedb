package v1

import (
	"math"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

// lookupResourcesResponseStream optionally holds back one response until another
// result is found, so the final response in an exhausted lookup loses its cursor.
// Calls to publish must be serialized, as they are by HandlingDispatchStream.
type lookupResourcesResponseStream struct {
	send      func(*v1.LookupResourcesResponse) error
	limit     uint32
	lookahead bool
	published uint64
	pending   *v1.LookupResourcesResponse
	sendErr   error
}

func (s *lookupResourcesResponseStream) publish(response *v1.LookupResourcesResponse) error {
	if s.sendErr != nil {
		return s.sendErr
	}
	if !s.lookahead {
		s.pending = response
		return s.sendPending()
	}
	if s.pending != nil {
		if err := s.sendPending(); err != nil {
			return err
		}
	}

	// The extra result proves that the last published cursor can be resumed. It
	// belongs to the next page and must not be sent in this one.
	if s.limit == 0 || s.published < uint64(s.limit) {
		s.pending = response
	}
	return nil
}

// run dispatches one extra result for limited lookups when lookahead is enabled.
// dispatch must resume from the most recently received dispatch cursor if invoked
// a second time.
func (s *lookupResourcesResponseStream) run(dispatch func(uint32) error) error {
	if !s.lookahead {
		return dispatch(s.limit)
	}

	dispatchLimit := s.limit
	if dispatchLimit > 0 && dispatchLimit < math.MaxUint32 {
		dispatchLimit++
	}

	if err := dispatch(dispatchLimit); err != nil {
		return err
	}

	// The largest possible limit cannot be incremented without becoming an
	// unlimited request (which also disables LR3 cursors). Probe separately only
	// if that page filled completely.
	if s.limit == math.MaxUint32 && s.pending != nil && s.published+1 == uint64(s.limit) {
		if err := dispatch(1); err != nil {
			return err
		}
	}

	if s.pending != nil {
		// Dispatch completed successfully without another visible result.
		s.pending.AfterResultCursor = nil
		return s.sendPending()
	}
	return nil
}

func (s *lookupResourcesResponseStream) sendPending() error {
	s.sendErr = s.send(s.pending)
	if s.sendErr != nil {
		return s.sendErr
	}
	s.published++
	s.pending = nil
	return nil
}
