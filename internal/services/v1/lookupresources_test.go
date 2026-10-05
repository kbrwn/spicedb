package v1

import (
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

func TestLookupResourcesResponseStreamWithoutLookahead(t *testing.T) {
	for _, limit := range []uint32{0, 1, math.MaxUint32} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			var sent []*v1.LookupResourcesResponse
			stream := &lookupResourcesResponseStream{
				send: func(response *v1.LookupResourcesResponse) error {
					sent = append(sent, response)
					return nil
				},
				limit: limit,
			}
			var dispatchCalls int
			err := stream.run(func(dispatchLimit uint32) error {
				dispatchCalls++
				require.Equal(t, limit, dispatchLimit, "disabled lookahead must not request extra results")
				require.NoError(t, stream.publish(&v1.LookupResourcesResponse{
					ResourceObjectId: "last", AfterResultCursor: &v1.Cursor{Token: "last"},
				}))
				require.Len(t, sent, 1, "disabled lookahead must send each response immediately")
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, 1, dispatchCalls)
			require.Equal(t, "last", sent[0].GetAfterResultCursor().GetToken(), "disabled lookahead must retain the final cursor")
			require.Equal(t, uint64(1), stream.published)
		})
	}
}

func TestLookupResourcesResponseStreamDispatchError(t *testing.T) {
	dispatchErr := errors.New("lookup failed")
	var sent []*v1.LookupResourcesResponse
	stream := &lookupResourcesResponseStream{
		send: func(response *v1.LookupResourcesResponse) error {
			sent = append(sent, response)
			return nil
		},
		limit:     2,
		lookahead: true,
	}

	err := stream.run(func(limit uint32) error {
		require.Equal(t, uint32(3), limit)
		require.NoError(t, stream.publish(&v1.LookupResourcesResponse{
			ResourceObjectId: "first", AfterResultCursor: &v1.Cursor{Token: "first"},
		}))
		require.NoError(t, stream.publish(&v1.LookupResourcesResponse{
			ResourceObjectId: "second", AfterResultCursor: &v1.Cursor{Token: "second"},
		}))
		return dispatchErr
	})

	require.ErrorIs(t, err, dispatchErr)
	require.Len(t, sent, 1)
	require.NotNil(t, sent[0].AfterResultCursor, "failed lookups must not signal exhaustion")
	require.Equal(t, uint64(1), stream.published)
}

func TestLookupResourcesResponseStreamSendError(t *testing.T) {
	for _, failOnFinalResponse := range []bool{false, true} {
		name := "intermediate response"
		if failOnFinalResponse {
			name = "final response"
		}
		t.Run(name, func(t *testing.T) {
			sendErr := errors.New("send failed")
			sendAttempts := 0
			stream := &lookupResourcesResponseStream{
				send: func(*v1.LookupResourcesResponse) error {
					sendAttempts++
					return sendErr
				},
				limit:     1,
				lookahead: true,
			}
			err := stream.run(func(uint32) error {
				if err := stream.publish(&v1.LookupResourcesResponse{ResourceObjectId: "first"}); err != nil {
					return err
				}
				if failOnFinalResponse {
					return nil
				}
				return stream.publish(&v1.LookupResourcesResponse{ResourceObjectId: "second"})
			})
			require.ErrorIs(t, err, sendErr)
			require.Zero(t, stream.published)
			// Queued dispatch publishers must not retry a failed Send.
			require.ErrorIs(t, stream.publish(&v1.LookupResourcesResponse{ResourceObjectId: "third"}), sendErr)
			require.Equal(t, 1, sendAttempts)
		})
	}
}

func TestLookupResourcesResponseStreamMaximumLimit(t *testing.T) {
	for _, tc := range []struct {
		name         string
		fullPage     bool
		hasNext      bool
		dispatchErr  error
		wantDispatch []uint32
	}{
		{"short page", false, false, nil, []uint32{math.MaxUint32}},
		{"full final page", true, false, nil, []uint32{math.MaxUint32, 1}},
		{"full page with more", true, true, nil, []uint32{math.MaxUint32, 1}},
		{"probe error", true, false, errors.New("probe failed"), []uint32{math.MaxUint32, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sent []*v1.LookupResourcesResponse
			stream := &lookupResourcesResponseStream{
				send: func(response *v1.LookupResourcesResponse) error {
					sent = append(sent, response)
					return nil
				},
				limit:     math.MaxUint32,
				lookahead: true,
			}
			var dispatchLimits []uint32
			err := stream.run(func(limit uint32) error {
				dispatchLimits = append(dispatchLimits, limit)
				if len(dispatchLimits) == 1 {
					// Simulate the already streamed part of this very large page.
					if tc.fullPage {
						stream.published = math.MaxUint32 - 1
					}
					return stream.publish(&v1.LookupResourcesResponse{
						ResourceObjectId: "last", AfterResultCursor: &v1.Cursor{Token: "last"},
					})
				}
				if tc.hasNext {
					return stream.publish(&v1.LookupResourcesResponse{ResourceObjectId: "next"})
				}
				return tc.dispatchErr
			})
			require.Equal(t, tc.wantDispatch, dispatchLimits)
			if tc.dispatchErr != nil {
				require.ErrorIs(t, err, tc.dispatchErr)
				require.Empty(t, sent)
				return
			}
			require.NoError(t, err)
			require.Len(t, sent, 1)
			require.Equal(t, "last", sent[0].ResourceObjectId)
			if tc.hasNext {
				require.Equal(t, "last", sent[0].GetAfterResultCursor().GetToken())
			} else {
				require.Nil(t, sent[0].AfterResultCursor)
			}
		})
	}
}
