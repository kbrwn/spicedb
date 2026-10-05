package tables

import (
	"context"
	"io"
	"testing"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/v1"
)

func TestLookupResourcesFetchStopsAtFinalCursor(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pages [][]*v1.LookupResourcesResponse
	}{
		{
			name:  "empty result",
			pages: [][]*v1.LookupResourcesResponse{nil},
		},
		{
			name: "single page",
			pages: [][]*v1.LookupResourcesResponse{
				{{ResourceObjectId: "first"}},
			},
		},
		{
			name: "multiple pages",
			pages: [][]*v1.LookupResourcesResponse{
				{{ResourceObjectId: "first", AfterResultCursor: &v1.Cursor{Token: "next"}}},
				{{ResourceObjectId: "second"}},
			},
		},
		{
			name: "empty final page from older server",
			pages: [][]*v1.LookupResourcesResponse{
				{{ResourceObjectId: "first", AfterResultCursor: &v1.Cursor{Token: "next"}}},
				nil,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler, err := buildLookupResourcesHandler(fieldMap[permissionsField]{
				permissionsResourceTypeField: {value: "document"},
				permissionsPermissionField:   {value: "view"},
				permissionsSubjectTypeField:  {value: "user"},
				permissionsSubjectIDField:    {value: "tom"},
			})
			require.NoError(t, err)

			lookupClient := &lookupResourcesTestClient{pages: tc.pages}
			client := &authzed.Client{PermissionsServiceClient: lookupClient}
			statement := &SelectStatement{columnNames: []string{"resource_id"}}
			var rowsCursor RowsCursor
			var expectedCursor *v1.Cursor
			for pageIndex, page := range tc.pages {
				writer := &lookupResourcesTestWriter{}
				rowsCursor, err = handler(t.Context(), client, nil, 1, writer, rowsCursor, statement)
				require.NoError(t, err)
				require.Len(t, lookupClient.requests, pageIndex+1)
				require.Equal(t, expectedCursor, lookupClient.requests[pageIndex].OptionalCursor)
				require.Equal(t, uint32(1), lookupClient.requests[pageIndex].OptionalLimit)
				require.Equal(t, "SELECT", writer.completed)
				require.Len(t, writer.rows, len(page))
				for index, response := range page {
					require.Equal(t, []any{response.ResourceObjectId}, writer.rows[index])
					expectedCursor = response.AfterResultCursor
				}
			}

			// PostgreSQL can fetch again after exhaustion; keep returning zero rows
			// without sending a nil API cursor and restarting the lookup.
			for range 2 {
				writer := &lookupResourcesTestWriter{}
				rowsCursor, err = handler(t.Context(), client, nil, 1, writer, rowsCursor, statement)
				require.NoError(t, err)
				require.Empty(t, writer.rows)
				require.Equal(t, "SELECT", writer.completed)
				require.Len(t, lookupClient.requests, len(tc.pages))
			}
		})
	}
}

type lookupResourcesTestClient struct {
	v1.PermissionsServiceClient
	pages    [][]*v1.LookupResourcesResponse
	requests []*v1.LookupResourcesRequest
}

func (c *lookupResourcesTestClient) LookupResources(_ context.Context, request *v1.LookupResourcesRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[v1.LookupResourcesResponse], error) {
	c.requests = append(c.requests, request)
	page := c.pages[0]
	c.pages = c.pages[1:]
	return &lookupResourcesTestStream{responses: page}, nil
}

type lookupResourcesTestStream struct {
	grpc.ClientStream
	responses []*v1.LookupResourcesResponse
}

func (s *lookupResourcesTestStream) Recv() (*v1.LookupResourcesResponse, error) {
	if len(s.responses) == 0 {
		return nil, io.EOF
	}
	response := s.responses[0]
	s.responses = s.responses[1:]
	return response, nil
}

func (s *lookupResourcesTestStream) Trailer() metadata.MD {
	return metadata.Pairs("io.spicedb.respmeta.dispatchedoperationscount", "1", "io.spicedb.respmeta.cachedoperationscount", "0")
}

type lookupResourcesTestWriter struct {
	wire.DataWriter
	rows      [][]any
	completed string
}

func (w *lookupResourcesTestWriter) Row(values []any) error {
	w.rows = append(w.rows, values)
	return nil
}

func (w *lookupResourcesTestWriter) Written() uint32 {
	return uint32(len(w.rows)) //nolint:gosec // Test pages contain at most one row.
}

func (w *lookupResourcesTestWriter) Complete(description string) error {
	w.completed = description
	return nil
}
