package v1_test

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

func TestLookupResourcesCursorOnlyWhenMoreResults(t *testing.T) {
	for _, useLR3 := range []bool{false, true} {
		t.Run(fmt.Sprintf("LR3=%t", useLR3), func(t *testing.T) {
			for _, tc := range []struct {
				name      string
				count     int
				limit     uint32
				pageSizes []int
			}{
				{"empty", 0, 1, []int{0}},
				{"singleton", 1, 1, []int{1}},
				{"less than limit", 2, 3, []int{2}},
				{"exactly limit", 2, 2, []int{2}},
				{"more than limit", 3, 2, []int{2, 1}},
				{"exact final page", 4, 2, []int{2, 2}},
				{"single result pages", 4, 1, []int{1, 1, 1, 1}},
				{"maximum allowed limit", 1001, 1000, []int{1000, 1}},
				{"unlimited empty", 0, 0, []int{0}},
				{"unlimited singleton", 1, 0, []int{1}},
				{"unlimited multiple", 4, 0, []int{4}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ids := make([]string, 0, tc.count)
					relationships := make([]tuple.Relationship, 0, tc.count)
					for index := range tc.count {
						id := fmt.Sprintf("document%04d", index)
						ids = append(ids, id)
						relationships = append(relationships, tuple.MustParse("document:"+id+"#viewer@user:tom"))
					}

					client, request := newLookupResourcesCursorTestClient(t, useLR3, true, `
						definition user {}
						definition document {
							relation viewer: user
							permission view = viewer
						}
					`, relationships)
					request.OptionalLimit = tc.limit

					var receivedIDs []string
					for pageIndex, pageSize := range tc.pageSizes {
						if pageIndex > 0 {
							require.NotNil(t, request.OptionalCursor, "a nonfinal page must have a continuation cursor")
						}
						responses := lookupResourcesCursorTestPage(t, client, request)
						require.Len(t, responses, pageSize)
						for _, response := range responses {
							receivedIDs = append(receivedIDs, response.ResourceObjectId)
							require.Equal(t, v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION, response.Permissionship)
							require.NotNil(t, response.LookedUpAt)
							if useLR3 && tc.limit == 0 {
								// LR3 disables cursors for unlimited lookups.
								// Preserve that behavior for every response.
								require.Nil(t, response.AfterResultCursor)
							} else if len(receivedIDs) < tc.count {
								require.NotEmpty(t, response.GetAfterResultCursor().GetToken(), "every nonterminal result must remain resumable")
							} else {
								require.Nil(t, response.AfterResultCursor, "the last available result must not advertise an empty next page")
							}
							request.OptionalCursor = response.AfterResultCursor
						}
					}
					require.ElementsMatch(t, ids, receivedIDs)
				})
			}
		})
	}
}

func TestLookupResourcesCursorWithUnlimitedDuplicates(t *testing.T) {
	for _, useLR3 := range []bool{false, true} {
		t.Run(fmt.Sprintf("LR3=%t", useLR3), func(t *testing.T) {
			client, request := newLookupResourcesCursorTestClient(t, useLR3, true, `
				definition user {}
				definition document {
					relation viewer: user
					relation editor: user
					permission view = viewer + editor
				}
			`, []tuple.Relationship{
				tuple.MustParse("document:a#viewer@user:tom"),
				tuple.MustParse("document:b#viewer@user:tom"),
				tuple.MustParse("document:b#editor@user:tom"),
			})

			responses := lookupResourcesCursorTestPage(t, client, request)
			require.Len(t, responses, 2)
			require.ElementsMatch(t, []string{"a", "b"}, []string{responses[0].ResourceObjectId, responses[1].ResourceObjectId})
			require.Nil(t, responses[1].AfterResultCursor, "filtered duplicate results must not cause a continuation cursor")
		})
	}
}

func TestLookupResourcesCursorResumesAfterReturnedResult(t *testing.T) {
	for _, useLR3 := range []bool{false, true} {
		t.Run(fmt.Sprintf("LR3=%t", useLR3), func(t *testing.T) {
			client, request := newLookupResourcesCursorTestClient(t, useLR3, true, `
				definition user {}
				definition document {
					relation viewer: user
					permission view = viewer
				}
			`, []tuple.Relationship{
				tuple.MustParse("document:a#viewer@user:tom"),
				tuple.MustParse("document:b#viewer@user:tom"),
				tuple.MustParse("document:c#viewer@user:tom"),
				tuple.MustParse("document:d#viewer@user:tom"),
			})
			request.OptionalLimit = 3
			firstPage := lookupResourcesCursorTestPage(t, client, request)
			require.Len(t, firstPage, 3)

			// Resuming any returned result must include the lookahead result; the
			// cursor must describe the returned result, not the lookahead position.
			for index, response := range firstPage {
				require.NotEmpty(t, response.GetAfterResultCursor().GetToken())
				request.OptionalCursor = response.AfterResultCursor
				remaining := lookupResourcesCursorTestPage(t, client, request)
				require.Len(t, remaining, 3-index)
				var remainingIDs []string
				for _, result := range remaining {
					remainingIDs = append(remainingIDs, result.ResourceObjectId)
				}
				require.Equal(t, []string{"b", "c", "d"}[index:], remainingIDs)
				require.Nil(t, remaining[len(remaining)-1].AfterResultCursor)
			}
		})
	}
}

func TestLookupResourcesCursorWithCaveats(t *testing.T) {
	for _, useLR3 := range []bool{false, true} {
		t.Run(fmt.Sprintf("LR3=%t", useLR3), func(t *testing.T) {
			client, request := newLookupResourcesCursorTestClient(t, useLR3, true, `
				definition user {}
				caveat allowed(enabled bool) { enabled }
				definition document {
					relation viewer: user | user with allowed
					permission view = viewer
				}
			`, []tuple.Relationship{
				tuple.MustParse("document:a#viewer@user:tom"),
				tuple.MustWithCaveat(tuple.MustParse("document:b#viewer@user:tom"), "allowed"),
			})
			request.OptionalLimit = 1

			firstPage := lookupResourcesCursorTestPage(t, client, request)
			require.Len(t, firstPage, 1)
			require.Equal(t, "a", firstPage[0].ResourceObjectId)
			require.NotEmpty(t, firstPage[0].GetAfterResultCursor().GetToken(), "a conditional result counts as a next result")

			request.OptionalCursor = firstPage[0].AfterResultCursor
			lastPage := lookupResourcesCursorTestPage(t, client, request)
			require.Len(t, lastPage, 1)
			require.Equal(t, "b", lastPage[0].ResourceObjectId)
			require.Equal(t, v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION, lastPage[0].Permissionship)
			require.Equal(t, []string{"enabled"}, lastPage[0].PartialCaveatInfo.MissingRequiredContext)
			require.Nil(t, lastPage[0].AfterResultCursor)

			request.OptionalCursor = nil
			var err error
			request.Context, err = structpb.NewStruct(map[string]any{"enabled": false})
			require.NoError(t, err)
			responses := lookupResourcesCursorTestPage(t, client, request)
			require.Len(t, responses, 1)
			require.Equal(t, "a", responses[0].ResourceObjectId)
			require.Nil(t, responses[0].AfterResultCursor, "a denied caveat must not cause a continuation cursor")
		})
	}
}

func TestLookupResourcesCursorWithDeniedTrailingCandidates(t *testing.T) {
	for _, useLR3 := range []bool{false, true} {
		t.Run(fmt.Sprintf("LR3=%t", useLR3), func(t *testing.T) {
			client, request := newLookupResourcesCursorTestClient(t, useLR3, true, `
				definition user {}
				definition document {
					relation viewer: user
					relation banned: user
					permission view = viewer - banned
				}
			`, []tuple.Relationship{
				tuple.MustParse("document:a#viewer@user:tom"),
				tuple.MustParse("document:b#viewer@user:tom"),
				tuple.MustParse("document:c#viewer@user:tom"),
				tuple.MustParse("document:b#banned@user:tom"),
				tuple.MustParse("document:c#banned@user:tom"),
			})
			request.OptionalLimit = 1
			responses := lookupResourcesCursorTestPage(t, client, request)
			require.Len(t, responses, 1)
			require.Equal(t, "a", responses[0].ResourceObjectId)
			require.Nil(t, responses[0].AfterResultCursor, "remaining candidates without permission must not cause a continuation cursor")
		})
	}
}

func TestLookupResourcesCursorLookaheadDisabled(t *testing.T) {
	require.False(t, testserver.DefaultTestServerConfig.EnableLookupResourcesCursorLookahead)
	for _, useLR3 := range []bool{false, true} {
		t.Run(fmt.Sprintf("LR3=%t", useLR3), func(t *testing.T) {
			for _, limit := range []uint32{0, 1, 2, 3} {
				t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
					client, request := newLookupResourcesCursorTestClient(t, useLR3, false, `
						definition user {}
						definition document {
							relation viewer: user
							permission view = viewer
						}
					`, []tuple.Relationship{
						tuple.MustParse("document:a#viewer@user:tom"),
						tuple.MustParse("document:b#viewer@user:tom"),
					})
					request.OptionalLimit = limit
					var receivedIDs []string
					for range 3 {
						responses := lookupResourcesCursorTestPage(t, client, request)
						if len(responses) == 0 {
							break
						}
						if limit > 0 {
							require.LessOrEqual(t, len(responses), int(limit))
						}
						for _, response := range responses {
							receivedIDs = append(receivedIDs, response.ResourceObjectId)
							if useLR3 && limit == 0 {
								require.Nil(t, response.AfterResultCursor, "unlimited LR3 lookups never include cursors")
							} else {
								require.NotEmpty(t, response.GetAfterResultCursor().GetToken(), "without lookahead, even the final result keeps its cursor")
							}
							request.OptionalCursor = response.AfterResultCursor
						}
						if len(receivedIDs) == 2 {
							break
						}
					}
					require.Equal(t, []string{"a", "b"}, receivedIDs)
					if !useLR3 || limit > 0 {
						require.Empty(t, lookupResourcesCursorTestPage(t, client, request), "the final cursor retains the legacy empty-next-page behavior")
					}
				})
			}
		})
	}
}

func newLookupResourcesCursorTestClient(t *testing.T, useLR3, enableCursorLookahead bool, schema string, relationships []tuple.Relationship) (v1.PermissionsServiceClient, *v1.LookupResourcesRequest) {
	t.Helper()
	config := testserver.DefaultTestServerConfig
	config.EnableExperimentalLookupResources3 = useLR3
	config.EnableLookupResourcesCursorLookahead = enableCursorLookahead
	conn, _, revision := testserver.NewTestServerWithConfig(t, 0, memdb.DisableGC, true, config,
		func(t testing.TB, ds datastore.Datastore) (datastore.Datastore, datastore.Revision) {
			return tf.DatastoreFromSchemaAndTestRelationships(t, ds, schema, relationships)
		})
	return v1.NewPermissionsServiceClient(conn), &v1.LookupResourcesRequest{
		ResourceObjectType: "document",
		Permission:         "view",
		Subject:            sub("user", "tom", ""),
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevisionForTesting(revision, datalayer.NoSchemaHashInLegacyZedToken),
			},
		},
	}
}

func lookupResourcesCursorTestPage(t *testing.T, client v1.PermissionsServiceClient, request *v1.LookupResourcesRequest) []*v1.LookupResourcesResponse {
	t.Helper()
	stream, err := client.LookupResources(t.Context(), request)
	require.NoError(t, err)
	var responses []*v1.LookupResourcesResponse
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return responses
		}
		require.NoError(t, err)
		responses = append(responses, response)
	}
}
