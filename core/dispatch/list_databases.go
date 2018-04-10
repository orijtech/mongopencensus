package dispatch

import (
	"context"

	"github.com/mongodb/mongo-go-driver/core/command"
	"github.com/mongodb/mongo-go-driver/core/result"
	"github.com/mongodb/mongo-go-driver/core/topology"
	"github.com/mongodb/mongo-go-driver/internal/trace"
)

// ListDatabases handles the full cycle dispatch and execution of a listDatabases command against the provided
// topology.
func ListDatabases(
	ctx context.Context,
	cmd command.ListDatabases,
	topo *topology.Topology,
	selector topology.ServerSelector,
) (result.ListDatabases, error) {

	ctx, span := trace.SpanFromFunctionCaller(ctx)
	defer span.End()

	ss, err := topo.SelectServer(ctx, selector)
	if err != nil {
		return result.ListDatabases{}, err
	}

	conn, err := ss.Connection(ctx)
	if err != nil {
		return result.ListDatabases{}, err
	}
	defer conn.Close()

	return cmd.RoundTrip(ctx, ss.Description(), conn)
}