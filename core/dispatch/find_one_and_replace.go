package dispatch

import (
	"context"

	"github.com/mongodb/mongo-go-driver/core/command"
	"github.com/mongodb/mongo-go-driver/core/options"
	"github.com/mongodb/mongo-go-driver/core/result"
	"github.com/mongodb/mongo-go-driver/core/topology"
	"github.com/mongodb/mongo-go-driver/core/writeconcern"
	"github.com/mongodb/mongo-go-driver/internal/trace"
)

// FindOneAndReplace handles the full cycle dispatch and execution of a FindOneAndReplace command against the provided
// topology.
func FindOneAndReplace(
	ctx context.Context,
	cmd command.FindOneAndReplace,
	topo *topology.Topology,
	selector topology.ServerSelector,
	wc *writeconcern.WriteConcern,
) (result.FindAndModify, error) {

	ctx, span := trace.SpanFromFunctionCaller(ctx)
	defer span.End()

	ss, err := topo.SelectServer(ctx, selector)
	if err != nil {
		return result.FindAndModify{}, err
	}

	if wc != nil {
		_, wcSpan := trace.SpanWithName(ctx, "writeConcernOption")
		opt, err := writeConcernOption(wc)
		wcSpan.End()
		if err != nil {
			return result.FindAndModify{}, err
		}
		cmd.Opts = append(cmd.Opts, opt)
	}

	// NOTE: We iterate through the options because the user may have provided
	// an option explicitly and that needs to override the provided write concern.
	// We put this here because it would complicate the methods that call this to
	// parse out the option.
	acknowledged := true
	for _, opt := range cmd.Opts {
		wc, ok := opt.(options.OptWriteConcern)
		if !ok {
			continue
		}
		acknowledged = wc.Acknowledged
		break
	}

	desc := ss.Description()
	conn, err := ss.Connection(ctx)
	if err != nil {
		return result.FindAndModify{}, err
	}

	if !acknowledged {
		go func() {
			defer func() {
				_ = recover()
			}()
			defer conn.Close()
			_, _ = cmd.RoundTrip(ctx, desc, conn)
		}()
		return result.FindAndModify{}, ErrUnacknowledgedWrite
	}

	defer conn.Close()
	return cmd.RoundTrip(ctx, desc, conn)
}