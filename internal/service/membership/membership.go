package membership

import (
	"context"
	"errors"
	"log/slog"

	"github.com/w-h-a/tally/internal/client/consensus"
	"github.com/w-h-a/tally/internal/client/discovery"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Service orchestrates Discovery and Consensus for cluster voter
// management. It runs a background routine that reacts to membership
// changes.
type Service struct {
	discovery discovery.Discovery
	consensus consensus.Consensus
	tracer    trace.Tracer
	stop      chan struct{}
	done      chan struct{}
}

func New(d discovery.Discovery, c consensus.Consensus) *Service {
	return &Service{
		discovery: d,
		consensus: c,
		tracer:    otel.Tracer("tally/internal/service/membership"),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// Start launches the background event loop that reads discovery events
// and updates consensus voter membership. The routine exits when the
// Events channel closes or Stop is called.
func (s *Service) Start() {
	go s.eventLoop()
}

// Stop signals the event loop to stop processing new events and waits
// for any in-flight handler to complete. Call this before leadership
// transfer to ensure no membership commits are in-flight.
func (s *Service) Stop(ctx context.Context) {
	close(s.stop)
	select {
	case <-s.done:
	case <-ctx.Done():
	}
}

// Close leaves the discovery cluster. It does NOT close Consensus.
// DistributedLog owns that lifecycle.
func (s *Service) Close(ctx context.Context) error {
	return s.discovery.Leave(ctx)
}

func (s *Service) eventLoop() {
	defer close(s.done)

	for {
		select {
		case <-s.stop:
			return
		case event, ok := <-s.discovery.Events():
			if !ok {
				return
			}
			switch event.EventType {
			case discovery.Join:
				s.handleJoin(event)
			case discovery.Leave:
				s.handleLeave(event)
			}
		}
	}
}

func (s *Service) handleJoin(event discovery.MemberEvent) {
	ctx, span := s.tracer.Start(context.Background(), "membership.HandleJoin", trace.WithAttributes(
		attribute.String("membership.node_id", event.ID),
		attribute.String("membership.node_addr", event.Addr),
		attribute.String("membership.event_type", "join"),
	))
	defer span.End()

	if err := s.consensus.AddVoter(ctx, event.ID, event.Addr); err != nil {
		if !errors.Is(err, consensus.ErrNotLeader) {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			slog.WarnContext(ctx, "membership: add voter failed",
				"node_id", event.ID,
				"node_addr", event.Addr,
				"error", err,
			)
		}
		return
	}
}

func (s *Service) handleLeave(event discovery.MemberEvent) {
	ctx, span := s.tracer.Start(context.Background(), "membership.HandleLeave", trace.WithAttributes(
		attribute.String("membership.node_id", event.ID),
		attribute.String("membership.node_addr", event.Addr),
		attribute.String("membership.event_type", "leave"),
	))
	defer span.End()

	if err := s.consensus.RemoveServer(ctx, event.ID); err != nil {
		if !errors.Is(err, consensus.ErrNotLeader) {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			slog.WarnContext(ctx, "membership: remove server failed",
				"node_id", event.ID,
				"node_addr", event.Addr,
				"error", err,
			)
		}
		return
	}
}
