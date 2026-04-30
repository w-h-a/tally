package serf

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"

	hserf "github.com/hashicorp/serf/serf"
	discovery "github.com/w-h-a/tally/internal/client/discovery"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// serfDiscovery wraps hashicorp/serf behind the discovery.Discovery port
// interface. It translates Serf membership events into port-level
// MemberEvents on a buffered channel for the service layer to consume.
type serfDiscovery struct {
	serf   *hserf.Serf
	events chan discovery.MemberEvent
	nodeID string
	tracer trace.Tracer
}

// NewDiscovery creates a Serf agent and returns it as a discovery.Discovery.
// Setup:
//  1. Parse BindAddr into host and port for Serf's memberlist config.
//  2. Configure Serf with NodeName, Tags, and an EventCh for receiving
//     membership events.
//  3. Start the Serf agent.
//  4. Launch the eventHandler goroutine to translate Serf events into
//     port-level MemberEvents.
//  5. If StartJoinAddrs are provided, attempt to join existing cluster
//     nodes. On failure, log and continue. The node starts as a
//     single-member cluster and Serf will retry via gossip.
func NewDiscovery(opts ...discovery.Option) (discovery.Discovery, error) {
	options := discovery.NewOptions(opts...)

	host, portStr, err := net.SplitHostPort(options.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("discovery/serf: parse bind addr: %w", err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("discovery/serf: parse bind port: %w", err)
	}

	events := make(chan discovery.MemberEvent, 16)
	serfEvents := make(chan hserf.Event, 16)

	config := hserf.DefaultConfig()
	config.Init()
	config.NodeName = options.NodeName
	config.MemberlistConfig.BindAddr = host
	config.MemberlistConfig.BindPort = port
	config.Tags = options.Tags
	config.EventCh = serfEvents

	s, err := hserf.Create(config)
	if err != nil {
		return nil, fmt.Errorf("discovery/serf: create: %w", err)
	}

	d := &serfDiscovery{
		serf:   s,
		events: events,
		nodeID: options.NodeName,
		tracer: otel.Tracer("tally/internal/client/discovery/serf"),
	}

	go d.eventHandler(serfEvents)

	if len(options.StartJoinAddrs) > 0 {
		_, err := s.Join(options.StartJoinAddrs, false)
		if err != nil {
			slog.Warn("discovery/serf: join failed, continuing as single node",
				"addrs", options.StartJoinAddrs,
				"error", err,
			)
		}
	}

	return d, nil
}

// Join attempts to join an existing cluster via the given addresses.
func (d *serfDiscovery) Join(ctx context.Context, addrs []string) error {
	_, span := d.tracer.Start(ctx, "discovery.Join", trace.WithAttributes(
		attribute.StringSlice("discovery.join_addrs", addrs),
	))
	defer span.End()

	_, err := d.serf.Join(addrs, false)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}

// Leave gracefully leaves the cluster and shuts down the Serf agent.
// Leave broadcasts a leave intent to peers so they receive a
// MemberLeave event (rather than MemberFailed). Shutdown stops the
// agent and closes ShutdownCh, which causes the eventHandler routine
// to exit and close the Events channel.
func (d *serfDiscovery) Leave(ctx context.Context) error {
	_, span := d.tracer.Start(ctx, "discovery.Leave", trace.WithAttributes(
		attribute.String("discovery.node_id", d.nodeID),
	))
	defer span.End()

	if err := d.serf.Leave(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	if err := d.serf.Shutdown(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}

// Members returns all currently alive cluster members, including self.
// Each member's Addr is the RPC address extracted from Serf tags.
func (d *serfDiscovery) Members(ctx context.Context) []discovery.Member {
	_, span := d.tracer.Start(ctx, "discovery.Members")
	defer span.End()

	result := []discovery.Member{}

	for _, m := range d.serf.Members() {
		if m.Status != hserf.StatusAlive {
			continue
		}

		result = append(result, discovery.Member{
			ID:   m.Name,
			Addr: m.Tags["rpc_addr"], // client facing
		})
	}

	span.SetAttributes(attribute.Int("discovery.member_count", len(result)))

	return result
}

// Events returns a receive-only channel of membership changes.
// The channel is closed when the Serf agent shuts down.
func (d *serfDiscovery) Events() <-chan discovery.MemberEvent {
	return d.events
}

// eventHandler reads raw Serf events and translates them into
// MemberEvents. The routine exits when ShutdownCh closes and defers
// closing the events channel.
func (d *serfDiscovery) eventHandler(serfEvents chan hserf.Event) {
	defer close(d.events)

	for {
		select {
		case <-d.serf.ShutdownCh():
			return
		case e := <-serfEvents:
			switch e.EventType() {
			case hserf.EventMemberJoin:
				d.handleMembers(discovery.Join, e.(hserf.MemberEvent).Members)
			case hserf.EventMemberLeave, hserf.EventMemberFailed:
				d.handleMembers(discovery.Leave, e.(hserf.MemberEvent).Members)
			}
		}
	}
}

// handleMembers sends a MemberEvent for each member in the list,
// skipping self. The select on ShutdownCh prevents blocking if the
// events buffer is full during shutdown.
func (d *serfDiscovery) handleMembers(eventType discovery.EventType, members []hserf.Member) {
	for _, m := range members {
		if m.Name == d.nodeID {
			continue
		}

		select {
		case d.events <- discovery.MemberEvent{
			EventType: eventType,
			ID:        m.Name,
			Addr:      m.Tags["raft_addr"], // what raft needs
		}:
		case <-d.serf.ShutdownCh():
			return
		}
	}
}
