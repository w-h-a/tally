package distributedlog

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	commitlog "github.com/w-h-a/tally/internal/client/commit_log"
	"github.com/w-h-a/tally/internal/client/consensus"
	api "github.com/w-h-a/tally/proto/log/v1"
	"google.golang.org/protobuf/proto"
)

const (
	AppendRequestType uint8 = 0
)

var (
	ErrOffsetOutOfRange = errors.New("offset out of range")
)

type Service struct {
	commitlog commitlog.CommitLog
	consensus consensus.Consensus
	nodeID    string
	rpcAddr   string
}

func New(commitLog commitlog.CommitLog, nodeID string, rpcAddr string) *Service {
	return &Service{
		commitlog: commitLog,
		nodeID:    nodeID,
		rpcAddr:   rpcAddr,
	}
}

// SetConsensus injects the Consensus dependency after construction.
// Two-phase construction breaks the circular dependency: DistributedLog
// needs Consensus to propose, Consensus needs DistributedLog callbacks
// to apply.
func (s *Service) SetConsensus(c consensus.Consensus) {
	s.consensus = c
}

func (s *Service) ApplyFn() consensus.ApplyFn {
	return s.apply
}

func (s *Service) SnapshotFn() consensus.SnapshotFn {
	return s.snapshot
}

func (s *Service) RestoreFn() consensus.RestoreFn {
	return s.restore
}

// Append proposes a record for replicated write via the consensus layer.
// The record is serialized with a leading request type byte so we can
// distringuish mutation types (e.g., append vs offset commit).
// consensus.Apply blocks until a quorum commits the entry, then we
// append it to the local CommitLog and return the assigned offset.
func (s *Service) Append(ctx context.Context, rec *api.Record) (uint64, error) {
	b, err := proto.Marshal(rec)
	if err != nil {
		return 0, fmt.Errorf("marshal record: %w", err)
	}

	data := make([]byte, 1+len(b))
	data[0] = AppendRequestType
	copy(data[1:], b)

	offset, err := s.consensus.Apply(ctx, data)
	if err != nil {
		if errors.Is(err, consensus.ErrNotLeader) {
			return 0, &NotLeaderError{LeaderAddr: s.consensus.Leader(ctx)}
		}
		return 0, err
	}

	return offset, nil
}

func (s *Service) Read(ctx context.Context, offset uint64) (*api.Record, error) {
	rec, err := s.commitlog.Read(ctx, offset)
	if err != nil {
		if errors.Is(err, commitlog.ErrOffsetOutOfRange) {
			return nil, ErrOffsetOutOfRange
		}
		return nil, err
	}
	return rec, nil
}

func (s *Service) GetServers(ctx context.Context) ([]*api.Server, error) {
	servers, err := s.consensus.GetServers(ctx)
	if err != nil {
		return nil, err
	}

	for _, srv := range servers {
		if srv.Id == s.nodeID {
			srv.RpcAddr = s.rpcAddr
		}
	}

	return servers, nil
}

func (s *Service) WaitForLeader(ctx context.Context) error {
	return s.consensus.WaitForLeader(ctx)
}

func (s *Service) Close(ctx context.Context) error {
	consensusErr := s.consensus.Close(ctx)
	commitlogErr := s.commitlog.Close(ctx)
	return errors.Join(consensusErr, commitlogErr)
}

// apply decodes the request type prefix and dispatches
// to the appropriate CommitLog method.
func (s *Service) apply(data []byte) (uint64, error) {
	if len(data) == 0 {
		return 0, fmt.Errorf("empty apply data")
	}

	switch data[0] {
	case AppendRequestType:
		var rec api.Record
		if err := proto.Unmarshal(data[1:], &rec); err != nil {
			return 0, fmt.Errorf("unmarshal record: %w", err)
		}
		return s.commitlog.Append(context.Background(), &rec)
	default:
		return 0, fmt.Errorf("unknown request type: %d", data[0])
	}
}

// snapshot reads all records from the CommitLog and writes them as
// length-prefixed protobuf bytes.
func (s *Service) snapshot() (io.ReadCloser, error) {
	ctx := context.Background()

	lowest, err := s.commitlog.LowestOffset(ctx)
	if err != nil {
		return nil, err
	}

	highest, err := s.commitlog.HighestOffset(ctx)
	if err != nil {
		return nil, err
	}

	buf := bytes.Buffer{}

	for offset := lowest; offset <= highest; offset++ {
		rec, err := s.commitlog.Read(ctx, offset)
		if err != nil {
			return nil, err
		}

		b, err := proto.Marshal(rec)
		if err != nil {
			return nil, err
		}

		if err := binary.Write(&buf, binary.BigEndian, uint64(len(b))); err != nil {
			return nil, err
		}

		if _, err := buf.Write(b); err != nil {
			return nil, err
		}
	}

	return io.NopCloser(&buf), nil
}

// restore rebuilds CommitLog state from a snapshot. Destructive:
// calls Reset before re-appending.
func (s *Service) restore(rc io.ReadCloser) error {
	ctx := context.Background()

	if err := s.commitlog.Reset(ctx); err != nil {
		return err
	}

	lenBuf := [8]byte{}

	for {
		_, err := io.ReadFull(rc, lenBuf[:])
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		length := binary.BigEndian.Uint64(lenBuf[:])
		data := make([]byte, length)

		if _, err := io.ReadFull(rc, data); err != nil {
			return err
		}

		var rec api.Record
		if err := proto.Unmarshal(data, &rec); err != nil {
			return err
		}

		if _, err := s.commitlog.Append(ctx, &rec); err != nil {
			return err
		}
	}
}
