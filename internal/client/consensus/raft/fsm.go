package raft

import (
	"io"

	hraft "github.com/hashicorp/raft"
	consensus "github.com/w-h-a/tally/internal/client/consensus"
)

type fsmResponse struct {
	result uint64
	err    error
}

// fsm (finite state machine) is a thin callback wrapper satisfying hashicorp/raft.FSM.
// All behavior is delegated to the callbacks injected by the service layer.
// ApplyFn processes committed log entries, SnapshotFn captures
// current state, and RestoreFn rebuilds state from a snapshot.
type fsm struct {
	apply    consensus.ApplyFn
	snapshot consensus.SnapshotFn
	restore  consensus.RestoreFn
}

func newFSM(apply consensus.ApplyFn, snapshot consensus.SnapshotFn, restore consensus.RestoreFn) *fsm {
	return &fsm{
		apply:    apply,
		snapshot: snapshot,
		restore:  restore,
	}
}

// Apply is called by Raft after a log entry is committed by a quorum.
// It delegates to the injected callback.
func (f *fsm) Apply(log *hraft.Log) any {
	result, err := f.apply(log.Data)
	return &fsmResponse{result: result, err: err}
}

// Snapshot is called by Raft to capture a point-in-time snapshot of
// state. It delegates to the injected callback.
func (f *fsm) Snapshot() (hraft.FSMSnapshot, error) {
	r, err := f.snapshot()
	if err != nil {
		return nil, err
	}

	return &fsmSnapshot{reader: r}, nil
}

// Restore is called by Raft to rebuild state from a
// leader's snapshot. Typically after a node falls too far behind
// or joins fresh. It delegates to the injected callback.
func (f *fsm) Restore(rc io.ReadCloser) error {
	return f.restore(rc)
}

type fsmSnapshot struct {
	reader io.ReadCloser
}

func (s *fsmSnapshot) Persist(sink hraft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		sink.Cancel()
		return err
	}

	return sink.Close()
}

func (s *fsmSnapshot) Release() {
	s.reader.Close()
}
