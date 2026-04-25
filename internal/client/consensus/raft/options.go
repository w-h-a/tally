package raft

import (
	"context"

	hraft "github.com/hashicorp/raft"
	consensus "github.com/w-h-a/tally/internal/client/consensus"
)

type logStoreKey struct{}
type stableStoreKey struct{}

func WithLogStore(store hraft.LogStore) consensus.Option {
	return func(o *consensus.Options) {
		o.Context = context.WithValue(o.Context, logStoreKey{}, store)
	}
}

func LogStoreFrom(ctx context.Context) (hraft.LogStore, bool) {
	store, ok := ctx.Value(logStoreKey{}).(hraft.LogStore)
	return store, ok
}

func WithStableStore(store hraft.StableStore) consensus.Option {
	return func(o *consensus.Options) {
		o.Context = context.WithValue(o.Context, stableStoreKey{}, store)
	}
}

func StableStoreFrom(ctx context.Context) (hraft.StableStore, bool) {
	store, ok := ctx.Value(stableStoreKey{}).(hraft.StableStore)
	return store, ok
}
