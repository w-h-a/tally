package consensus

import (
	"context"
	"io"
)

type ApplyFn func([]byte) (uint64, error)
type SnapshotFn func() (io.ReadCloser, error)
type RestoreFn func(io.ReadCloser) error

type Option func(*Options)

type Options struct {
	ApplyFn    ApplyFn
	SnapshotFn SnapshotFn
	RestoreFn  RestoreFn
	DataDir    string
	BindAddr   string
	LocalID    string
	Bootstrap  bool
	Context    context.Context
}

func WithApplyFn(fn ApplyFn) Option {
	return func(o *Options) {
		o.ApplyFn = fn
	}
}

func WithSnapshotFn(fn SnapshotFn) Option {
	return func(o *Options) {
		o.SnapshotFn = fn
	}
}

func WithRestoreFn(fn RestoreFn) Option {
	return func(o *Options) {
		o.RestoreFn = fn
	}
}

func WithDataDir(dir string) Option {
	return func(o *Options) {
		o.DataDir = dir
	}
}

func WithBindAddr(addr string) Option {
	return func(o *Options) {
		o.BindAddr = addr
	}
}

func WithLocalID(id string) Option {
	return func(o *Options) {
		o.LocalID = id
	}
}

func WithBootstrap(b bool) Option {
	return func(o *Options) {
		o.Bootstrap = b
	}
}

func NewOptions(opts ...Option) Options {
	options := Options{
		Context: context.Background(),
	}

	for _, fn := range opts {
		fn(&options)
	}

	return options
}
