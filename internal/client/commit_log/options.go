package commitlog

import (
	"context"
	"time"
)

type Option func(*Options)

type Options struct {
	Location      string
	MaxStoreBytes uint64
	MaxIndexBytes uint64
	Retention     RetentionOptions
	Context       context.Context
}

type RetentionOptions struct {
	MaxAge   time.Duration
	MaxBytes uint64
	Interval time.Duration
}

func WithLocation(loc string) Option {
	return func(o *Options) {
		o.Location = loc
	}
}

func WithMaxStoreBytes(max uint64) Option {
	return func(o *Options) {
		o.MaxStoreBytes = max
	}
}

func WithMaxIndexBytes(max uint64) Option {
	return func(o *Options) {
		o.MaxIndexBytes = max
	}
}

func WithRetentionMaxAge(d time.Duration) Option {
	return func(o *Options) {
		o.Retention.MaxAge = d
	}
}

func WithRetentionMaxBytes(max uint64) Option {
	return func(o *Options) {
		o.Retention.MaxBytes = max
	}
}

func WithRetentionInterval(d time.Duration) Option {
	return func(o *Options) {
		o.Retention.Interval = d
	}
}

func NewOptions(opts ...Option) Options {
	options := Options{
		Retention: RetentionOptions{},
		Context:   context.Background(),
	}

	for _, fn := range opts {
		fn(&options)
	}

	return options
}
