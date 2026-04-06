package commitlog

import "context"

type Option func(*Options)

type Options struct {
	Location      string
	MaxStoreBytes uint64
	MaxIndexBytes uint64
	Context       context.Context
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

func NewOptions(opts ...Option) Options {
	options := Options{
		Context: context.Background(),
	}

	for _, fn := range opts {
		fn(&options)
	}

	return options
}
