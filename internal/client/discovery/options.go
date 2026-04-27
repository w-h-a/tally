package discovery

import "context"

type Option func(*Options)

type Options struct {
	NodeName       string
	BindAddr       string
	Tags           map[string]string
	StartJoinAddrs []string
	Context        context.Context
}

func WithNodeName(name string) Option {
	return func(o *Options) {
		o.NodeName = name
	}
}

func WithBindAddr(addr string) Option {
	return func(o *Options) {
		o.BindAddr = addr
	}
}

func WithTags(tags map[string]string) Option {
	return func(o *Options) {
		o.Tags = tags
	}
}

func WithStartJoinAddrs(addrs []string) Option {
	return func(o *Options) {
		o.StartJoinAddrs = addrs
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
