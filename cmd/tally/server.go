package main

import (
	"context"
	"errors"

	distributedlog "github.com/w-h-a/tally/internal/service/distributed_log"
	api "github.com/w-h-a/tally/proto/log/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Config struct {
	Service *distributedlog.Service
}

type grpcServer struct {
	api.UnimplementedLogServiceServer
	service *distributedlog.Service
}

func newGRPCServer(config Config) *grpc.Server {
	srv := grpc.NewServer()

	s := &grpcServer{
		service: config.Service,
	}

	api.RegisterLogServiceServer(srv, s)

	return srv
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	if req.Record == nil {
		return nil, status.Error(codes.InvalidArgument, "record is required")
	}

	offset, err := s.service.Append(ctx, req.Record)
	if err != nil {
		return nil, err
	}

	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	rec, err := s.service.Read(ctx, req.Offset)
	if err != nil {
		if errors.Is(err, distributedlog.ErrOffsetOutOfRange) {
			return nil, status.Error(codes.NotFound, err.Error())
		}

		return nil, err
	}

	return &api.ConsumeResponse{Record: rec}, nil
}
