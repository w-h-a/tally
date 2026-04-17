package main

import (
	"context"
	"errors"
	"io"

	distributedlog "github.com/w-h-a/tally/internal/service/distributed_log"
	api "github.com/w-h-a/tally/proto/log/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
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
	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)

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

func (s *grpcServer) ProduceStream(stream api.LogService_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF || stream.Context().Err() != nil {
			return nil
		}
		if err != nil {
			return err
		}

		if req.Record == nil {
			return status.Error(codes.InvalidArgument, "record is required")
		}

		offset, err := s.service.Append(stream.Context(), req.Record)
		if err != nil {
			return err
		}

		if err := stream.Send(&api.ProduceStreamResponse{Offset: offset}); err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(req *api.ConsumeStreamRequest, stream api.LogService_ConsumeStreamServer) error {
	for offset := req.Offset; ; offset++ {
		rec, err := s.service.Read(stream.Context(), offset)
		if err != nil {
			if errors.Is(err, distributedlog.ErrOffsetOutOfRange) {
				return nil
			}
			return err
		}

		if err := stream.Send(&api.ConsumeStreamResponse{Record: rec}); err != nil {
			return err
		}
	}
}
