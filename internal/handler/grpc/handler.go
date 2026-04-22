package grpchandler

import (
	"context"
	"errors"
	"io"

	distributedlog "github.com/w-h-a/tally/internal/service/distributed_log"
	api "github.com/w-h-a/tally/proto/log/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	api.UnimplementedLogServiceServer
	service *distributedlog.Service
}

func New(service *distributedlog.Service) *Handler {
	return &Handler{service: service}
}

func (h *Handler) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	if req.Record == nil {
		return nil, status.Error(codes.InvalidArgument, "record is required")
	}

	offset, err := h.service.Append(ctx, req.Record)
	if err != nil {
		return nil, err
	}

	return &api.ProduceResponse{Offset: offset}, nil
}

func (h *Handler) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	rec, err := h.service.Read(ctx, req.Offset)
	if err != nil {
		if errors.Is(err, distributedlog.ErrOffsetOutOfRange) {
			return nil, status.Error(codes.NotFound, err.Error())
		}

		return nil, err
	}

	return &api.ConsumeResponse{Record: rec}, nil
}

func (h *Handler) GetServers(ctx context.Context, req *api.GetServersRequest) (*api.GetServersResponse, error) {
	servers, err := h.service.GetServers(ctx)
	if err != nil {
		return nil, err
	}

	return &api.GetServersResponse{Servers: servers}, nil
}

func (h *Handler) ProduceStream(stream api.LogService_ProduceStreamServer) error {
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

		offset, err := h.service.Append(stream.Context(), req.Record)
		if err != nil {
			return err
		}

		if err := stream.Send(&api.ProduceStreamResponse{Offset: offset}); err != nil {
			return err
		}
	}
}

func (h *Handler) ConsumeStream(req *api.ConsumeStreamRequest, stream api.LogService_ConsumeStreamServer) error {
	for offset := req.Offset; ; offset++ {
		rec, err := h.service.Read(stream.Context(), offset)
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
