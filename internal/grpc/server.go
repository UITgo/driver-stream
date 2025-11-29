package grpcsrv

import (
	"context"
	"net"
	"time"

	pb "github.com/UITGo/driver-stream/internal/pb"
	kafka "github.com/UITGo/driver-stream/internal/kafka"
	redisstore "github.com/UITGo/driver-stream/internal/redis"

	"google.golang.org/grpc"
)

type Server struct {
	pb.UnimplementedDriverServiceServer
	Store    *redisstore.Store
	Producer *kafka.Producer
}

func (s *Server) UpdateStatus(ctx context.Context, r *pb.UpdateStatusRequest) (*pb.DriverAck, error) {
	err := s.Store.SetStatus(ctx, r.DriverId, r.Status == "ONLINE")
	if err != nil {
		return &pb.DriverAck{Success: false}, err
	}
	return &pb.DriverAck{Success: true}, nil
}

func (s *Server) UpdateLocation(ctx context.Context, r *pb.UpdateLocationRequest) (*pb.DriverAck, error) {
	fields := make(map[string]any)
	if r.Speed != 0 {
		fields["speed"] = r.Speed
	}
	if r.Heading != 0 {
		fields["heading"] = r.Heading
	}

	err := s.Store.UpsertLocation(ctx, r.DriverId, r.Location.Lat, r.Location.Lng, fields)
	if err != nil {
		return &pb.DriverAck{Success: false}, err
	}

	// Publish to Kafka
	ts := r.Ts
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	_ = s.Producer.PublishDriverLocation(ctx, r.DriverId, map[string]any{
		"event":    "driver.location",
		"driverId": r.DriverId,
		"lat":      r.Location.Lat,
		"lng":      r.Location.Lng,
		"speed":    r.Speed,
		"heading":  r.Heading,
		"ts":       ts,
	})

	return &pb.DriverAck{Success: true}, nil
}

func (s *Server) GetNearbyDrivers(ctx context.Context, r *pb.GetNearbyDriversRequest) (*pb.GetNearbyDriversResponse, error) {
	radius := int(r.Radius)
	if radius == 0 {
		radius = 2000 // default 2km
	}
	limit := int(r.Limit)
	if limit == 0 {
		limit = 20 // default
	}

	list, err := s.Store.Nearby(ctx, r.Location.Lat, r.Location.Lng, radius, limit)
	if err != nil {
		return nil, err
	}

	drivers := make([]*pb.NearbyDriver, 0, len(list))
	for _, d := range list {
		drivers = append(drivers, &pb.NearbyDriver{
			DriverId: d.ID,
			Distance: d.Dist,
			Lat:      d.Lat,
			Lng:      d.Lng,
		})
	}

	return &pb.GetNearbyDriversResponse{Drivers: drivers}, nil
}

func (s *Server) PrepareAssign(ctx context.Context, r *pb.PrepareAssignRequest) (*pb.PrepareAssignResponse, error) {
	ttlSeconds := int(r.TtlSeconds)
	if ttlSeconds == 0 {
		ttlSeconds = 15 // default
	}

	err := s.Store.PrepareAssign(
		ctx,
		r.TripId,
		r.CandidateIds,
		time.Duration(ttlSeconds)*time.Second,
	)
	if err != nil {
		return &pb.PrepareAssignResponse{Queued: false, Topic: ""}, err
	}

	// Note: SSE push is handled in HTTP server, not here
	// If needed, we can add SSE push logic here too, but for now keep it simple

	return &pb.PrepareAssignResponse{Queued: true, Topic: "assigns"}, nil
}

func (s *Server) ClaimTrip(ctx context.Context, r *pb.ClaimTripRequest) (*pb.ClaimTripResponse, error) {
	err := s.Store.Claim(ctx, r.TripId, r.DriverId)
	if err != nil {
		switch err {
		case redisstore.ErrExpired:
			return &pb.ClaimTripResponse{Status: "EXPIRED"}, nil
		case redisstore.ErrAlready:
			return &pb.ClaimTripResponse{Status: "ALREADY_CLAIMED"}, nil
		case redisstore.ErrNotCand:
			return &pb.ClaimTripResponse{Status: "NOT_CANDIDATE"}, nil
		case redisstore.ErrOffline:
			return &pb.ClaimTripResponse{Status: "DRIVER_OFFLINE"}, nil
		default:
			return &pb.ClaimTripResponse{Status: "DECLINED"}, err
		}
	}

	return &pb.ClaimTripResponse{Status: "ACCEPTED"}, nil
}

func New(addr string, store *redisstore.Store, prod *kafka.Producer) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	s := grpc.NewServer()
	pb.RegisterDriverServiceServer(s, &Server{
		Store:    store,
		Producer: prod,
	})
	return s, lis, nil
}
