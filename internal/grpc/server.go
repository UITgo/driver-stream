package grpcsrv

import (
	"context"
	"net"

	pb "github.com/UITGo/driver-stream/internal/pb"

	"google.golang.org/grpc"
)

type Server struct {
	pb.UnimplementedDriverServiceServer
}

func (s *Server) UpdateStatus(ctx context.Context, r *pb.UpdateStatusRequest) (*pb.DriverAck, error) {
	// TODO: cập nhật trạng thái vào Redis hoặc in-memory
	return &pb.DriverAck{Success: true}, nil
}

func (s *Server) UpdateLocation(ctx context.Context, r *pb.UpdateLocationRequest) (*pb.DriverAck, error) {
	// TODO: lưu GeoRedis
	return &pb.DriverAck{Success: true}, nil
}

func (s *Server) GetNearbyDrivers(ctx context.Context, r *pb.GetNearbyDriversRequest) (*pb.GetNearbyDriversResponse, error) {
	// Demo: trả cứng 1 driver gần
	return &pb.GetNearbyDriversResponse{
		Drivers: []*pb.NearbyDriver{{DriverId: "d1", Distance: 150, Lat: r.Location.Lat, Lng: r.Location.Lng}},
	}, nil
}

func (s *Server) PrepareAssign(ctx context.Context, r *pb.PrepareAssignRequest) (*pb.PrepareAssignResponse, error) {
	// Demo: giả lập đã publish assign
	return &pb.PrepareAssignResponse{Queued: true, Topic: "assigns"}, nil
}

func (s *Server) ClaimTrip(ctx context.Context, r *pb.ClaimTripRequest) (*pb.ClaimTripResponse, error) {
	// Demo: luôn ACCEPTED
	return &pb.ClaimTripResponse{Status: "ACCEPTED"}, nil
}

func New(addr string) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	s := grpc.NewServer()
	pb.RegisterDriverServiceServer(s, &Server{}) // Server bạn đã triển khai
	return s, lis, nil
}
