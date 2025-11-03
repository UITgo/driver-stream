package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	grpcsrv "github.com/UITGo/driver-stream/internal/grpc"
	httpapi "github.com/UITGo/driver-stream/internal/http"
	kafka "github.com/UITGo/driver-stream/internal/kafka"
	redisstore "github.com/UITGo/driver-stream/internal/redis"
)

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}

func main() {
	// === env ===
	redisAddr := getenv("REDIS_ADDR", "localhost:6379")
	kBrokers := splitCSV(getenv("KAFKA_BROKERS", "localhost:9092"))
	topic := getenv("KAFKA_TOPIC_LOCATION", "driver.location")
	httpAddr := getenv("HTTP_ADDR", ":8080")
	grpcAddr := getenv("GRPC_ADDR", ":50052")

	// === deps ===
	store := redisstore.New(redisAddr)
	if err := store.LoadClaimScript(context.Background(), mustRead("internal/assign/claim.lua")); err != nil {
		log.Fatal(err)
	}
	prod, err := kafka.New(kBrokers, topic)
	if err != nil {
		log.Fatal(err)
	}

	// === HTTP server ===
	srv := httpapi.NewServer(store, prod)
	httpSrv := &http.Server{
		Addr:         httpAddr,
		Handler:      srv.Router(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	// === gRPC server ===
	grpcServer, lis, err := grpcsrv.New(grpcAddr)
	if err != nil {
		log.Fatal(err)
	}

	// === run both ===
	go func() {
		log.Printf("[driver-stream] HTTP listening on %s", httpAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("http error: %v", err)
		}
	}()
	go func() {
		log.Printf("[driver-stream] gRPC listening on %s", grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("grpc error: %v", err)
		}
	}()

	// === graceful shutdown ===
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("[driver-stream] shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = httpSrv.Shutdown(ctx) // đóng HTTP mềm
	grpcServer.GracefulStop() // đóng gRPC mềm (Stop() nếu muốn nhanh)
}

func mustRead(p string) string {
	b, err := os.ReadFile(p)
	if err != nil {
		log.Fatal(err)
	}
	return string(b)
}
