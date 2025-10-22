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

	httpapi "github.com/UITGo/driver-stream/internal/http"
	kafka "github.com/UITGo/driver-stream/internal/kafka"
	redisstore "github.com/UITGo/driver-stream/internal/redis"
)

func getenv(k, def string) string { if v := os.Getenv(k); v != "" { return v }; return def }
func splitCSV(s string) []string { if s == "" { return nil }; return strings.Split(s, ",") }

func main() {
	redisAddr := getenv("REDIS_ADDR", "localhost:6379")
	kBrokers  := splitCSV(getenv("KAFKA_BROKERS", "localhost:9092"))
	topic     := getenv("KAFKA_TOPIC_LOCATION", "driver.location")
	addr      := getenv("HTTP_ADDR", ":8080")

	store := redisstore.New(redisAddr)
	if err := store.LoadClaimScript(context.Background(),
		mustRead("internal/assign/claim.lua")); err != nil {
		log.Fatal(err)
	}

	prod, err := kafka.New(kBrokers, topic)
	if err != nil { log.Fatal(err) }

	srv := httpapi.NewServer(store, prod)
	httpSrv := &http.Server{ Addr: addr, Handler: srv.Router(),
		ReadTimeout: 5*time.Second, WriteTimeout: 5*time.Second }

	go func() {
		log.Printf("driver-stream listening on %s", addr)
		log.Fatal(httpSrv.ListenAndServe())
	}()

	stop := make(chan os.Signal,1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
}

func mustRead(p string) string {
	b, err := os.ReadFile(p); if err != nil { log.Fatal(err) }
	return string(b)
}
