package redisstore

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

type Store struct {
	Rdb      *redis.Client
	claimSHA string
}

func New(addr string) *Store {
	return &Store{Rdb: redis.NewClient(&redis.Options{Addr: addr})}
}

const (
	geoKey      = "geo:drivers"
	ttlPresence = 60 * time.Second
)

func (s *Store) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return s.Rdb.HGetAll(ctx, key).Result()
}

func (s *Store) DelKeys(ctx context.Context, keys ...string) error {
	pipe := s.Rdb.Pipeline()
	for _, k := range keys {
		pipe.Del(ctx, k)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (s *Store) SetStatus(ctx context.Context, driverID string, online bool) error {
	key := "presence:driver:" + driverID
	if online {
		pipe := s.Rdb.Pipeline()
		pipe.HSet(ctx, key, "status", "ONLINE", "last_seen", time.Now().UnixMilli())
		pipe.Expire(ctx, key, ttlPresence)
		_, err := pipe.Exec(ctx)
		return err
	}
	if err := s.Rdb.Del(ctx, key).Err(); err != nil {
		return err
	}
	return s.Rdb.ZRem(ctx, geoKey, driverID).Err()
}

func (s *Store) UpsertLocation(ctx context.Context, driverID string, lat, lng float64, fields map[string]any) error {
	pipe := s.Rdb.Pipeline()
	pipe.GeoAdd(ctx, geoKey, &redis.GeoLocation{Name: driverID, Longitude: lng, Latitude: lat})
	args := []any{"lat", lat, "lng", lng, "last_seen", time.Now().UnixMilli()}
	for k, v := range fields {
		args = append(args, k, v)
	}
	pipe.HSet(ctx, "presence:driver:"+driverID, args...)
	pipe.Expire(ctx, "presence:driver:"+driverID, ttlPresence)
	_, err := pipe.Exec(ctx)
	return err
}

type NearbyDriver struct {
	ID       string
	Dist     float64
	Lat      float64
	Lng      float64
	LastSeen int64
}

func (s *Store) Nearby(ctx context.Context, lat, lng float64, radius, limit int) ([]NearbyDriver, error) {
	res, err := s.Rdb.GeoRadius(ctx, geoKey, lng, lat, &redis.GeoRadiusQuery{
		Radius: float64(radius), Unit: "m", WithDist: true, WithCoord: true, Count: limit, Sort: "ASC",
	}).Result()
	if err != nil {
		return nil, err
	}
	out := make([]NearbyDriver, 0, len(res))
	for _, it := range res {
		h, _ := s.Rdb.HGetAll(ctx, "presence:driver:"+it.Name).Result()
		if h["status"] != "ONLINE" {
			continue
		}
		out = append(out, NearbyDriver{ID: it.Name, Dist: it.Dist, Lat: it.Latitude, Lng: it.Longitude})
	}
	return out, nil
}

func (s *Store) PrepareAssign(ctx context.Context, tripID string, cands []string, ttl time.Duration) error {
	candKey := "trip:" + tripID + ":candidates"
	claimKey := "trip:" + tripID + ":claimed"
	dlKey := "trip:" + tripID + ":deadline"

	pipe := s.Rdb.TxPipeline()

	// reset keys cũ (nếu có)
	pipe.Del(ctx, candKey, claimKey, dlKey)

	// 1) candidates
	members := make([]interface{}, 0, len(cands))
	for _, id := range cands {
		members = append(members, id)
	}
	pipe.SAdd(ctx, candKey, members...)
	pipe.Expire(ctx, candKey, ttl)

	// 2) claimed = "0" (chưa ai claim) + TTL
	pipe.Set(ctx, claimKey, "0", ttl)

	// 3) deadline đặt giá trị bất kỳ + TTL
	// dùng Set với TTL thay vì Set rồi PEXPIRE để tránh race
	pipe.Set(ctx, dlKey, time.Now().UnixMilli(), ttl)

	// Exec
	_, err := pipe.Exec(ctx)
	return err
}

var (
	ErrExpired = errors.New("EXPIRED")
	ErrAlready = errors.New("ALREADY_CLAIMED")
	ErrNotCand = errors.New("NOT_CANDIDATE")
	ErrOffline = errors.New("DRIVER_OFFLINE")
)

func (s *Store) LoadClaimScript(ctx context.Context, lua string) error {
	sha := s.Rdb.ScriptLoad(ctx, lua).Val()
	s.claimSHA = sha
	return nil
}

func (s *Store) Claim(ctx context.Context, tripID, driverID string) error {
	keys := []string{
		"trip:" + tripID + ":deadline",
		"trip:" + tripID + ":claimed",
		"trip:" + tripID + ":candidates",
		"presence:driver:" + driverID,
	}
	_, err := s.Rdb.EvalSha(ctx, s.claimSHA, keys, time.Now().UnixMilli(), driverID).Result()
	if err != nil {
		switch err.Error() {
		case "EXPIRED":
			return ErrExpired
		case "ALREADY_CLAIMED":
			return ErrAlready
		case "NOT_CANDIDATE":
			return ErrNotCand
		case "DRIVER_OFFLINE":
			return ErrOffline
		}
	}
	return nil
}
