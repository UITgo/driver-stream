package httpapi

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-playground/validator/v10"

	kafka "github.com/UITGo/driver-stream/internal/kafka"
	redisstore "github.com/UITGo/driver-stream/internal/redis"
	. "github.com/UITGo/driver-stream/internal/util"
)

type Server struct {
	Store    *redisstore.Store
	Producer *kafka.Producer
	Validate *validator.Validate
}

func NewServer(store *redisstore.Store, prod *kafka.Producer) *Server {
	return &Server{Store: store, Producer: prod, Validate: validator.New()}
}

func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.RealIP, middleware.Logger, middleware.Recoverer)
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request){ Ok(w, M{"status":"ok"}) })

	r.Post("/v1/drivers/{id}/status", s.postStatus)
	r.Put("/v1/drivers/{id}/location", s.putLocation)
	r.Get("/v1/drivers/nearby", s.getNearby)
	r.Get("/v1/drivers/{id}/presence", s.getPresence) // debug

	// internal (gateway/TripService)
	r.Post("/v1/assign/prepare", s.prepareAssign)
	r.Post("/v1/assign/claim", s.claimAssign)
	r.Delete("/v1/assign/{tripId}", s.cleanupAssign)
	return r
}

// DTOs
type StatusReq struct{ Status string `json:"status" validate:"required,oneof=ONLINE OFFLINE"` }
type LocationReq struct{
	Lat float64 `json:"lat" validate:"required"`
	Lng float64 `json:"lng" validate:"required"`
	Speed *float64 `json:"speed,omitempty"`
	Heading *int   `json:"heading,omitempty"`
	Ts *int64      `json:"ts,omitempty"`
}
type PrepareReq struct{
	TripID string `json:"tripId" validate:"required"`
	Candidates []string `json:"candidates" validate:"required,min=1"`
	TTLSeconds int `json:"ttlSeconds" validate:"gte=5,lte=60"`
}

func (s *Server) postStatus(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r,"id")
	var req StatusReq; if ErrBindJSON(w,r,&req) { return }
	if err := s.Validate.Struct(req); err != nil { Bad(w,"INVALID_INPUT"); return }
	if err := s.Store.SetStatus(r.Context(), id, req.Status=="ONLINE"); err != nil { SrvErr(w,err); return }
	Ok(w, M{"driverId":id, "status":req.Status, "expiresInSec":60})
}

func (s *Server) putLocation(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r,"id")
	var req LocationReq; if ErrBindJSON(w,r,&req) { return }
	if err := s.Validate.Struct(req); err != nil { Bad(w,"INVALID_INPUT"); return }

	fields := M{}; if req.Speed!=nil { fields["speed"]=*req.Speed }; if req.Heading!=nil { fields["heading"]=*req.Heading }
	if err := s.Store.UpsertLocation(r.Context(), id, req.Lat, req.Lng, fields); err != nil { SrvErr(w,err); return }

	now := time.Now().UnixMilli(); if req.Ts!=nil { now = *req.Ts }
	_ = s.Producer.PublishDriverLocation(r.Context(), id, M{
		"event":"driver.location","driverId":id,"lat":req.Lat,"lng":req.Lng,"speed":req.Speed,"heading":req.Heading,"ts":now,
	})
	Status(w, 202, M{"ingested":true})
}

func (s *Server) getNearby(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	lat, _ := strconv.ParseFloat(q.Get("lat"),64)
	lng, _ := strconv.ParseFloat(q.Get("lng"),64)
	radius := AtoiDef(q.Get("radius"), 2000)
	limit  := AtoiDef(q.Get("limit"), 20)

	list, err := s.Store.Nearby(r.Context(), lat, lng, radius, limit)
	if err != nil { SrvErr(w,err); return }

	out := make([]M,0,len(list))
	for _, d := range list {
		out = append(out, M{"driverId":d.ID, "distance":d.Dist, "lat":d.Lat, "lng":d.Lng, "status":"ONLINE", "lastSeen":d.LastSeen})
	}
	Ok(w, M{"count": len(out), "drivers": out})
}

func (s *Server) getPresence(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r,"id")
	m, err := s.Store.HGetAll(r.Context(), "presence:driver:"+id)
	if err != nil { SrvErr(w,err); return }
	Ok(w, m)
}

func (s *Server) prepareAssign(w http.ResponseWriter, r *http.Request) {
	var req PrepareReq; if ErrBindJSON(w,r,&req) { return }
	if req.TTLSeconds==0 { req.TTLSeconds=15 }
	if err := s.Validate.Struct(req); err != nil { Bad(w,"INVALID_INPUT"); return }
	if err := s.Store.PrepareAssign(r.Context(), req.TripID, req.Candidates, time.Duration(req.TTLSeconds)*time.Second); err != nil { SrvErr(w,err); return }
	Ok(w, M{"tripId": req.TripID, "expiresInSec": req.TTLSeconds})
}

func (s *Server) claimAssign(w http.ResponseWriter, r *http.Request) {
	var body struct{ TripID, DriverID string }
	if ErrBindJSON(w,r,&body) { return }
	err := s.Store.Claim(r.Context(), body.TripID, body.DriverID)
	switch err {
	case nil: Ok(w, M{"tripId":body.TripID, "claimedBy":body.DriverID})
	case redisstore.ErrExpired: Conflict(w,"EXPIRED")
	case redisstore.ErrAlready: Conflict(w,"ALREADY_CLAIMED")
	case redisstore.ErrNotCand: Forbidden(w,"NOT_CANDIDATE")
	case redisstore.ErrOffline: Conflict(w,"DRIVER_OFFLINE")
	default: SrvErr(w,err)
	}
}

func (s *Server) cleanupAssign(w http.ResponseWriter, r *http.Request) {
	trip := chi.URLParam(r,"tripId")
	_ = s.Store.DelKeys(r.Context(),
		"trip:"+trip+":deadline","trip:"+trip+":candidates","trip:"+trip+":claimed")
	w.WriteHeader(204)
}
