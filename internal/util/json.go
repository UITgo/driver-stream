package util

import (
	"encoding/json"
	"net/http"
	"strconv"
)

type M = map[string]any

func ErrBindJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil { Bad(w,"INVALID_JSON"); return true }
	return false
}
func write(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type","application/json"); w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
func Ok(w http.ResponseWriter, v any)      { write(w, 200, v) }
func Bad(w http.ResponseWriter, msg string){ write(w, 400, M{"error":msg}) }
func SrvErr(w http.ResponseWriter, err error){ write(w, 500, M{"error":"SERVER_ERROR","detail": err.Error()}) }
func Status(w http.ResponseWriter, code int, v any){ write(w, code, v) }
func Conflict(w http.ResponseWriter, msg string){ write(w, 409, M{"error":msg}) }
func Forbidden(w http.ResponseWriter, msg string){ write(w, 403, M{"error":msg}) }
func AtoiDef(s string, def int) int { i,err:=strconv.Atoi(s); if err!=nil { return def }; return i }
