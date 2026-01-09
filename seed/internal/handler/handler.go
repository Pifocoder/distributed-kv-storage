package handler

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"seed/internal/cluster"
)

type RegisterResp struct {
	ID string `json:"id"`
}
type HeartbeatReq struct {
	ID string `json:"id"`
}
type NodeDTO struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

func Register(uc *cluster.Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}

		realAddr := fmt.Sprintf("%s:8080", host)

		id := uc.Register(realAddr)

		json.NewEncoder(w).Encode(RegisterResp{ID: id})
	}
}

func Heartbeat(uc *cluster.Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req HeartbeatReq
		json.NewDecoder(r.Body).Decode(&req)

		nodes, ok := uc.Heartbeat(req.ID)
		if !ok {
			http.Error(w, "Unknown node", 401)
			return
		}

		dtos := make([]NodeDTO, len(nodes))
		for i, n := range nodes {
			dtos[i] = NodeDTO{ID: n.ID, Addr: n.Addr}
		}

		json.NewEncoder(w).Encode(map[string]interface{}{"active_nodes": dtos})
	}
}
