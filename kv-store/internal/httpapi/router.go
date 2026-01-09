package httpapi

import "net/http"

func NewRouter(h *Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/put", h.Put)
	mux.HandleFunc("/get", h.Get)
	mux.HandleFunc("/delete", h.Delete)
	mux.HandleFunc("/health", h.Health)
	return mux
}
