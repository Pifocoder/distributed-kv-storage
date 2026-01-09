package httpapi

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"kv-store/internal/hashring"
	"kv-store/internal/kv"
)

type Handler struct {
	store  *kv.Store
	ring   *hashring.HashRing
	self   hashring.NodeID
	client *http.Client
}

func NewHandler(store *kv.Store, ring *hashring.HashRing, self hashring.NodeID) *Handler {
	return &Handler{
		store:  store,
		ring:   ring,
		self:   self,
		client: &http.Client{Timeout: 5 * time.Second}, // Таймаут для межсервисных запросов
	}
}

// proxyRequest выполняет запрос к другой ноде
func (h *Handler) proxyRequest(w http.ResponseWriter, r *http.Request, targetID hashring.NodeID) {
	targetAddr, ok := h.ring.GetNodeAddr(targetID)
	if !ok {
		http.Error(w, "node address not found", http.StatusInternalServerError)
		return
	}

	url := fmt.Sprintf("http://%s%s", targetAddr, r.URL.RequestURI())

	// Создаем новый запрос (копируем тело, если есть)
	proxyReq, err := http.NewRequest(r.Method, url, r.Body)
	if err != nil {
		http.Error(w, "proxy error", http.StatusInternalServerError)
		return
	}

	// Копируем заголовки (Content-Type и т.д.)
	for name, values := range r.Header {
		for _, value := range values {
			proxyReq.Header.Add(name, value)
		}
	}

	// Выполняем запрос
	resp, err := h.client.Do(proxyReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("proxy failed: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Копируем заголовки ответа
	for name, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}

	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (h *Handler) Put(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}

	node, err := h.ring.PrimaryNode(key)
	if err != nil {
		http.Error(w, "no nodes", http.StatusServiceUnavailable)
		return
	}

	if node != h.self {
		h.proxyRequest(w, r, node)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	h.store.Put(key, body)
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}

	node, err := h.ring.PrimaryNode(key)
	if err != nil {
		http.Error(w, "no nodes", http.StatusServiceUnavailable)
		return
	}

	if node != h.self {
		h.proxyRequest(w, r, node)
		return
	}

	val, err := h.store.Get(key)
	if errors.Is(err, kv.ErrNotFound) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "error", http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(val)
}

func (h *Handler) Delete(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}

	node, err := h.ring.PrimaryNode(key)
	if err != nil {
		http.Error(w, "no nodes", http.StatusServiceUnavailable)
		return
	}

	if node != h.self {
		h.proxyRequest(w, r, node)
		return
	}

	h.store.Delete(key)
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}
