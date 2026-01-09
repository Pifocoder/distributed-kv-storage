package cluster

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

var ErrUnauthorized = errors.New("node unauthorized")

type registerResponse struct {
	ID string `json:"id"`
}

type heartbeatRequest struct {
	ID string `json:"id"`
}

type nodeDTO struct {
	ID   string `json:"id"`
	Addr string `json:"addr"` // Добавили адрес
}

type heartbeatResponse struct {
	ActiveNodes []nodeDTO `json:"active_nodes"`
}

// NodeInfo — структура, которую мы отдаем наружу (в канал)
type NodeInfo struct {
	ID   string
	Addr string
}

type DiscoveryClient struct {
	seedURL string
	myID    string
	mu      sync.RWMutex
	client  *http.Client
}

func NewDiscoveryClient(seedAddr string) *DiscoveryClient {
	return &DiscoveryClient{
		seedURL: "http://" + seedAddr,
		client:  &http.Client{Timeout: 3 * time.Second},
	}
}

// Start теперь пишет в канал []NodeInfo, содержащий и ID, и Адрес
func (d *DiscoveryClient) Start(interval time.Duration, updates chan<- []NodeInfo) {
	d.ensureRegistered()

	d.doHeartbeat(updates)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		d.doHeartbeat(updates)
	}
}

func (d *DiscoveryClient) GetMyID() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.myID
}

func (d *DiscoveryClient) ensureRegistered() {
	for {
		if err := d.register(); err == nil {
			log.Printf("[Discovery] Registered successfully. ID: %s", d.GetMyID())
			return
		}
		time.Sleep(2 * time.Second)
	}
}

func (d *DiscoveryClient) register() error {
	body := []byte("{}")
	resp, err := d.client.Post(d.seedURL+"/register", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("register failed: status %d", resp.StatusCode)
	}

	var res registerResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return err
	}

	if res.ID == "" {
		return errors.New("empty ID received")
	}

	d.mu.Lock()
	d.myID = res.ID
	d.mu.Unlock()

	return nil
}

func (d *DiscoveryClient) doHeartbeat(updates chan<- []NodeInfo) {
	nodes, err := d.heartbeat()

	if errors.Is(err, ErrUnauthorized) {
		log.Println("[Discovery] Session lost. Re-registering...")
		d.ensureRegistered()
		nodes, err = d.heartbeat()
	}

	if err != nil {
		log.Printf("[Discovery] Heartbeat error: %v", err)
		return
	}

	select {
	case updates <- nodes:
	default:
	}
}

func (d *DiscoveryClient) heartbeat() ([]NodeInfo, error) {
	id := d.GetMyID()
	if id == "" {
		return nil, errors.New("no ID")
	}

	reqPayload, _ := json.Marshal(heartbeatRequest{ID: id})
	resp, err := d.client.Post(d.seedURL+"/heartbeat", "application/json", bytes.NewReader(reqPayload))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return nil, ErrUnauthorized
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var res heartbeatResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}

	// Преобразуем DTO в наш формат
	infos := make([]NodeInfo, len(res.ActiveNodes))
	for i, n := range res.ActiveNodes {
		infos[i] = NodeInfo{ID: n.ID, Addr: n.Addr}
	}

	return infos, nil
}
