package cluster

import (
	"crypto/rand"
	"encoding/hex"
	"seed/internal/entity"
	"sync"
	"time"
)

type Cluster struct {
	nodes map[string]*entity.Node
	mu    sync.RWMutex
}

func NewCluster() *Cluster {
	return &Cluster{nodes: make(map[string]*entity.Node)}
}

func (c *Cluster) Register(addr string) string {
	id := generateID()
	c.mu.Lock()
	c.nodes[id] = &entity.Node{ID: id, LastSeen: time.Now(), Addr: addr}
	c.mu.Unlock()
	return id
}

// Heartbeat - обновление статуса и возврат живых нод
func (c *Cluster) Heartbeat(id string) ([]entity.Node, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if node, exists := c.nodes[id]; exists {
		node.LastSeen = time.Now()
	} else {
		return nil, false
	}

	// Собираем список всех
	active := make([]entity.Node, 0)
	for _, n := range c.nodes {
		active = append(active, *n)
	}
	return active, true
}

// CleanUp - удаление старых
func (c *Cluster) CleanUp() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for id, n := range c.nodes {
		if now.Sub(n.LastSeen) > 15*time.Second {
			delete(c.nodes, id)
		}
	}
}

func generateID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
