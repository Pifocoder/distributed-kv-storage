package hashring

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"kv-store/internal/cluster"
	"sort"
	"sync"
)

type NodeID string

type HashRing struct {
	mu         sync.RWMutex
	vnodes     int
	ring       []uint32
	hashToNode map[uint32]NodeID

	// Храним не пустую структуру, а адрес ноды
	nodes map[NodeID]string
}

func New(vnodes int) *HashRing {
	return &HashRing{
		vnodes:     vnodes,
		ring:       []uint32{},
		hashToNode: make(map[uint32]NodeID),
		nodes:      make(map[NodeID]string),
	}
}

func hash(s string) uint32 {
	h := md5.Sum([]byte(s))
	return binary.BigEndian.Uint32(h[:4])
}

// UpdateRing теперь принимает структуру с адресами
func (r *HashRing) UpdateRing(activeNodes []cluster.NodeInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()

	newSet := make(map[NodeID]string, len(activeNodes))
	for _, info := range activeNodes {
		newSet[NodeID(info.ID)] = info.Addr
	}

	changed := false

	// 2. Удаляем ноды
	nodesToRemove := []NodeID{}
	for id := range r.nodes {
		if _, exists := newSet[id]; !exists {
			nodesToRemove = append(nodesToRemove, id)
		}
	}

	if len(nodesToRemove) > 0 {
		r.removeNodes(nodesToRemove)
		changed = true
	}

	// 3. Добавляем новые ноды или обновляем адреса
	for id, addr := range newSet {
		oldAddr, exists := r.nodes[id]
		if !exists {
			r.addNode(id, addr)
			changed = true
		} else if oldAddr != addr {
			// Если ID тот же, но сменился IP (редкий кейс, но возможный)
			r.nodes[id] = addr
		}
	}

	if changed {
		sort.Slice(r.ring, func(i, j int) bool { return r.ring[i] < r.ring[j] })
	}
}

func (r *HashRing) removeNodes(idsToRemove []NodeID) {
	toRemoveSet := make(map[NodeID]struct{})
	for _, id := range idsToRemove {
		toRemoveSet[id] = struct{}{}
		delete(r.nodes, id)
	}

	newRing := make([]uint32, 0, len(r.ring))

	for _, h := range r.ring {
		nodeID := r.hashToNode[h]
		if _, shouldRemove := toRemoveSet[nodeID]; !shouldRemove {
			newRing = append(newRing, h)
		} else {
			delete(r.hashToNode, h)
		}
	}
	r.ring = newRing
}

func (r *HashRing) addNode(id NodeID, addr string) {
	r.nodes[id] = addr
	for i := 0; i < r.vnodes; i++ {
		vID := fmt.Sprintf("%s#%d", id, i)
		h := hash(vID)
		r.ring = append(r.ring, h)
		r.hashToNode[h] = id
	}
}

func (r *HashRing) AddNode(id NodeID, addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.nodes[id]; ok {
		return
	}

	r.addNode(id, addr)
	sort.Slice(r.ring, func(i, j int) bool { return r.ring[i] < r.ring[j] })
}

// GetNodeAddr возвращает адрес ноды по её ID
func (r *HashRing) GetNodeAddr(id NodeID) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	addr, ok := r.nodes[id]
	return addr, ok
}

func (r *HashRing) PrimaryNode(key string) (NodeID, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.ring) == 0 {
		return "", fmt.Errorf("no nodes")
	}
	h := hash(key)
	i := sort.Search(len(r.ring), func(i int) bool { return r.ring[i] >= h })
	if i == len(r.ring) {
		i = 0
	}
	return r.hashToNode[r.ring[i]], nil
}
