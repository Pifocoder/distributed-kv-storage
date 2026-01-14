package rebalance

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"kv-store/internal/hashring"
	"kv-store/internal/kv"
)

type Service struct {
	store  *kv.Store
	ring   *hashring.HashRing
	myID   hashring.NodeID
	client *http.Client

	triggerCh chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
}

func NewService(store *kv.Store, ring *hashring.HashRing, myID hashring.NodeID) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	return &Service{
		store:     store,
		ring:      ring,
		myID:      myID,
		client:    &http.Client{Timeout: 5 * time.Second},
		triggerCh: make(chan struct{}, 1),
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (s *Service) Start() {
	log.Println("Rebalance worker started")
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.triggerCh:
			s.performMigration()
		}
	}
}

func (s *Service) Stop() {
	s.cancel()
}

func (s *Service) Trigger() {
	select {
	case s.triggerCh <- struct{}{}:
	default:
	}
}

func (s *Service) performMigration() {
	log.Println("Starting rebalance cycle...")
	start := time.Now()
	moved := 0
	errors := 0

	keys := s.store.KeysSnapshot()

	for _, key := range keys {
		if s.ctx.Err() != nil {
			return
		}

		ownerID, err := s.ring.PrimaryNode(key)
		if err != nil {
			continue
		}

		if ownerID == s.myID {
			continue
		}

		val, err := s.store.Get(key)
		if err == kv.ErrNotFound {
			continue
		}

		targetAddr, ok := s.ring.GetNodeAddr(ownerID)
		if !ok {
			log.Printf("ERR: No addr for node %s", ownerID)
			errors++
			continue
		}

		if err := s.moveKey(key, val, targetAddr); err != nil {
			log.Printf("ERR: Failed to move %s to %s: %v", key, targetAddr, err)
			errors++
		} else {
			s.store.Delete(key)
			moved++
		}
	}

	log.Printf("Rebalance finished in %v. Moved: %d, Errors: %d", time.Since(start), moved, errors)
}

func (s *Service) moveKey(key string, val []byte, targetAddr string) error {
	url := fmt.Sprintf("http://%s/internal/put?key=%s", targetAddr, key)

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(val))
	if err != nil {
		return err
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}
