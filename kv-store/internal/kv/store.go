package kv

import (
	"errors"
	"sync"
)

var ErrNotFound = errors.New("not found")

type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewStore() *Store {
	return &Store{data: make(map[string][]byte)}
}

func (s *Store) Put(k string, v []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[k] = v
}

func (s *Store) Get(k string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[k]
	if !ok {
		return nil, ErrNotFound
	}
	return v, nil
}

func (s *Store) Delete(k string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, k)
}
