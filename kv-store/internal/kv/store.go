package kv

import (
	"errors"
	"sync"
)

var ErrNotFound = errors.New("key not found")

type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewStore() *Store {
	return &Store{
		data: make(map[string][]byte),
	}
}

func (s *Store) Put(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	valCopy := make([]byte, len(value))
	copy(valCopy, value)
	s.data[key] = valCopy
}

func (s *Store) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	ret := make([]byte, len(val))
	copy(ret, val)
	return ret, nil
}

func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

func (s *Store) KeysSnapshot() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

func (s *Store) PutIfNotExists(key string, value []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.data[key]; exists {
		return false
	}

	valCopy := make([]byte, len(value))
	copy(valCopy, value)
	s.data[key] = valCopy
	return true
}
