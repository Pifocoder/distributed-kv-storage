package entity

import "time"

type Node struct {
	ID       string
	LastSeen time.Time
	Addr     string
}
