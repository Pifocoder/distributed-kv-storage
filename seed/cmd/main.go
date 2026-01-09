package main

import (
	"net/http"
	"seed/internal/cluster"
	"seed/internal/handler"
	"time"
)

func main() {
	// Инициализация Core
	cluster := cluster.NewCluster()

	// Фоновая очистка
	go func() {
		for range time.Tick(2 * time.Second) {
			cluster.CleanUp()
		}
	}()

	// Роутинг
	http.HandleFunc("/register", handler.Register(cluster))
	http.HandleFunc("/heartbeat", handler.Heartbeat(cluster))

	http.ListenAndServe(":9000", nil)
}
