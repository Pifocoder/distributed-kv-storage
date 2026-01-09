package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"kv-store/internal/cluster"
	"kv-store/internal/config"
	"kv-store/internal/hashring"
	"kv-store/internal/httpapi"
	"kv-store/internal/kv"
)

const (
	heartbeatInterval = 5 * time.Second
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: kv-node <config.yaml>")
	}

	cfg, err := config.Load(os.Args[1])
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	store := kv.NewStore()
	ring := hashring.New(cfg.Hash.VNodesPerNode)

	dc := cluster.NewDiscoveryClient(cfg.Cluster.SeedAddr)
	nodesChan := make(chan []cluster.NodeInfo, 10)

	log.Println("Starting discovery...")
	go dc.Start(heartbeatInterval, nodesChan)

	log.Println("Waiting for initial registration...")
	initialNodes := <-nodesChan

	myID := dc.GetMyID()
	log.Printf("Node initialized. ID: %s", myID)

	ring.UpdateRing(initialNodes)

	go func() {
		for nodes := range nodesChan {
			ring.UpdateRing(nodes)
			log.Printf("Cluster updated. Peers: %d", len(nodes))
		}
	}()

	h := httpapi.NewHandler(store, ring, hashring.NodeID(myID))
	router := httpapi.NewRouter(h)

	srvAddr := fmt.Sprintf(":%s", cfg.Cluster.Port)
	log.Printf("HTTP API listening on %s", srvAddr)
	if err := http.ListenAndServe(srvAddr, router); err != nil {
		log.Fatal(err)
	}
}
