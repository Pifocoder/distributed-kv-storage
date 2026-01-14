package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const (
	// Количество тестовых ключей
	NumKeys = 1000

	// Интервал проверки после удаления ноды
	VerifyInterval = 10 * time.Second

	// Сколько раз проверять доступность после удаления
	VerifyAttempts = 3
)

type TestReport struct {
	TotalKeys     int       `json:"total_keys"`
	KeysLost      int       `json:"keys_lost"`
	KeysRecovered int       `json:"keys_recovered"`
	Timestamp     time.Time `json:"timestamp"`
	AffectedKeys  []string  `json:"affected_keys"`
}

type Cluster struct {
	mu         sync.RWMutex
	nodesPorts []int // Список портов активных нод
	client     *http.Client
}

func NewCluster(initialPorts []int) *Cluster {
	return &Cluster{
		nodesPorts: initialPorts,
		client:     &http.Client{Timeout: 3 * time.Second},
	}
}

func (c *Cluster) GetRandomNode() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.nodesPorts) == 0 {
		return ""
	}
	port := c.nodesPorts[rand.Intn(len(c.nodesPorts))]
	return fmt.Sprintf("http://localhost:%d", port)
}

func (c *Cluster) PutKey(key, value string) error {
	node := c.GetRandomNode()
	if node == "" {
		return fmt.Errorf("no nodes available")
	}

	url := fmt.Sprintf("%s/put?key=%s", node, key)
	req, _ := http.NewRequest("PUT", url, bytes.NewReader([]byte(value)))

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("bad status: %d", resp.StatusCode)
	}
	return nil
}

func (c *Cluster) GetKey(key string) (string, error) {
	node := c.GetRandomNode()
	if node == "" {
		return "", fmt.Errorf("no nodes available")
	}

	url := fmt.Sprintf("%s/get?key=%s", node, key)
	resp, err := c.client.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", fmt.Errorf("key not found")
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	return string(body), nil
}

// DockerContainerList возвращает список ID контейнеров kv-node
func DockerContainerList() ([]string, error) {
	cmd := exec.Command("docker", "ps", "-q", "--filter", "name=kv-node")
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	ids := strings.Split(strings.TrimSpace(string(out)), "\n")
	var result []string
	for _, id := range ids {
		if id != "" {
			result = append(result, id)
		}
	}
	return result, nil
}

func DockerStopContainer(containerID string) error {
	log.Printf("🔴 Stopping container %s", containerID[:12])
	cmd := exec.Command("docker", "stop", containerID)
	return cmd.Run()
}

func DockerStartContainer(containerID string) error {
	log.Printf("🟢 Starting container %s", containerID[:12])
	cmd := exec.Command("docker", "start", containerID)
	return cmd.Run()
}

// DiscoverNodePorts находит порты активных kv-node контейнеров
func DiscoverNodePorts() ([]int, error) {
	cmd := exec.Command("docker", "compose", "ps", "--format", "json")
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	var ports []int
	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		var container map[string]interface{}
		if err := json.Unmarshal([]byte(line), &container); err != nil {
			continue
		}

		// Фильтруем только ноды (игнорируем seed)
		if name, ok := container["Name"].(string); ok {
			if !strings.Contains(name, "kv-node") {
				continue
			}
		}

		// Ищем публикованные порты
		if publishers, ok := container["Publishers"].([]interface{}); ok {
			for _, pub := range publishers {
				if pubMap, ok := pub.(map[string]interface{}); ok {
					if portFloat, ok := pubMap["PublishedPort"].(float64); ok {
						if int(portFloat) > 0 {
							ports = append(ports, int(portFloat))
						}
					}
				}
			}
		}
	}

	return ports, nil
}

func main() {
	rand.Seed(time.Now().UnixNano())
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	log.Println("🚀 Starting distributed KV-store chaos test")

	// 1. Обнаруживаем активные ноды
	ports, err := DiscoverNodePorts()
	if err != nil || len(ports) == 0 {
		log.Fatal("Failed to discover nodes. Make sure docker compose is running.")
	}
	log.Printf("✅ Discovered %d nodes: %v", len(ports), ports)

	cluster := NewCluster(ports)

	// 2. Записываем тестовые данные
	log.Printf("📝 Writing %d test keys...", NumKeys)
	testData := make(map[string]string)
	for i := 0; i < NumKeys; i++ {
		key := fmt.Sprintf("test_key_%d", i)
		value := fmt.Sprintf("value_%d_%d", i, time.Now().Unix())
		testData[key] = value

		if err := cluster.PutKey(key, value); err != nil {
			log.Printf("⚠️  Failed to write %s: %v", key, err)
		}
	}
	log.Println("✅ Data written")

	// 3. Проверяем, что все ключи доступны
	time.Sleep(2 * time.Second)
	log.Println("🔍 Verifying initial data...")
	initialLost := verifyKeys(cluster, testData)
	if initialLost > 0 {
		log.Printf("⚠️  WARNING: %d keys already missing before chaos!", initialLost)
	}

	// 4. Начинаем chaos testing
	log.Println("\n💥 Starting chaos: killing random node...")

	containers, _ := DockerContainerList()
	if len(containers) == 0 {
		log.Fatal("No containers found")
	}

	victimContainer := containers[rand.Intn(len(containers))]
	if err := DockerStopContainer(victimContainer); err != nil {
		log.Fatalf("Failed to stop container: %v", err)
	}

	// Обновляем список портов (убираем мертвую ноду из пула)
	time.Sleep(3 * time.Second)
	newPorts, _ := DiscoverNodePorts()
	cluster.mu.Lock()
	cluster.nodesPorts = newPorts
	cluster.mu.Unlock()
	log.Printf("🔄 Cluster updated. Active nodes: %d", len(newPorts))

	// 5. Периодически проверяем доступность данных
	reports := make([]TestReport, 0)

	for attempt := 1; attempt <= VerifyAttempts; attempt++ {
		log.Printf("\n⏳ Waiting %v before verification attempt %d/%d...",
			VerifyInterval, attempt, VerifyAttempts)
		time.Sleep(VerifyInterval)

		log.Printf("🔍 Verification attempt %d", attempt)
		lostKeys, affectedKeys := verifyKeysDetailed(cluster, testData)

		report := TestReport{
			TotalKeys:     NumKeys,
			KeysLost:      lostKeys,
			KeysRecovered: NumKeys - lostKeys - initialLost,
			Timestamp:     time.Now(),
			AffectedKeys:  affectedKeys,
		}
		reports = append(reports, report)

		log.Printf("📊 Lost: %d, Available: %d", lostKeys, NumKeys-lostKeys)
	}

	// 6. Возвращаем ноду обратно
	log.Println("\n🔄 Restoring killed node...")
	if err := DockerStartContainer(victimContainer); err != nil {
		log.Printf("⚠️  Failed to restart: %v", err)
	}
	time.Sleep(5 * time.Second)

	// Финальная проверка
	log.Println("🔍 Final verification after node recovery...")
	finalLost := verifyKeys(cluster, testData)

	// 7. Итоговый отчет
	log.Println("\n" + strings.Repeat("=", 60))
	log.Println("📊 CHAOS TEST REPORT")
	log.Println(strings.Repeat("=", 60))
	log.Printf("Total Keys: %d", NumKeys)
	log.Printf("Initially Missing: %d", initialLost)
	log.Printf("Final Missing: %d", finalLost)
	log.Printf("Data Loss: %.2f%%", float64(finalLost)/float64(NumKeys)*100)

	// Сохраняем детальный отчет
	reportFile, _ := os.Create("chaos_test_report.json")
	defer reportFile.Close()
	json.NewEncoder(reportFile).Encode(reports)
	log.Println("\n📄 Detailed report saved to: chaos_test_report.json")
}

func verifyKeys(cluster *Cluster, testData map[string]string) int {
	lost := 0
	for key, expectedValue := range testData {
		val, err := cluster.GetKey(key)
		if err != nil || val != expectedValue {
			lost++
		}
	}
	return lost
}

func verifyKeysDetailed(cluster *Cluster, testData map[string]string) (int, []string) {
	lost := 0
	affectedKeys := []string{}

	for key, expectedValue := range testData {
		val, err := cluster.GetKey(key)
		if err != nil {
			lost++
			affectedKeys = append(affectedKeys, key)
		} else if val != expectedValue {
			lost++
			affectedKeys = append(affectedKeys, key)
		}
	}
	return lost, affectedKeys
}
