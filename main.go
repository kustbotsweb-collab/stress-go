package main

import (
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"sync"
	"syscall"
	"time"
)

// ==========================================
// CONFIGURATION (STAY STEALTHY)
// ==========================================
var (
	// Split the URL to insert random IDs dynamically
	BASE_URL        = getEnv("TARGET_BASE_URL", "https://shrutibots.site/stream/")
	URL_PARAMS      = getEnv("TARGET_PARAMS", "?type=audio&token=ShrutiMusic8WnZDCSQoIjwGMmDlyPcVvKmK7YOfObUdWYVgZ6hTK4U0WGgwU5HZIhMhByPoZSDc0EwzT2LnChE1LtUj4oYyCANu3qLLgIXgSBgShrutiBots")
	TOTAL_CLIENTS   = 3                                      // Number of concurrent refresh clients
	MAX_WORKERS     = 3
	REFRESH_DELAY   = 1000 * time.Millisecond // Lower = heavier stress (80ms ≈ 375 RPS total)
)

// Worker Semaphore to limit max workers
var workerSemaphore = make(chan struct{}, MAX_WORKERS)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// ==========================================
// HELPERS
// ==========================================
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

// Generates a random 11-character string resembling a YouTube Video ID
func generateRandomVideoID() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
	b := make([]byte, 11)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// ==========================================
// CLIENT STRUCT
// ==========================================
type StressClient struct {
	clientID     int
	running      bool
	lastActivity time.Time
	lock         sync.Mutex
	httpClient   *http.Client
}

func NewStressClient(id int) *StressClient {
	return &StressClient{
		clientID: id,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

func (c *StressClient) DoRefresh() {
	c.lock.Lock()
	c.lastActivity = time.Now()
	c.lock.Unlock()

	// Construct the new URL with the random video ID
	randomVideoID := generateRandomVideoID()
	targetURL := BASE_URL + randomVideoID + URL_PARAMS

	req, err := http.NewRequest("GET", targetURL, nil)
	if err != nil {
		log.Printf("[Client %d] NewRequest failed: %v", c.clientID, err)
		return
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.Printf("[Client %d] Request error: %v", c.clientID, err)
		return
	}
	defer resp.Body.Close()

	// Consume body: Downloads the audio file chunk by chunk and discards it instantly.
	// This ensures the stream is pulled from the server but deleted from RAM immediately,
	// preventing memory leaks during sustained load testing.
	_, err = io.Copy(io.Discard, resp.Body)
	if err != nil {
		log.Printf("[Client %d] Stream read error: %v", c.clientID, err)
		return
	}

	// Logging response headers like X-Cache can show if you hit or missed (Fastly specific)
	log.Printf("[Client %d] Page Refresh -> Status: %d | Video ID: %s", c.clientID, resp.StatusCode, randomVideoID)
}

func (c *StressClient) Run() {
	c.running = true
	workerSemaphore <- struct{}{}
	defer func() { <-workerSemaphore }()

	for c.running {
		c.DoRefresh()
		time.Sleep(REFRESH_DELAY)
		runtime.Gosched()
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	debug.SetMemoryLimit(850 * 1024 * 1024)
	runtime.GOMAXPROCS(runtime.NumCPU())

	log.Println("========================================")
	log.Println(" REBATE CLAIMER HTTP REFRESH STRESS TESTER ")
	log.Printf(" Target Base: %s", BASE_URL)
	log.Printf(" Clients: %d | Workers: %d | Delay: %v", TOTAL_CLIENTS, MAX_WORKERS, REFRESH_DELAY)
	log.Println(" Mode: Repeated page refresh with random Video IDs")
	log.Println(" Purpose: Test regional routing + load balancing")
	log.Println(" Contact: @Rabit0505")
	log.Println("========================================")

	var wg sync.WaitGroup
	for i := 0; i < TOTAL_CLIENTS; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			client := NewStressClient(id)
			client.Run()
		}(i)
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-done

	log.Println("Shutting down gracefully...")
	wg.Wait()
}
