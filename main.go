package main

import (
	"encoding/json"
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

	"github.com/gorilla/websocket"
)

// ==========================================
// CONFIGURATION (STAY STEALTHY)
// ==========================================
var (
	SERVER_URL      = getEnv("TARGET_URL", "wss://kingclaimer.xyz:8443/")
	TARGET_USERNAME = "AlbertS03" // HARDCODED USERNAME
	TOTAL_CLIENTS   = 1           // Recommended to keep at 1 for this specific username
	MAX_WORKERS     = 1           
	RECONNECT_DELAY = 10 * time.Second // Slower reconnect to avoid IP bans
)

// Worker Semaphore to limit max workers
var workerSemaphore = make(chan struct{}, MAX_WORKERS)

var (
	printHandshakeOnce sync.Once
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

// ==========================================
// CLIENT STRUCT
// ==========================================
type StressClient struct {
	clientID     int
	username     string
	ws           *websocket.Conn
	connected    bool
	running      bool
	lastActivity time.Time
	lock         sync.Mutex
}

func NewStressClient(id int) *StressClient {
	return &StressClient{
		clientID: id,
		username: TARGET_USERNAME, 
	}
}

func getWAFHeaders() http.Header {
	headers := http.Header{}
	headers.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	headers.Add("Origin", "https://stake.com")
	return headers
}

func (c *StressClient) Connect() bool {
	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 10 * time.Second

	ws, resp, err := dialer.Dial(SERVER_URL, getWAFHeaders())
	if err != nil {
		if resp != nil {
			log.Printf("[Client %d] Dial failed with status: %d", c.clientID, resp.StatusCode)
		}
		return false
	}

	c.ws = ws

	// Wait for "WELCOME"
	ws.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, welcomeMsg, err := ws.ReadMessage()
	if err != nil {
		c.Disconnect()
		return false
	}

	printHandshakeOnce.Do(func() {
		log.Printf("\n[+] SERVER WELCOME: %s\n", string(welcomeMsg))
	})

	// REGISTER WITH FIXED USERNAME
	regPayload := map[string]string{
		"type":     "register",
		"role":     "claimer",
		"username": c.username, // Using AlbertS03
	}
	
	err = ws.WriteJSON(regPayload)
	if err != nil {
		c.Disconnect()
		return false
	}

	ws.SetReadDeadline(time.Time{})

	c.lock.Lock()
	c.connected = true
	c.lastActivity = time.Now()
	c.lock.Unlock()

	log.Printf("[Client %d] Logged in as: %s", c.clientID, c.username)
	return true
}

func (c *StressClient) Disconnect() {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.connected {
		return
	}
	if c.ws != nil {
		c.ws.Close()
	}
	c.connected = false
}

func (c *StressClient) Run() {
	c.running = true
	workerSemaphore <- struct{}{}
	defer func() { <-workerSemaphore }()

	for c.running {
		c.lock.Lock()
		isConnected := c.connected
		c.lock.Unlock()

		if !isConnected {
			if !c.Connect() {
				time.Sleep(RECONNECT_DELAY)
				continue
			}
		}

		for {
			_, message, err := c.ws.ReadMessage()
			if err != nil {
				c.Disconnect()
				break
			}

			var data map[string]interface{}
			if err := json.Unmarshal(message, &data); err == nil {
				if data["type"] == "ping" {
					c.ws.WriteJSON(map[string]string{"type": "pong"})
				}

				if code, exists := data["code"]; exists {
					log.Printf("\n🔥 [LEAKED]: %v 🔥\n", code)
				}
				
				// Shutdown if authentication fails specifically
				if data["message"] == "Authentication failed" {
					log.Printf("🛑 BANNED/INVALID. Stopping...")
					c.running = false
					c.Disconnect()
					return
				}
			}
		}
		runtime.Gosched()
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	debug.SetMemoryLimit(850 * 1024 * 1024) 
	runtime.GOMAXPROCS(runtime.NumCPU())

	log.Println("========================================")
	log.Println(" KING-CLAIMER STEALTH GHOST ACTIVE ")
	log.Printf(" User: %s | Target: %s", TARGET_USERNAME, SERVER_URL)
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
	wg.Wait()
}
