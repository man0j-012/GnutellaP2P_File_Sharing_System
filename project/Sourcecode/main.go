package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/google/uuid" //  package installed: go get github.com/google/uuid
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// -----------------------------
// Constants and Types
// -----------------------------

// Message Types
const (
	MsgTypePeerID           = "peer_id"
	MsgTypeFileRegistration = "file_registration"
	MsgTypeFileQuery        = "file_query"
	MsgTypeQueryHit         = "query_hit"
)

// Peer Types
const (
	SuperPeerType = "super_peer"
	LeafNodeType  = "leaf_node"
)

// PeerIDMessage is used by peers to identify themselves
type PeerIDMessage struct {
	MessageType string `json:"message_type"`
	PeerType    string `json:"peer_type"`
	PeerID      string `json:"peer_id"`
}

// FileMetadata contains information about a shared file
type FileMetadata struct {
	FileName string `json:"file_name"`
	FileSize int64  `json:"file_size"`
}

// FileRegistrationMessage is sent by leaf-nodes to register their files
type FileRegistrationMessage struct {
	MessageType string         `json:"message_type"`
	LeafNodeID  string         `json:"leaf_node_id"`
	Files       []FileMetadata `json:"files"`
}

// FileQueryMessage is used to query for files
type FileQueryMessage struct {
	MessageType string `json:"message_type"`
	MessageID   string `json:"message_id"`
	OriginID    string `json:"origin_id"`
	FileName    string `json:"file_name"`
	TTL         int    `json:"ttl"`
}

// QueryHitMessage is sent in response to a FileQueryMessage
type QueryHitMessage struct {
	MessageType  string `json:"message_type"`
	MessageID    string `json:"message_id"`
	TTL          int    `json:"ttl"`
	RespondingID string `json:"responding_id"`
	FileName     string `json:"file_name"`
	Address      string `json:"address"`
	Port         int    `json:"port"`
}

// SuperPeerConfig contains configuration for a super-peer
type SuperPeerConfig struct {
	ID        string   `json:"id"`
	Address   string   `json:"address"`
	Port      int      `json:"port"`
	Neighbors []string `json:"neighbors"`
	LeafNodes []string `json:"leaf_nodes"`
}

// LeafNodeConfig contains configuration for a leaf-node
type LeafNodeConfig struct {
	ID        string `json:"id"`
	Address   string `json:"address"`
	Port      int    `json:"port"`
	SuperPeer string `json:"super_peer"`
}

// Config contains the overall configuration
type Config struct {
	SuperPeers []SuperPeerConfig `json:"super_peers"`
	LeafNodes  []LeafNodeConfig  `json:"leaf_nodes"`
}

// DownloadPrompt is used to prompt the user for downloading a file
type DownloadPrompt struct {
	QueryHitMessage QueryHitMessage
	ResponseChan    chan bool
}

// -----------------------------
// Configuration Loading
// -----------------------------

// LoadConfig loads configuration from a JSON file
func LoadConfig(filename string) (*Config, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

// -----------------------------
// SuperPeer Implementation
// -----------------------------

// SuperPeer represents a super-peer in the network
type SuperPeer struct {
	Config          SuperPeerConfig
	GlobalConfig    *Config
	NeighborConns   map[string]net.Conn
	NeighborConfigs map[string]SuperPeerConfig
	LeafNodeConns   map[string]net.Conn
	LeafNodeConfigs map[string]LeafNodeConfig
	FileIndex       map[string]map[string]struct{} // FileName -> LeafNodeID set
	MessageCache    map[string]CacheEntry
	mu              sync.Mutex
	connMu          sync.Mutex
}

// CacheEntry stores information about forwarded messages
type CacheEntry struct {
	OriginID     string
	UpstreamConn net.Conn
	Timestamp    time.Time
}

// NewSuperPeer creates a new SuperPeer instance
func NewSuperPeer(config SuperPeerConfig, globalConfig *Config) *SuperPeer {
	neighborConfigs := make(map[string]SuperPeerConfig)
	for _, neighborID := range config.Neighbors {
		for _, spConfig := range globalConfig.SuperPeers {
			if spConfig.ID == neighborID {
				neighborConfigs[neighborID] = spConfig
				break
			}
		}
	}

	leafNodeConfigs := make(map[string]LeafNodeConfig)
	for _, lnID := range config.LeafNodes {
		for _, lnConfig := range globalConfig.LeafNodes {
			if lnConfig.ID == lnID {
				leafNodeConfigs[lnID] = lnConfig
				break
			}
		}
	}

	// Initialize FileIndex with maps to ensure uniqueness
	fileIndex := make(map[string]map[string]struct{})
	return &SuperPeer{
		Config:          config,
		GlobalConfig:    globalConfig,
		NeighborConns:   make(map[string]net.Conn),
		NeighborConfigs: neighborConfigs,
		LeafNodeConns:   make(map[string]net.Conn),
		LeafNodeConfigs: leafNodeConfigs,
		FileIndex:       fileIndex,
		MessageCache:    make(map[string]CacheEntry),
	}
}

// Start initializes the super-peer and begins listening for connections
func (sp *SuperPeer) Start() {
	// Start listening for incoming connections
	address := fmt.Sprintf("%s:%d", sp.Config.Address, sp.Config.Port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Super-Peer %s failed to listen on %s: %v", sp.Config.ID, address, err)
	}
	log.Printf("Super-Peer %s listening on %s", sp.Config.ID, address)

	// Connect to neighbor super-peers
	for neighborID, neighborConfig := range sp.NeighborConfigs {
		go sp.connectToNeighbor(neighborID, neighborConfig)
	}

	// Start a goroutine to accept incoming connections
	go sp.acceptConnections(listener)

	// Start the message cache cleanup routine
	go sp.cleanupMessageCache()

	// Start logging FileIndex status (optional)
	go sp.logFileIndex()

	// Keep the main function running indefinitely
	select {}
}

// connectToNeighbor establishes a connection to a neighbor super-peer
func (sp *SuperPeer) connectToNeighbor(neighborID string, neighborConfig SuperPeerConfig) {
	address := fmt.Sprintf("%s:%d", neighborConfig.Address, neighborConfig.Port)
	for {
		conn, err := net.Dial("tcp", address)
		if err != nil {
			log.Printf("Super-Peer %s failed to connect to neighbor Super-Peer %s at %s: %v", sp.Config.ID, neighborID, address, err)
			time.Sleep(5 * time.Second) // Retry after delay
			continue
		}

		// Send identification message to the neighbor
		peerIDMsg := PeerIDMessage{
			MessageType: MsgTypePeerID,
			PeerType:    SuperPeerType,
			PeerID:      sp.Config.ID,
		}
		encoder := json.NewEncoder(conn)
		err = encoder.Encode(peerIDMsg)
		if err != nil {
			log.Printf("Super-Peer %s failed to send ID to neighbor Super-Peer %s: %v", sp.Config.ID, neighborID, err)
			conn.Close()
			time.Sleep(5 * time.Second) // Retry after delay
			continue
		}

		// Store the connection
		sp.mu.Lock()
		sp.NeighborConns[neighborID] = conn
		sp.mu.Unlock()

		// Handle communication with the neighbor
		go sp.handleNeighborConnection(conn, neighborID)
		break // Exit the loop upon successful connection
	}
}

// acceptConnections handles incoming connections to the super-peer
func (sp *SuperPeer) acceptConnections(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Super-Peer %s failed to accept connection: %v", sp.Config.ID, err)
			continue
		}

		// Handle the connection in a separate goroutine
		go sp.handleIncomingConnection(conn)
	}
}

// handleIncomingConnection processes an incoming connection (either Super-Peer or Leaf-Node)
func (sp *SuperPeer) handleIncomingConnection(conn net.Conn) {
	decoder := json.NewDecoder(conn)
	var msg PeerIDMessage
	err := decoder.Decode(&msg)
	if err != nil {
		log.Printf("Super-Peer %s failed to decode message from %s: %v",
			sp.Config.ID, conn.RemoteAddr().String(), err)
		conn.Close()
		return
	}

	if msg.MessageType == MsgTypePeerID {
		if msg.PeerType == LeafNodeType {
			log.Printf("Super-Peer %s accepted connection from Leaf-Node %s", sp.Config.ID, msg.PeerID)
			// Handle leaf-node connection
			go sp.handleLeafNodeConnection(conn, msg.PeerID)
		} else if msg.PeerType == SuperPeerType {
			log.Printf("Super-Peer %s accepted connection from Super-Peer %s", sp.Config.ID, msg.PeerID)
			sp.mu.Lock()
			sp.NeighborConns[msg.PeerID] = conn
			sp.mu.Unlock()
			// Handle super-peer connection
			go sp.handleNeighborConnection(conn, msg.PeerID)
		} else {
			log.Printf("Super-Peer %s received unknown peer type from %s", sp.Config.ID, conn.RemoteAddr().String())
			conn.Close()
		}
	} else {
		log.Printf("Super-Peer %s received unknown message type from %s", sp.Config.ID, conn.RemoteAddr().String())
		conn.Close()
	}
}

// handleNeighborConnection manages communication with a neighbor Super-Peer
func (sp *SuperPeer) handleNeighborConnection(conn net.Conn, neighborID string) {
	log.Printf("Super-Peer %s handling neighbor connection with Super-Peer %s", sp.Config.ID, neighborID)

	decoder := json.NewDecoder(conn)
	for {
		var msg map[string]interface{}
		err := decoder.Decode(&msg)
		if err != nil {
			if err == io.EOF {
				log.Printf("Neighbor Super-Peer %s disconnected", neighborID)
			} else {
				log.Printf("Error decoding message from Neighbor Super-Peer %s: %v", neighborID, err)
			}
			break
		}

		messageType, ok := msg["message_type"].(string)
		if !ok {
			log.Printf("Invalid message from Neighbor Super-Peer %s: missing message_type", neighborID)
			continue
		}

		switch messageType {
		case MsgTypeFileQuery:
			var queryMsg FileQueryMessage
			err := mapToStruct(msg, &queryMsg)
			if err != nil {
				log.Printf("Error decoding FileQueryMessage from Neighbor Super-Peer %s: %v", neighborID, err)
				continue
			}
			log.Printf("Super-Peer %s received FileQueryMessage for '%s' from %s", sp.Config.ID, queryMsg.FileName, queryMsg.OriginID)
			sp.handleFileQuery(queryMsg, conn)
		case MsgTypeQueryHit:
			var queryHitMsg QueryHitMessage
			err := mapToStruct(msg, &queryHitMsg)
			if err != nil {
				log.Printf("Error decoding QueryHitMessage from Neighbor Super-Peer %s: %v", neighborID, err)
				continue
			}
			sp.forwardQueryHit(queryHitMsg)
		default:
			log.Printf("Unknown message type '%s' from Neighbor Super-Peer %s", messageType, neighborID)
		}
	}

	// Remove the connection upon disconnection
	sp.mu.Lock()
	delete(sp.NeighborConns, neighborID)
	sp.mu.Unlock()
}

// handleLeafNodeConnection manages communication with a Leaf-Node
func (sp *SuperPeer) handleLeafNodeConnection(conn net.Conn, leafNodeID string) {
	log.Printf("Super-Peer %s handling connection with Leaf-Node %s", sp.Config.ID, leafNodeID)

	// Store the connection
	sp.mu.Lock()
	sp.LeafNodeConns[leafNodeID] = conn
	sp.mu.Unlock()

	decoder := json.NewDecoder(conn)
	for {
		var msg map[string]interface{}
		err := decoder.Decode(&msg)
		if err != nil {
			if err == io.EOF {
				log.Printf("Leaf-Node %s disconnected", leafNodeID)
			} else {
				log.Printf("Error decoding message from Leaf-Node %s: %v", leafNodeID, err)
			}
			break
		}

		messageType, ok := msg["message_type"].(string)
		if !ok {
			log.Printf("Invalid message from Leaf-Node %s: missing message_type", leafNodeID)
			continue
		}

		switch messageType {
		case MsgTypeFileRegistration:
			var registrationMsg FileRegistrationMessage
			err := mapToStruct(msg, &registrationMsg)
			if err != nil {
				log.Printf("Error decoding FileRegistrationMessage from Leaf-Node %s: %v", leafNodeID, err)
				continue
			}
			sp.handleFileRegistration(registrationMsg)
		case MsgTypeFileQuery:
			var queryMsg FileQueryMessage
			err := mapToStruct(msg, &queryMsg)
			if err != nil {
				log.Printf("Error decoding FileQueryMessage from Leaf-Node %s: %v", leafNodeID, err)
				continue
			}
			log.Printf("Super-Peer %s received FileQueryMessage for '%s' from %s", sp.Config.ID, queryMsg.FileName, queryMsg.OriginID)
			sp.handleFileQuery(queryMsg, conn)
		default:
			log.Printf("Unknown message type '%s' from Leaf-Node %s", messageType, leafNodeID)
		}
	}

	// Remove the connection upon disconnection
	sp.mu.Lock()
	delete(sp.LeafNodeConns, leafNodeID)
	sp.mu.Unlock()
}

// sendJSONMessage sends a JSON-encoded message to a connection
func (sp *SuperPeer) sendJSONMessage(conn net.Conn, msg interface{}) error {
	sp.connMu.Lock()
	defer sp.connMu.Unlock()
	encoder := json.NewEncoder(conn)
	return encoder.Encode(msg)
}

// handleFileRegistration registers files from a Leaf-Node, ensuring no duplicates
func (sp *SuperPeer) handleFileRegistration(msg FileRegistrationMessage) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	for _, file := range msg.Files {
		if sp.FileIndex[file.FileName] == nil {
			sp.FileIndex[file.FileName] = make(map[string]struct{})
		}

		if _, exists := sp.FileIndex[file.FileName][msg.LeafNodeID]; !exists {
			sp.FileIndex[file.FileName][msg.LeafNodeID] = struct{}{}
			log.Printf("Super-Peer %s registered file '%s' from Leaf-Node %s", sp.Config.ID, file.FileName, msg.LeafNodeID)
		} else {
			log.Printf("Super-Peer %s ignored duplicate registration of file '%s' from Leaf-Node %s", sp.Config.ID, file.FileName, msg.LeafNodeID)
		}
	}
}

// handleFileQuery processes a FileQueryMessage
func (sp *SuperPeer) handleFileQuery(msg FileQueryMessage, sourceConn net.Conn) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	// Check if the message ID is already processed
	if _, exists := sp.MessageCache[msg.MessageID]; exists {
		// Already processed, ignore
		return
	}

	// Store in MessageCache
	sp.MessageCache[msg.MessageID] = CacheEntry{
		OriginID:     msg.OriginID,
		UpstreamConn: sourceConn,
		Timestamp:    time.Now(),
	}

	// Check if the file exists in the local FileIndex
	leafNodeIDs, found := sp.FileIndex[msg.FileName]

	if found {
		// Send QueryHitMessages back to the originator, excluding the origin Leaf-Node
		for leafNodeID := range leafNodeIDs {
			if leafNodeID == msg.OriginID {
				// Skip the origin Leaf-Node
				continue
			}

			leafConfig, exists := sp.LeafNodeConfigs[leafNodeID]
			if !exists {
				continue
			}

			queryHitMsg := QueryHitMessage{
				MessageType:  MsgTypeQueryHit,
				MessageID:    msg.MessageID,
				TTL:          msg.TTL,
				RespondingID: leafNodeID,
				FileName:     msg.FileName,
				Address:      leafConfig.Address,
				Port:         leafConfig.Port,
			}

			err := sp.sendJSONMessage(sourceConn, queryHitMsg)
			if err != nil {
				log.Printf("Error sending QueryHitMessage to originator: %v", err)
			}
		}
	}

	// Forward the query to neighbors if TTL > 1
	if msg.TTL > 1 {
		msg.TTL--

		for neighborID, conn := range sp.NeighborConns {
			// Avoid sending back to the source if necessary
			if conn == sourceConn {
				continue
			}
			log.Printf("Super-Peer %s forwarding query for '%s' to neighbor Super-Peer %s", sp.Config.ID, msg.FileName, neighborID)
			err := sp.sendJSONMessage(conn, msg)
			if err != nil {
				log.Printf("Error forwarding query to neighbor Super-Peer %s: %v", neighborID, err)
			}
		}
	}
}

// forwardQueryHit forwards a QueryHitMessage to the originator
func (sp *SuperPeer) forwardQueryHit(msg QueryHitMessage) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	entry, exists := sp.MessageCache[msg.MessageID]
	if !exists {
		log.Printf("No origin connection found for MessageID %s", msg.MessageID)
		return
	}

	// Decrement TTL
	if msg.TTL > 1 {
		msg.TTL--
	}

	log.Printf("Super-Peer %s forwarding QueryHitMessage for MessageID %s to originator", sp.Config.ID, msg.MessageID)
	err := sp.sendJSONMessage(entry.UpstreamConn, msg)
	if err != nil {
		log.Printf("Error forwarding QueryHitMessage: %v", err)
	}
}

// cleanupMessageCache periodically removes old entries from MessageCache
func (sp *SuperPeer) cleanupMessageCache() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		sp.mu.Lock()
		for msgID, entry := range sp.MessageCache {
			if time.Since(entry.Timestamp) > 30*time.Minute {
				delete(sp.MessageCache, msgID)
				log.Printf("Super-Peer %s removed MessageID %s from MessageCache", sp.Config.ID, msgID)
			}
		}
		sp.mu.Unlock()
	}
}

// logFileIndex periodically logs the current FileIndex for debugging
func (sp *SuperPeer) logFileIndex() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		sp.mu.Lock()
		log.Printf("Super-Peer %s FileIndex Status:", sp.Config.ID)
		for file, leafNodes := range sp.FileIndex {
			leafList := []string{}
			for ln := range leafNodes {
				leafList = append(leafList, ln)
			}
			log.Printf("  File: '%s' -> Leaf-Nodes: %v", file, leafList)
		}
		sp.mu.Unlock()
	}
}

// -----------------------------
// LeafNode Implementation
// -----------------------------

// LeafNode represents a leaf-node in the network
type LeafNode struct {
	Config             LeafNodeConfig
	SuperPeerConfig    SuperPeerConfig
	conn               net.Conn
	connMu             sync.Mutex // Mutex to protect writes to connection
	mu                 sync.Mutex
	responseTimes      []time.Duration
	startTimes         map[string]time.Time
	downloadPromptChan chan DownloadPrompt
}

// NewLeafNode creates a new LeafNode instance
func NewLeafNode(config LeafNodeConfig, globalConfig *Config) *LeafNode {
	var superPeerConfig SuperPeerConfig
	found := false
	for _, spConfig := range globalConfig.SuperPeers {
		if spConfig.ID == config.SuperPeer {
			superPeerConfig = spConfig
			found = true
			break
		}
	}
	if !found {
		log.Fatalf("Super-Peer ID %s for Leaf-Node %s not found in configuration", config.SuperPeer, config.ID)
	}

	return &LeafNode{
		Config:             config,
		SuperPeerConfig:    superPeerConfig,
		responseTimes:      []time.Duration{},
		startTimes:         make(map[string]time.Time),
		downloadPromptChan: make(chan DownloadPrompt),
	}
}

// Start initializes the leaf-node and connects to its super-peer
func (ln *LeafNode) Start() {
	// Specify the shared directory
	sharedDir := "./shared_files/" + ln.Config.ID

	// Discover shared files
	files, err := ln.discoverFiles(sharedDir)
	if err != nil {
		log.Fatalf("Leaf-Node %s failed to discover files: %v", ln.Config.ID, err)
	}

	log.Printf("Leaf-Node %s discovered %d files", ln.Config.ID, len(files))

	// Start the file server
	ln.startFileServer()

	// Connect to the super-peer with retry logic
	address := fmt.Sprintf("%s:%d", ln.SuperPeerConfig.Address, ln.SuperPeerConfig.Port)
	var conn net.Conn
	for {
		conn, err = net.Dial("tcp", address)
		if err != nil {
			log.Printf("Leaf-Node %s failed to connect to Super-Peer at %s: %v", ln.Config.ID, address, err)
			time.Sleep(5 * time.Second) // Retry after delay
			continue
		}
		break // Exit the loop upon successful connection
	}

	// Store the connection
	ln.conn = conn

	// Send identification message to the super-peer
	peerIDMsg := PeerIDMessage{
		MessageType: MsgTypePeerID,
		PeerType:    LeafNodeType,
		PeerID:      ln.Config.ID,
	}
	err = ln.sendJSONMessage(peerIDMsg)
	if err != nil {
		log.Fatalf("Leaf-Node %s failed to send ID to Super-Peer: %v", ln.Config.ID, err)
	}

	log.Printf("Leaf-Node %s connected to Super-Peer at %s", ln.Config.ID, address)

	// Send file registration message
	registrationMsg := FileRegistrationMessage{
		MessageType: MsgTypeFileRegistration,
		LeafNodeID:  ln.Config.ID,
		Files:       files,
	}
	err = ln.sendJSONMessage(registrationMsg)
	if err != nil {
		log.Fatalf("Leaf-Node %s failed to send file registration to Super-Peer: %v", ln.Config.ID, err)
	}

	// Handle communication with the super-peer
	go ln.handleSuperPeerConnection(conn)

	// Start the user interface
	ln.startUserInterface()
}

// discoverFiles scans the shared directory and returns metadata of shared files
func (ln *LeafNode) discoverFiles(sharedDir string) ([]FileMetadata, error) {
	var files []FileMetadata

	err := filepath.Walk(sharedDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, FileMetadata{
				FileName: info.Name(),
				FileSize: info.Size(),
			})
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return files, nil
}

// startFileServer starts an HTTP server to serve shared files
func (ln *LeafNode) startFileServer() {
	sharedDir := "./shared_files/" + ln.Config.ID
	fs := http.FileServer(http.Dir(sharedDir))
	http.Handle("/", fs)

	address := fmt.Sprintf("%s:%d", ln.Config.Address, ln.Config.Port)
	log.Printf("Leaf-Node %s starting file server at %s", ln.Config.ID, address)
	go func() {
		if err := http.ListenAndServe(address, nil); err != nil {
			log.Fatalf("Leaf-Node %s file server error: %v", ln.Config.ID, err)
		}
	}()
}

// handleSuperPeerConnection manages incoming messages from the super-peer
func (ln *LeafNode) handleSuperPeerConnection(conn net.Conn) {
	decoder := json.NewDecoder(conn)
	for {
		var msg map[string]interface{}
		err := decoder.Decode(&msg)
		if err != nil {
			if err == io.EOF {
				log.Printf("Super-Peer disconnected")
			} else {
				log.Printf("Error decoding message from Super-Peer: %v", err)
			}
			break
		}

		messageType, ok := msg["message_type"].(string)
		if !ok {
			log.Printf("Invalid message from Super-Peer: missing message_type")
			continue
		}

		switch messageType {
		case MsgTypeQueryHit:
			// Decode the message as QueryHitMessage
			var queryHitMsg QueryHitMessage
			err := mapToStruct(msg, &queryHitMsg)
			if err != nil {
				log.Printf("Error decoding QueryHitMessage: %v", err)
				continue
			}
			go ln.handleQueryHit(queryHitMsg)
		default:
			log.Printf("Unknown message type '%s' from Super-Peer", messageType)
		}
	}
}

// startUserInterface handles user inputs and download prompts
func (ln *LeafNode) startUserInterface() {
	reader := bufio.NewReader(os.Stdin)
	for {
		select {
		case prompt := <-ln.downloadPromptChan:
			// Handle download prompt
			fmt.Printf("\nQuery Hit: File '%s' is available at Leaf-Node %s (%s:%d)\n",
				prompt.QueryHitMessage.FileName, prompt.QueryHitMessage.RespondingID,
				prompt.QueryHitMessage.Address, prompt.QueryHitMessage.Port)
			fmt.Printf("Do you want to download this file? (yes/no): ")
			response, _ := reader.ReadString('\n')
			response = strings.TrimSpace(strings.ToLower(response))
			if response == "yes" {
				ln.downloadFile(prompt.QueryHitMessage)
				prompt.ResponseChan <- true
			} else {
				prompt.ResponseChan <- false
			}
		default:
			// Prompt user for file search
			fmt.Printf("\nEnter file name to search (or 'exit' to quit): ")
			fileName, _ := reader.ReadString('\n')
			fileName = strings.TrimSpace(fileName)

			if fileName == "exit" {
				fmt.Println("Exiting...")
				ln.conn.Close()
				os.Exit(0)
			}

			if fileName != "" {
				ln.sendFileQuery(fileName)
			}
		}
	}
}

// sendFileQuery sends a file query to the super-peer
func (ln *LeafNode) sendFileQuery(fileName string) {
	messageID := ln.generateMessageID()
	queryMsg := FileQueryMessage{
		MessageType: MsgTypeFileQuery,
		MessageID:   messageID,
		OriginID:    ln.Config.ID,
		FileName:    fileName,
		TTL:         5, // Set an appropriate TTL value
	}

	// Record the start time
	startTime := time.Now()

	// Store the start time associated with the MessageID
	ln.mu.Lock()
	ln.startTimes[messageID] = startTime
	ln.mu.Unlock()

	err := ln.sendJSONMessage(queryMsg)
	if err != nil {
		log.Printf("Leaf-Node %s failed to send file query: %v", ln.Config.ID, err)
		return
	}

	log.Printf("Leaf-Node %s sent file query for '%s' with MessageID %s", ln.Config.ID, fileName, messageID)

	// Optionally, print the query issued in a nicely formatted manner
	fmt.Printf("Issued Query: Looking for file '%s' with MessageID %s\n", fileName, messageID)
}

// sendJSONMessage sends a JSON-encoded message to the super-peer
func (ln *LeafNode) sendJSONMessage(msg interface{}) error {
	ln.connMu.Lock()
	defer ln.connMu.Unlock()
	encoder := json.NewEncoder(ln.conn)
	return encoder.Encode(msg)
}

// generateMessageID generates a unique MessageID for each query using UUID
func (ln *LeafNode) generateMessageID() string {
	return fmt.Sprintf("%s-%s", ln.Config.ID, uuid.New().String())
}

// handleQueryHit processes a QueryHitMessage
func (ln *LeafNode) handleQueryHit(msg QueryHitMessage) {
	// Record the response time
	endTime := time.Now()

	ln.mu.Lock()
	startTime, exists := ln.startTimes[msg.MessageID]
	if exists {
		responseTime := endTime.Sub(startTime)
		ln.responseTimes = append(ln.responseTimes, responseTime)
		// Remove the startTime as it's no longer needed
		delete(ln.startTimes, msg.MessageID)
		log.Printf("Response time for MessageID %s: %v", msg.MessageID, responseTime)
	}
	ln.mu.Unlock()

	// Prepare to prompt the user
	responseChan := make(chan bool)
	prompt := DownloadPrompt{
		QueryHitMessage: msg,
		ResponseChan:    responseChan,
	}

	// Send the prompt to the main input loop
	ln.downloadPromptChan <- prompt
}

// downloadFile downloads the specified file from the responding leaf-node
func (ln *LeafNode) downloadFile(msg QueryHitMessage) {
	url := fmt.Sprintf("http://%s:%d/%s", msg.Address, msg.Port, msg.FileName)
	log.Printf("Downloading file from %s", url)

	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Error downloading file: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Failed to download file: %s", resp.Status)
		return
	}

	// Save the file to the local shared directory
	sharedDir := "./shared_files/" + ln.Config.ID
	filePath := filepath.Join(sharedDir, msg.FileName)

	// Check if the file already exists to prevent re-registration
	if _, err := os.Stat(filePath); err == nil {
		log.Printf("File '%s' already exists. Skipping download.", msg.FileName)
		return
	}

	outFile, err := os.Create(filePath)
	if err != nil {
		log.Printf("Error creating file: %v", err)
		return
	}
	defer outFile.Close()

	_, err = io.Copy(outFile, resp.Body)
	if err != nil {
		log.Printf("Error saving file: %v", err)
		return
	}

	// Display the downloaded file message as per requirement
	fmt.Printf("display file '%s'\n", msg.FileName)

	log.Printf("File '%s' downloaded successfully", msg.FileName)

	// Re-register the new file only if it's newly downloaded
	newFile := FileMetadata{
		FileName: msg.FileName,
		FileSize: getFileSize(filePath),
	}

	registrationMsg := FileRegistrationMessage{
		MessageType: MsgTypeFileRegistration,
		LeafNodeID:  ln.Config.ID,
		Files:       []FileMetadata{newFile},
	}

	err = ln.sendJSONMessage(registrationMsg)
	if err != nil {
		log.Printf("Error re-registering file: %v", err)
	}
}

// getFileSize returns the size of the file at the given path
func getFileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

// -----------------------------
// Helper Functions
// -----------------------------

// mapToStruct converts a map to a struct using JSON marshalling
func mapToStruct(m map[string]interface{}, result interface{}) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, result)
}

// -----------------------------
// Main Function
// -----------------------------

func main() {
	// Check if the config file path and peer ID are provided
	if len(os.Args) < 3 {
		log.Fatal("Usage: go run main.go [config file] [peer ID]")
	}

	// Get the config file path and peer ID from the command-line arguments
	configFile := os.Args[1]
	peerID := os.Args[2]

	// Load the configuration
	config, err := LoadConfig(configFile)
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	// Determine if the peer is a super-peer or leaf-node
	isSuperPeer := false
	var superPeerConfig SuperPeerConfig
	var leafNodeConfig LeafNodeConfig

	// Search for the peer ID in the super-peers list
	for _, sp := range config.SuperPeers {
		if sp.ID == peerID {
			isSuperPeer = true
			superPeerConfig = sp
			break
		}
	}

	if !isSuperPeer {
		// If not found in super-peers, search in the leaf-nodes list
		found := false
		for _, ln := range config.LeafNodes {
			if ln.ID == peerID {
				leafNodeConfig = ln
				found = true
				break
			}
		}
		if !found {
			log.Fatalf("Peer ID %s not found in configuration", peerID)
		}
	}

	// Initialize the peer based on its role
	if isSuperPeer {
		fmt.Printf("Starting Super-Peer %s\n", superPeerConfig.ID)
		// Initialize and start the super-peer
		sp := NewSuperPeer(superPeerConfig, config)
		sp.Start()
	} else {
		fmt.Printf("Starting Leaf-Node %s\n", leafNodeConfig.ID)
		// Initialize and start the leaf-node
		ln := NewLeafNode(leafNodeConfig, config)
		ln.Start()
	}
}
