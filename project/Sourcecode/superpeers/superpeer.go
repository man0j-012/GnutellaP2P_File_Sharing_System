// superpeer.go
package main

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "net"
    "strings"
    "sync"
    "time"

    "github.com/google/uuid"
)
// Message Types as constants
const (
    MsgTypePeerID           = "peer_id"
    MsgTypeFileRegistration = "file_registration"
    MsgTypeInvalidation     = "invalidation"
    MsgTypePollRequest      = "poll_request"
    MsgTypePollResponse     = "poll_response"
    MsgTypeRefreshRequest   = "refresh_request"
    MsgTypeRefreshResponse  = "refresh_response"
    MsgTypeQuery            = "query"
    MsgTypeQueryResponse    = "query_response"
)

// SuperPeerConfig represents the configuration for a SuperPeer
type SuperPeerConfig struct {
    ID          string   `json:"id"`
    Address     string   `json:"address"`
    Port        int      `json:"port"`
    Neighbors   []string `json:"neighbors"`
    LeafNodes   []string `json:"leaf_nodes"`
    EnablePush  bool     `json:"enable_push"`
    EnablePull  bool     `json:"enable_pull"`
}

// SuperPeer represents a SuperPeer in the network
type SuperPeer struct {
    Config          SuperPeerConfig
    Listener        net.Listener
    LeafNodeConns   map[string]net.Conn               // LeafNodeID -> Connection
    LeafNodeConnsMu sync.RWMutex                      // Mutex for LeafNodeConns
    Neighbors       map[string]net.Conn               // NeighborID -> Connection
    NeighborsMu     sync.RWMutex                      // Mutex for Neighbors
    Files           map[string]SuperPeerFileMetadata  // FileName -> Metadata
    FilesMu         sync.RWMutex                      // Mutex for Files
}

// SuperPeerFileMetadata contains metadata about a file for SuperPeers
type SuperPeerFileMetadata struct {
    VersionNumber  int    `json:"version_number"`
    OriginServerID string `json:"origin_server_id"`
    LastModified   string `json:"last_modified_time"`
}

// NewSuperPeer initializes a new SuperPeer instance
func NewSuperPeer(config SuperPeerConfig) *SuperPeer {
    return &SuperPeer{
        Config:        config,
        LeafNodeConns: make(map[string]net.Conn),
        Neighbors:     make(map[string]net.Conn),
        Files:         make(map[string]SuperPeerFileMetadata),
    }
}

// Start launches the SuperPeer to listen for incoming connections and connect to neighbors
func (sp *SuperPeer) Start(wg *sync.WaitGroup) {
    defer wg.Done()

    // Start listening for incoming connections
    address := fmt.Sprintf("%s:%d", sp.Config.Address, sp.Config.Port)
    listener, err := net.Listen("tcp", address)
    if err != nil {
        log.Fatalf("SuperPeer %s: Failed to listen on %s: %v", sp.Config.ID, address, err)
    }
    sp.Listener = listener
    log.Printf("SuperPeer %s: Listening on %s", sp.Config.ID, address)

    // Connect to neighbor SuperPeers
    go sp.connectToNeighbors()

    // Accept incoming connections
    go sp.acceptConnections()

    // Keep the main goroutine alive
    select {}
}

// Helper function to extract numerical ID from SuperPeer ID
func extractIDNumber(id string) int {
    var num int
    fmt.Sscanf(id, "SP%d", &num)
    return num
}

// connectToNeighbors attempts to connect to all neighboring SuperPeers
func (sp *SuperPeer) connectToNeighbors() {
    myIDNum := extractIDNumber(sp.Config.ID)
    for _, neighborID := range sp.Config.Neighbors {
        if neighborID == sp.Config.ID {
            continue
        }
        neighborIDNum := extractIDNumber(neighborID)
        if myIDNum >= neighborIDNum {
            continue // Only connect to neighbors with higher IDs
        }

        neighborPort := getSuperPeerPort(neighborID)
        neighborAddress := fmt.Sprintf("127.0.0.1:%d", neighborPort)

        // Attempt connection with retry mechanism
        go func(neighborID, neighborAddress string) {
            for {
                conn, err := net.Dial("tcp", neighborAddress)
                if err != nil {
                    log.Printf("SuperPeer %s: Failed to connect to neighbor %s at %s. Retrying in 5 seconds...", sp.Config.ID, neighborID, neighborAddress)
                    time.Sleep(5 * time.Second)
                    continue
                }

                peerIDMsg := PeerIDMessage{
                    MessageType: MsgTypePeerID,
                    PeerType:    SuperPeerType,
                    PeerID:      sp.Config.ID,
                }
                err = sendJSONMessage(conn, peerIDMsg)
                if err != nil {
                    log.Printf("SuperPeer %s: Error sending PeerIDMessage to neighbor %s", sp.Config.ID, neighborID)
                    conn.Close()
                    time.Sleep(5 * time.Second)
                    continue
                }

                // Receive and verify PeerIDMessage from neighbor
                var neighborPeerIDMsg PeerIDMessage
                decoder := json.NewDecoder(conn)
                err = decoder.Decode(&neighborPeerIDMsg)
                if err != nil || neighborPeerIDMsg.MessageType != MsgTypePeerID || neighborPeerIDMsg.PeerType != SuperPeerType {
                    log.Printf("SuperPeer %s: Failed to validate neighbor %s via PeerIDMessage", sp.Config.ID, neighborID)
                    conn.Close()
                    time.Sleep(5 * time.Second)
                    continue
                }

                // Add the connection to neighbors map
                sp.NeighborsMu.Lock()
                sp.Neighbors[neighborID] = conn
                sp.NeighborsMu.Unlock()

                log.Printf("SuperPeer %s: Connected to neighbor %s at %s", sp.Config.ID, neighborID, neighborAddress)
                go sp.handleNeighborSuperPeerConnection(neighborID, conn)
                break
            }
        }(neighborID, neighborAddress)
    }
}

// acceptConnections listens for incoming connections and handles them
func (sp *SuperPeer) acceptConnections() {
    for {
        conn, err := sp.Listener.Accept()
        if err != nil {
            log.Printf("SuperPeer %s: Error accepting connection: %v", sp.Config.ID, err)
            continue
        }

        // Handle the connection in a separate goroutine
        go sp.handleIncomingConnection(conn)
    }
}

// handleIncomingConnection identifies the type of peer and handles accordingly
func (sp *SuperPeer) handleIncomingConnection(conn net.Conn) {
    // First message should be PeerIDMessage
    var peerMsg PeerIDMessage
    decoder := json.NewDecoder(conn)
    err := decoder.Decode(&peerMsg)
    if err != nil {
        log.Printf("SuperPeer %s: Failed to decode PeerIDMessage: %v", sp.Config.ID, err)
        conn.Close()
        return
    }

    if peerMsg.MessageType != MsgTypePeerID {
        log.Printf("SuperPeer %s: Expected PeerIDMessage, got %s", sp.Config.ID, peerMsg.MessageType)
        conn.Close()
        return
    }

    if peerMsg.PeerType == LeafNodeType {
        // Handle LeafNode connection
        sp.handleLeafNodeConnection(peerMsg.PeerID, conn, decoder)
    } else if peerMsg.PeerType == SuperPeerType {
        // Send our own PeerIDMessage back to the connecting SuperPeer
        ourPeerIDMsg := PeerIDMessage{
            MessageType: MsgTypePeerID,
            PeerType:    SuperPeerType,
            PeerID:      sp.Config.ID,
        }
        err = sendJSONMessage(conn, ourPeerIDMsg)
        if err != nil {
            log.Printf("SuperPeer %s: Error sending PeerIDMessage to connecting SuperPeer %s: %v", sp.Config.ID, peerMsg.PeerID, err)
            conn.Close()
            return
        }

        // Add the connection to neighbors map
        sp.NeighborsMu.Lock()
        sp.Neighbors[peerMsg.PeerID] = conn
        sp.NeighborsMu.Unlock()

        log.Printf("SuperPeer %s: Accepted connection from neighbor SuperPeer %s", sp.Config.ID, peerMsg.PeerID)
        sp.handleNeighborSuperPeerConnection(peerMsg.PeerID, conn)
    } else {
        log.Printf("SuperPeer %s: Unknown PeerType '%s' from PeerID '%s'", sp.Config.ID, peerMsg.PeerType, peerMsg.PeerID)
        conn.Close()
    }
}

// handleLeafNodeConnection manages communication with a connected LeafNode
func (sp *SuperPeer) handleLeafNodeConnection(leafID string, conn net.Conn, decoder *json.Decoder) {
    sp.LeafNodeConnsMu.Lock()
    sp.LeafNodeConns[leafID] = conn
    sp.LeafNodeConnsMu.Unlock()
    log.Printf("SuperPeer %s: Connected to LeafNode %s", sp.Config.ID, leafID)

    // Listen for messages from LeafNode
    for {
        var msgMap map[string]interface{}
        err := decoder.Decode(&msgMap)
        if err != nil {
            log.Printf("SuperPeer %s: Connection to LeafNode %s lost: %v", sp.Config.ID, leafID, err)
            sp.LeafNodeConnsMu.Lock()
            delete(sp.LeafNodeConns, leafID)
            sp.LeafNodeConnsMu.Unlock()
            conn.Close()
            return
        }

        messageType, ok := msgMap["message_type"].(string)
        if !ok {
            log.Printf("SuperPeer %s: Received message without message_type from LeafNode %s", sp.Config.ID, leafID)
            continue
        }

        // Trim whitespace from messageType
        messageType = strings.TrimSpace(messageType)

        // Log the received message_type for debugging
        log.Printf("SuperPeer %s: Received message_type '%s' from LeafNode %s", sp.Config.ID, messageType, leafID)

        switch messageType {
        case MsgTypeFileRegistration:
            var regMsg FileRegistrationMessage
            err := mapToStruct(msgMap, &regMsg)
            if err != nil {
                log.Printf("SuperPeer %s: Error parsing FileRegistrationMessage from LeafNode %s: %v", sp.Config.ID, leafID, err)
                continue
            }
            sp.handleFileRegistration(regMsg, leafID)
        case MsgTypeInvalidation:
            var invMsg InvalidationMessage
            err := mapToStruct(msgMap, &invMsg)
            if err != nil {
                log.Printf("SuperPeer %s: Error parsing InvalidationMessage from LeafNode %s: %v", sp.Config.ID, leafID, err)
                continue
            }
            if sp.Config.EnablePush {
                sp.handleInvalidation(invMsg, leafID)
            }
        case MsgTypePollRequest:
            var pollReq PollRequestMessage
            err := mapToStruct(msgMap, &pollReq)
            if err != nil {
                log.Printf("SuperPeer %s: Error parsing PollRequestMessage from LeafNode %s: %v", sp.Config.ID, leafID, err)
                continue
            }
            if sp.Config.EnablePull {
                sp.handlePollRequest(pollReq, leafID)
            }
        case MsgTypeRefreshRequest:
            var refreshReq RefreshRequestMessage
            err := mapToStruct(msgMap, &refreshReq)
            if err != nil {
                log.Printf("SuperPeer %s: Error parsing RefreshRequestMessage from LeafNode %s: %v", sp.Config.ID, leafID, err)
                continue
            }
            sp.handleRefreshRequest(refreshReq, leafID)
        case MsgTypeQuery:
            var queryMsg QueryMessage
            err := mapToStruct(msgMap, &queryMsg)
            if err != nil {
                log.Printf("SuperPeer %s: Error parsing QueryMessage from LeafNode %s: %v", sp.Config.ID, leafID, err)
                continue
            }
            sp.handleQuery(queryMsg, leafID)
        default:
            log.Printf("SuperPeer %s: Unknown message_type '%s' from LeafNode %s", sp.Config.ID, messageType, leafID)
        }
    }
}

// handleNeighborSuperPeerConnection manages communication with a connected neighbor SuperPeer
func (sp *SuperPeer) handleNeighborSuperPeerConnection(neighborID string, conn net.Conn) {
    log.Printf("SuperPeer %s: Handling communication with neighbor SuperPeer %s", sp.Config.ID, neighborID)

    decoder := json.NewDecoder(conn)

    // Listen for messages from Neighbor SuperPeer
    for {
        var msgMap map[string]interface{}
        err := decoder.Decode(&msgMap)
        if err != nil {
            log.Printf("SuperPeer %s: Connection to neighbor SuperPeer %s lost: %v", sp.Config.ID, neighborID, err)
            sp.NeighborsMu.Lock()
            delete(sp.Neighbors, neighborID)
            sp.NeighborsMu.Unlock()
            conn.Close()
            return
        }

        messageType, ok := msgMap["message_type"].(string)
        if !ok {
            log.Printf("SuperPeer %s: Received message without message_type from neighbor SuperPeer %s", sp.Config.ID, neighborID)
            continue
        }

        // Trim whitespace from messageType
        messageType = strings.TrimSpace(messageType)

        // Log the received message_type for debugging
        log.Printf("SuperPeer %s: Received message_type '%s' from neighbor SuperPeer %s", sp.Config.ID, messageType, neighborID)

        switch messageType {
        case MsgTypeInvalidation:
            var invMsg InvalidationMessage
            err := mapToStruct(msgMap, &invMsg)
            if err != nil {
                log.Printf("SuperPeer %s: Error parsing InvalidationMessage from neighbor SuperPeer %s: %v", sp.Config.ID, neighborID, err)
                continue
            }
            if sp.Config.EnablePush {
                sp.handleInvalidation(invMsg, neighborID)
            }
        case MsgTypePollRequest:
            var pollReq PollRequestMessage
            err := mapToStruct(msgMap, &pollReq)
            if err != nil {
                log.Printf("SuperPeer %s: Error parsing PollRequestMessage from neighbor SuperPeer %s: %v", sp.Config.ID, neighborID, err)
                continue
            }
            if sp.Config.EnablePull {
                sp.handlePollRequest(pollReq, neighborID)
            }
        case MsgTypeRefreshRequest:
            var refreshReq RefreshRequestMessage
            err := mapToStruct(msgMap, &refreshReq)
            if err != nil {
                log.Printf("SuperPeer %s: Error parsing RefreshRequestMessage from neighbor SuperPeer %s: %v", sp.Config.ID, neighborID, err)
                continue
            }
            sp.handleRefreshRequest(refreshReq, neighborID)
        case MsgTypeQuery:
            var queryMsg QueryMessage
            err := mapToStruct(msgMap, &queryMsg)
            if err != nil {
                log.Printf("SuperPeer %s: Error parsing QueryMessage from neighbor SuperPeer %s: %v", sp.Config.ID, neighborID, err)
                continue
            }
            sp.handleQuery(queryMsg, neighborID)
        case MsgTypePeerID:
            // Handle redundant PeerIDMessage
            log.Printf("SuperPeer %s: Received redundant PeerIDMessage from neighbor SuperPeer %s. Ignoring.", sp.Config.ID, neighborID)
        default:
            log.Printf("SuperPeer %s: Unknown message_type '%s' from neighbor SuperPeer %s. Full message: %+v", sp.Config.ID, messageType, neighborID, msgMap)
        }
    }
}


// handleFileRegistration processes a FileRegistrationMessage from a LeafNode
func (sp *SuperPeer) handleFileRegistration(regMsg FileRegistrationMessage, leafID string) {
    sp.FilesMu.Lock()
    defer sp.FilesMu.Unlock()

    for _, file := range regMsg.Files {
        if existingFile, exists := sp.Files[file.FileName]; exists {
            // If file already exists, you might want to handle duplicates or versioning
            log.Printf("SuperPeer %s: File '%s' already registered by SuperPeer %s. Ignoring duplicate registration from LeafNode %s.", sp.Config.ID, file.FileName, existingFile.OriginServerID, leafID)
            continue
        }

        sp.Files[file.FileName] = SuperPeerFileMetadata{
            VersionNumber:  1,
            OriginServerID: regMsg.LeafNodeID,
            LastModified:   time.Now().Format(time.RFC3339),
        }

        log.Printf("SuperPeer %s: Registered new file '%s' from LeafNode %s", sp.Config.ID, file.FileName, leafID)
    }
}

// handleInvalidation processes an InvalidationMessage from a LeafNode or neighbor SuperPeer
func (sp *SuperPeer) handleInvalidation(invMsg InvalidationMessage, sourceID string) {
    sp.FilesMu.Lock()
    defer sp.FilesMu.Unlock()

    fileMeta, exists := sp.Files[invMsg.FileName]
    if !exists {
        // If file doesn't exist, possibly add it or ignore
        log.Printf("SuperPeer %s: Received Invalidation for unknown file '%s' from %s. Ignoring.", sp.Config.ID, invMsg.FileName, sourceID)
        return
    }

    // Validate that the invalidation is coming from the registered origin server
    if invMsg.OriginServerID != fileMeta.OriginServerID {
        log.Printf("SuperPeer %s: Received invalidation for file '%s' from unknown Origin Server '%s'. Expected Origin Server: '%s'. Ignoring.",
            sp.Config.ID, invMsg.FileName, invMsg.OriginServerID, fileMeta.OriginServerID)
        return
    }

    if invMsg.VersionNumber > fileMeta.VersionNumber {
        // Update file metadata
        fileMeta.VersionNumber = invMsg.VersionNumber
        fileMeta.LastModified = time.Now().Format(time.RFC3339)
        sp.Files[invMsg.FileName] = fileMeta

        log.Printf("SuperPeer %s: Updated file '%s' to version %d based on Invalidation from %s",
            sp.Config.ID, invMsg.FileName, invMsg.VersionNumber, sourceID)

        // Notify all connected LeafNodes to invalidate their cached copies
        sp.notifyLeafNodesInvalidation(invMsg.FileName)

        // Optionally, notify neighbor SuperPeers
        if sp.Config.EnablePush {
            sp.notifyNeighborSuperPeersInvalidation(invMsg.FileName)
        }
    } else {
        log.Printf("SuperPeer %s: Received outdated Invalidation for file '%s' (Version: %d). Current Version: %d. Ignoring.",
            sp.Config.ID, invMsg.FileName, invMsg.VersionNumber, fileMeta.VersionNumber)
    }
}


// notifyLeafNodesInvalidation sends an InvalidationMessage to all connected LeafNodes
func (sp *SuperPeer) notifyLeafNodesInvalidation(fileName string) {
    sp.LeafNodeConnsMu.RLock()
    defer sp.LeafNodeConnsMu.RUnlock()

    invMsg := InvalidationMessage{
        MessageType:    MsgTypeInvalidation,
        MsgID:          uuid.New().String(),
        OriginServerID: sp.Config.ID,
        FileName:       fileName,
        VersionNumber:  sp.Files[fileName].VersionNumber,
    }

    for leafID, conn := range sp.LeafNodeConns {
        err := sendJSONMessage(conn, invMsg)
        if err != nil {
            log.Printf("SuperPeer %s: Error sending InvalidationMessage to LeafNode %s: %v", sp.Config.ID, leafID, err)
        } else {
            log.Printf("SuperPeer %s: Sent InvalidationMessage for '%s' to LeafNode %s", sp.Config.ID, fileName, leafID)
        }
    }
}

// handlePollRequest processes a PollRequestMessage from a LeafNode or neighbor SuperPeer
func (sp *SuperPeer) handlePollRequest(pollReq PollRequestMessage, sourceID string) {
    sp.FilesMu.RLock()
    fileMeta, exists := sp.Files[pollReq.FileName]
    sp.FilesMu.RUnlock()

    pollResp := PollResponseMessage{
        MessageType: MsgTypePollResponse,
        MessageID:   uuid.New().String(),
        FileName:    pollReq.FileName,
    }

    if !exists {
        // File does not exist
        pollResp.Status = "invalid"
        pollResp.NewVersionNumber = -1
    } else if fileMeta.VersionNumber > pollReq.CurrentVersionNumber {
        // File is outdated
        pollResp.Status = "invalid"
        pollResp.NewVersionNumber = fileMeta.VersionNumber
    } else {
        // File is still valid
        pollResp.Status = "valid"
        pollResp.NewVersionNumber = fileMeta.VersionNumber
    }

    // Determine where to send the PollResponse
    if sp.isLeafNodeConnected(sourceID) {
        // Source is a LeafNode
        sp.LeafNodeConnsMu.RLock()
        conn, exists := sp.LeafNodeConns[sourceID]
        sp.LeafNodeConnsMu.RUnlock()
        if exists {
            err := sendJSONMessage(conn, pollResp)
            if err != nil {
                log.Printf("SuperPeer %s: Error sending PollResponse to LeafNode %s: %v", sp.Config.ID, sourceID, err)
            } else {
                log.Printf("SuperPeer %s: Sent PollResponse for '%s' to LeafNode %s", sp.Config.ID, pollReq.FileName, sourceID)
            }
        }
    } else {
        // Source is a neighbor SuperPeer
        sp.NeighborsMu.RLock()
        conn, exists := sp.Neighbors[sourceID]
        sp.NeighborsMu.RUnlock()
        if exists {
            err := sendJSONMessage(conn, pollResp)
            if err != nil {
                log.Printf("SuperPeer %s: Error sending PollResponse to neighbor SuperPeer %s: %v", sp.Config.ID, sourceID, err)
            } else {
                log.Printf("SuperPeer %s: Sent PollResponse for '%s' to neighbor SuperPeer %s", sp.Config.ID, pollReq.FileName, sourceID)
            }
        }
    }
}

// handleRefreshRequest processes a RefreshRequestMessage from a LeafNode or neighbor SuperPeer
func (sp *SuperPeer) handleRefreshRequest(refreshReq RefreshRequestMessage, sourceID string) {
    sp.FilesMu.RLock()
    fileMeta, exists := sp.Files[refreshReq.FileName]
    sp.FilesMu.RUnlock()

    refreshResp := RefreshResponseMessage{
        MessageType: MsgTypeRefreshResponse,
        MessageID:   uuid.New().String(),
        FileName:    refreshReq.FileName,
    }

    if !exists {
        // File does not exist on SuperPeer
        refreshResp.VersionNumber = -1
    } else {
        // Read the latest file data from the origin server (LeafNode)
        // Assuming the origin server (LeafNode) maintains the latest file data
        // For simplicity, we'll simulate fetching the file data
        filePath := fmt.Sprintf("./leafnode_shared/%s/owned/%s", fileMeta.OriginServerID, refreshReq.FileName)
        data, err := ioutil.ReadFile(filePath)
        if err != nil {
            log.Printf("SuperPeer %s: Error reading file '%s' from origin server %s: %v", sp.Config.ID, refreshReq.FileName, fileMeta.OriginServerID, err)
            refreshResp.VersionNumber = -1
        } else {
            refreshResp.FileData = data
            refreshResp.VersionNumber = fileMeta.VersionNumber
            refreshResp.LastModified = fileMeta.LastModified
        }
    }

    // Determine where to send the RefreshResponse
    if sp.isLeafNodeConnected(sourceID) {
        // Source is a LeafNode
        sp.LeafNodeConnsMu.RLock()
        conn, exists := sp.LeafNodeConns[sourceID]
        sp.LeafNodeConnsMu.RUnlock()
        if exists {
            err := sendJSONMessage(conn, refreshResp)
            if err != nil {
                log.Printf("SuperPeer %s: Error sending RefreshResponse to LeafNode %s: %v", sp.Config.ID, sourceID, err)
            } else {
                log.Printf("SuperPeer %s: Sent RefreshResponse for '%s' to LeafNode %s", sp.Config.ID, refreshReq.FileName, sourceID)
            }
        }
    } else {
        // Source is a neighbor SuperPeer
        sp.NeighborsMu.RLock()
        conn, exists := sp.Neighbors[sourceID]
        sp.NeighborsMu.RUnlock()
        if exists {
            err := sendJSONMessage(conn, refreshResp)
            if err != nil {
                log.Printf("SuperPeer %s: Error sending RefreshResponse to neighbor SuperPeer %s: %v", sp.Config.ID, sourceID, err)
            } else {
                log.Printf("SuperPeer %s: Sent RefreshResponse for '%s' to neighbor SuperPeer %s", sp.Config.ID, refreshReq.FileName, sourceID)
            }
        }
    }
}


// getSuperPeerPort maps SuperPeerID to its corresponding port number.
// Ensure that all SuperPeers are included in this map.
func getSuperPeerPort(superPeerID string) int {
    superPeerPorts := map[string]int{
        "SP1": 8001,
        "SP2": 8002,
        "SP3": 8003,
        "SP4": 8004,
        "SP5": 8005,
        "SP6": 8006,
        "SP7": 8007,
        "SP8": 8008,
        "SP9": 8009,
        "SP10": 8010,
        // Add more SuperPeers as needed
    }

    port, exists := superPeerPorts[superPeerID]
    if !exists {
        log.Fatalf("SuperPeer: Unknown SuperPeerID '%s'. Please update getSuperPeerPort with the correct port.", superPeerID)
    }
    return port
}


// handleQuery processes a QueryMessage from a LeafNode or neighbor SuperPeer
func (sp *SuperPeer) handleQuery(queryMsg QueryMessage, sourceID string) {
    sp.FilesMu.RLock()
    fileMeta, exists := sp.Files[queryMsg.FileName]
    sp.FilesMu.RUnlock()

    queryResp := QueryResponseMessage{
        MessageType:    MsgTypeQueryResponse,
        MessageID:      uuid.New().String(),
        FileName:       queryMsg.FileName,
        OriginServerID: fileMeta.OriginServerID,
    }

    if !exists {
        // File does not exist on SuperPeer
        queryResp.LastModified = ""
    } else {
        queryResp.LastModified = fileMeta.LastModified
    }

    // Determine where to send the QueryResponse
    if sp.isLeafNodeConnected(sourceID) {
        // Source is a LeafNode
        sp.LeafNodeConnsMu.RLock()
        conn, exists := sp.LeafNodeConns[sourceID]
        sp.LeafNodeConnsMu.RUnlock()
        if exists {
            err := sendJSONMessage(conn, queryResp)
            if err != nil {
                log.Printf("SuperPeer %s: Error sending QueryResponse to LeafNode %s: %v", sp.Config.ID, sourceID, err)
            } else {
                log.Printf("SuperPeer %s: Sent QueryResponse for '%s' to LeafNode %s", sp.Config.ID, queryMsg.FileName, sourceID)
            }
        }
    } else {
        // Source is a neighbor SuperPeer
        sp.NeighborsMu.RLock()
        conn, exists := sp.Neighbors[sourceID]
        sp.NeighborsMu.RUnlock()
        if exists {
            err := sendJSONMessage(conn, queryResp)
            if err != nil {
                log.Printf("SuperPeer %s: Error sending QueryResponse to neighbor SuperPeer %s: %v", sp.Config.ID, sourceID, err)
            } else {
                log.Printf("SuperPeer %s: Sent QueryResponse for '%s' to neighbor SuperPeer %s", sp.Config.ID, queryMsg.FileName, sourceID)
            }
        }
    }
}

// isLeafNodeConnected checks if a given ID corresponds to a connected LeafNode
func (sp *SuperPeer) isLeafNodeConnected(peerID string) bool {
    sp.LeafNodeConnsMu.RLock()
    defer sp.LeafNodeConnsMu.RUnlock()
    _, exists := sp.LeafNodeConns[peerID]
    return exists
}

// isNeighborSuperPeerConnected checks if a given ID corresponds to a connected neighbor SuperPeer
func (sp *SuperPeer) isNeighborSuperPeerConnected(peerID string) bool {
    sp.NeighborsMu.RLock()
    defer sp.NeighborsMu.RUnlock()
    _, exists := sp.Neighbors[peerID]
    return exists
}

// handleQueryResponse processes a QueryResponseMessage from a neighbor SuperPeer
// This function can be expanded based on your network's requirements
func (sp *SuperPeer) handleQueryResponse(msg QueryResponseMessage) {
    // Placeholder for handling QueryResponse from neighbor SuperPeer
    // Implement based on your network's query handling strategy
    log.Printf("SuperPeer %s: Received QueryResponse for '%s' from neighbor SuperPeer %s", sp.Config.ID, msg.FileName, msg.OriginServerID)
    // You can implement caching or further propagation if needed
}

// handlePollResponse processes a PollResponseMessage from a neighbor SuperPeer
// This function can be expanded based on your network's requirements
func (sp *SuperPeer) handlePollResponse(msg PollResponseMessage) {
    // Placeholder for handling PollResponse from neighbor SuperPeer
    // Implement based on your network's polling strategy
    log.Printf("SuperPeer %s: Received PollResponse for '%s' from neighbor SuperPeer %s", sp.Config.ID, msg.FileName, msg.Status)
    // Update file metadata or propagate invalidation as needed
}



// notifyNeighborSuperPeersInvalidation sends an InvalidationMessage to all neighbor SuperPeers
func (sp *SuperPeer) notifyNeighborSuperPeersInvalidation(fileName string) {
    sp.NeighborsMu.RLock()
    defer sp.NeighborsMu.RUnlock()

    invMsg := InvalidationMessage{
        MessageType:    MsgTypeInvalidation,
        MsgID:          uuid.New().String(),
        OriginServerID: sp.Config.ID,
        FileName:       fileName,
        VersionNumber:  sp.Files[fileName].VersionNumber,
    }

    for neighborID, conn := range sp.Neighbors {
        err := sendJSONMessage(conn, invMsg)
        if err != nil {
            log.Printf("SuperPeer %s: Error sending InvalidationMessage to neighbor SuperPeer %s: %v", sp.Config.ID, neighborID, err)
        } else {
            log.Printf("SuperPeer %s: Sent InvalidationMessage for '%s' to neighbor SuperPeer %s", sp.Config.ID, fileName, neighborID)
        }
    }
}


