// leafnode.go
package main

import (
    "bufio"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "math/rand"
    "net"
    "os"
    "strings"
    "sync"
    "time"

    "github.com/google/uuid"
)

// FileMetadataClient holds metadata for files on the client side
type FileMetadataClient struct {
    VersionNumber    int    `json:"version_number"`
    OriginServerID   string `json:"origin_server_id"`
    ConsistencyState string `json:"consistency_state"` // "valid" or "invalid"
    LastModified     string `json:"last_modified_time"`
}

// ExperimentStats holds statistics for experiments
type ExperimentStats struct {
    TotalQueries   int
    InvalidQueries int
    mu             sync.Mutex
}

// LeafNodeConfig represents the configuration for a LeafNode
type LeafNodeConfig struct {
    ID          string `json:"id"`
    Address     string `json:"address"`
    Port        int    `json:"port"`
    SuperPeerID string `json:"super_peer"`
    EnablePush  bool   `json:"enable_push"`
    EnablePull  bool   `json:"enable_pull"`
    TTR         int    `json:"TTR"` // Time-To-Refresh in seconds
    IsQueryNode bool   `json:"is_query_node"`
}

// LeafNode represents a LeafNode in the network
type LeafNode struct {
    Config        LeafNodeConfig
    SuperPeerConn net.Conn
    Files         map[string]FileMetadataClient // FileName -> Metadata
    mu            sync.RWMutex                  // Mutex to protect shared resources
}

// NewLeafNode initializes a new LeafNode instance
func NewLeafNode(config LeafNodeConfig) *LeafNode {
    return &LeafNode{
        Config: config,
        Files:  make(map[string]FileMetadataClient),
    }
}

// Start launches the LeafNode's client to connect to the SuperPeer and handle messages
func (ln *LeafNode) Start(wg *sync.WaitGroup) {
    defer wg.Done()

    // Seed the random number generator
    rand.Seed(time.Now().UnixNano())

    // Connect to SuperPeer
    spAddress := fmt.Sprintf("127.0.0.1:%d", getSuperPeerPort(ln.Config.SuperPeerID))
    conn, err := net.Dial("tcp", spAddress)
    if err != nil {
        log.Fatalf("Leaf-Node %s: Failed to connect to SuperPeer %s at %s: %v", ln.Config.ID, ln.Config.SuperPeerID, spAddress, err)
    }
    ln.SuperPeerConn = conn
    log.Printf("Leaf-Node %s: Connected to SuperPeer %s at %s", ln.Config.ID, ln.Config.SuperPeerID, spAddress)

    // Send PeerIDMessage upon connection
    peerIDMsg := PeerIDMessage{
        MessageType: MsgTypePeerID,
        PeerType:    LeafNodeType,
        PeerID:      ln.Config.ID,
    }
    err = sendJSONMessage(conn, peerIDMsg)
    if err != nil {
        log.Printf("Leaf-Node %s: Error sending PeerIDMessage: %v", ln.Config.ID, err)
    } else {
        log.Printf("Leaf-Node %s: Sent PeerIDMessage to SuperPeer", ln.Config.ID)
    }

    // Register files with the SuperPeer
    ln.registerFiles()

    // Initialize experiment statistics if this is the querying LeafNode
    var stats *ExperimentStats
    if ln.Config.IsQueryNode { // Assuming IsQueryNode is set in config
        stats = &ExperimentStats{}
    }

    // Start listening for messages from SuperPeer
    go ln.listenToSuperPeer(conn, stats)

    // Start performing random queries if this is the querying LeafNode
    if ln.Config.IsQueryNode {
        go ln.performRandomQueries(5*time.Second, 100, stats) // Adjust parameters as needed
    }

    // Start polling if Pull-Based consistency is enabled
    if ln.Config.EnablePull {
        go ln.startPolling()
    }

    // Start user commands
    go ln.handleUserCommands()

    // Start simulating file modifications if this is a modifier LeafNode
    if !ln.Config.IsQueryNode { // Assuming non-query nodes are modifiers
        go ln.simulateModifications()
    }

    // Periodically print statistics if querying LeafNode
    if ln.Config.IsQueryNode {
        go func() {
            ticker := time.NewTicker(30 * time.Second)
            defer ticker.Stop()
            for {
                <-ticker.C
                stats.mu.Lock()
                percentage := 0.0
                if stats.TotalQueries > 0 {
                    percentage = (float64(stats.InvalidQueries) / float64(stats.TotalQueries)) * 100
                }
                fmt.Printf("=== Experiment Stats ===\n")
                fmt.Printf("Total Queries: %d\n", stats.TotalQueries)
                fmt.Printf("Invalid Queries: %d\n", stats.InvalidQueries)
                fmt.Printf("Percentage of Invalid Queries: %.2f%%\n", percentage)
                fmt.Printf("========================\n")
                stats.mu.Unlock()
            }
        }()
    }

    // Keep the main goroutine alive
    select {}
}

// getSuperPeerPort maps SuperPeerID to its corresponding port number.
// Update the map below with all your SuperPeers and their ports.
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
        log.Fatalf("Leaf-Node: Unknown SuperPeerID '%s'. Please update getSuperPeerPort with the correct port.", superPeerID)
    }
    return port
}

// registerFiles registers owned files with the SuperPeer
func (ln *LeafNode) registerFiles() {
    ln.mu.Lock()
    defer ln.mu.Unlock()

    // Read files from the owned directory
    ownedDir := fmt.Sprintf("./leafnode_shared/%s/owned", ln.Config.ID)
    files, err := ioutil.ReadDir(ownedDir)
    if err != nil {
        log.Printf("Leaf-Node %s: Error reading owned directory: %v", ln.Config.ID, err)
        return
    }

    var fileList []FileMetadata
    for _, file := range files {
        if !file.IsDir() {
            fileMeta := FileMetadata{
                FileName: file.Name(),
                FileSize: file.Size(),
            }
            fileList = append(fileList, fileMeta)

            // Initialize file metadata in ln.Files
            ln.Files[file.Name()] = FileMetadataClient{
                VersionNumber:    1,
                OriginServerID:   ln.Config.SuperPeerID,
                ConsistencyState: "valid",
                LastModified:     file.ModTime().Format(time.RFC3339),
            }
        }
    }

    // Send FileRegistrationMessage to SuperPeer
    regMsg := FileRegistrationMessage{
        MessageType: MsgTypeFileRegistration,
        LeafNodeID:  ln.Config.ID,
        Files:       fileList,
    }
    err = sendJSONMessage(ln.SuperPeerConn, regMsg)
    if err != nil {
        log.Printf("Leaf-Node %s: Error sending FileRegistrationMessage: %v", ln.Config.ID, err)
    } else {
        log.Printf("Leaf-Node %s: Registered %d files with SuperPeer", ln.Config.ID, len(fileList))
    }
}

// listenToSuperPeer continuously listens for messages from the connected SuperPeer
func (ln *LeafNode) listenToSuperPeer(conn net.Conn, stats *ExperimentStats) {
    decoder := json.NewDecoder(conn)
    decoder.UseNumber() // Handle numbers correctly
    for {
        var msgMap map[string]interface{}
        err := decoder.Decode(&msgMap)
        if err != nil {
            log.Printf("Leaf-Node %s: Connection to SuperPeer lost: %v", ln.Config.ID, err)
            return
        }

        messageType, ok := msgMap["message_type"].(string)
        if !ok {
            log.Printf("Leaf-Node %s: Received message without message_type from SuperPeer", ln.Config.ID)
            continue
        }

        // Log the received message_type for debugging
        log.Printf("Leaf-Node %s: Received message_type '%s' from SuperPeer", ln.Config.ID, messageType)

        switch messageType {
        case MsgTypePeerID:
            var peerMsg PeerIDMessage
            err := mapToStruct(msgMap, &peerMsg)
            if err != nil {
                log.Printf("Leaf-Node %s: Error parsing PeerIDMessage: %v", ln.Config.ID, err)
                continue
            }
            ln.handlePeerIDMessage(peerMsg)
        case MsgTypeInvalidation:
            var invMsg InvalidationMessage
            err := mapToStruct(msgMap, &invMsg)
            if err != nil {
                log.Printf("Leaf-Node %s: Error parsing InvalidationMessage: %v", ln.Config.ID, err)
                continue
            }
            if ln.Config.EnablePush {
                ln.handleInvalidation(invMsg)
            }
        case MsgTypePollResponse:
            var pollResp PollResponseMessage
            err := mapToStruct(msgMap, &pollResp)
            if err != nil {
                log.Printf("Leaf-Node %s: Error parsing PollResponseMessage: %v", ln.Config.ID, err)
                continue
            }
            if ln.Config.EnablePull {
                ln.handlePollResponse(pollResp)
            }
        case MsgTypeRefreshResponse:
            var refreshResp RefreshResponseMessage
            err := mapToStruct(msgMap, &refreshResp)
            if err != nil {
                log.Printf("Leaf-Node %s: Error parsing RefreshResponseMessage: %v", ln.Config.ID, err)
                continue
            }
            ln.handleRefreshResponse(refreshResp)
        case MsgTypeQueryResponse:
            var queryResp QueryResponseMessage
            err := mapToStruct(msgMap, &queryResp)
            if err != nil {
                log.Printf("Leaf-Node %s: Error parsing QueryResponseMessage: %v", ln.Config.ID, err)
                continue
            }
            if ln.Config.IsQueryNode {
                ln.handleQueryResponse(queryResp, stats)
            }
        // Handle other message types as needed
        default:
            log.Printf("Leaf-Node %s: Unknown message_type '%s' from SuperPeer", ln.Config.ID, messageType)
        }
    }
}

// handlePeerIDMessage processes a PeerIDMessage from SuperPeer
func (ln *LeafNode) handlePeerIDMessage(msg PeerIDMessage) {
    log.Printf("Leaf-Node %s: Received PeerIDMessage from SuperPeer %s", ln.Config.ID, msg.PeerID)
}

// handleInvalidation processes an InvalidationMessage
func (ln *LeafNode) handleInvalidation(msg InvalidationMessage) {
    ln.mu.Lock()
    defer ln.mu.Unlock()

    // Check if the LeafNode has a cached copy of the file
    fileMeta, exists := ln.Files[msg.FileName]
    if exists {
        // Mark the file as invalid
        fileMeta.ConsistencyState = "invalid"
        ln.Files[msg.FileName] = fileMeta

        log.Printf("Leaf-Node %s: Received INVALIDATION for '%s' from Origin Server %s (Version: %d)", ln.Config.ID, msg.FileName, msg.OriginServerID, msg.VersionNumber)

        // Notify the user
        fmt.Printf("*** ALERT: The file '%s' has been invalidated and is marked as INVALID in your cache. ***\n", msg.FileName)

        // Do NOT send RefreshRequest automatically
        // Refresh will occur only when the user invokes the 'refresh' command
    }
}

// performRandomQueries performs random queries at specified intervals
func (ln *LeafNode) performRandomQueries(queryInterval time.Duration, totalQueries int, stats *ExperimentStats) {
    ticker := time.NewTicker(queryInterval)
    defer ticker.Stop()

    for i := 0; i < totalQueries; i++ {
        <-ticker.C
        // Select a random file to query
        fileName := ln.getRandomFile()
        if fileName == "" {
            log.Printf("Leaf-Node %s: No files available for querying", ln.Config.ID)
            continue
        }
        // Send a query to SuperPeer
        ln.sendQuery(fileName, stats)
    }
}

// getRandomFile selects a random file from availableFiles
func (ln *LeafNode) getRandomFile() string {
    ln.mu.RLock()
    defer ln.mu.RUnlock()

    ownedDir := fmt.Sprintf("./leafnode_shared/%s/owned", ln.Config.ID)
    files, err := ioutil.ReadDir(ownedDir)
    if err != nil {
        log.Printf("Leaf-Node %s: Error reading owned directory: %v", ln.Config.ID, err)
        return ""
    }

    availableFiles := []string{}
    for _, file := range files {
        if !file.IsDir() {
            availableFiles = append(availableFiles, file.Name())
        }
    }

    if len(availableFiles) == 0 {
        return ""
    }

    return availableFiles[rand.Intn(len(availableFiles))]
}

// sendQuery sends a QueryMessage to the SuperPeer
func (ln *LeafNode) sendQuery(fileName string, stats *ExperimentStats) {
    // Generate a unique MessageID for the query
    messageID := uuid.New().String()

    // Construct the QueryMessage
    queryMsg := QueryMessage{
        MessageType: MsgTypeQuery,
        MessageID:   messageID,
        FileName:    fileName,
    }

    // Send the query to SuperPeer
    err := sendJSONMessage(ln.SuperPeerConn, queryMsg)
    if err != nil {
        log.Printf("Leaf-Node %s: Error sending query for '%s': %v", ln.Config.ID, fileName, err)
        return
    }
    log.Printf("Leaf-Node %s: Sent query for '%s' (MessageID: %s)", ln.Config.ID, fileName, messageID)

    // Record the query for statistics
    stats.mu.Lock()
    stats.TotalQueries++
    stats.mu.Unlock()
}

// handleQueryResponse processes a QueryResponseMessage from the SuperPeer
func (ln *LeafNode) handleQueryResponse(msg QueryResponseMessage, stats *ExperimentStats) {
    ln.mu.Lock()
    defer ln.mu.Unlock()

    fileMeta, exists := ln.Files[msg.FileName]
    if !exists {
        log.Printf("Leaf-Node %s: Received QueryResponse for unknown file '%s'", ln.Config.ID, msg.FileName)
        return
    }

    if msg.LastModified == "" {
        // File not found on SuperPeer
        stats.mu.Lock()
        stats.InvalidQueries++
        stats.mu.Unlock()
        log.Printf("Leaf-Node %s: QueryResponse indicates file '%s' does not exist on SuperPeer", ln.Config.ID, msg.FileName)
        return
    }

    // Compare last-mod-time
    originLastModified, err := time.Parse(time.RFC3339, msg.LastModified)
    if err != nil {
        log.Printf("Leaf-Node %s: Error parsing last-modified time: %v", ln.Config.ID, err)
        return
    }

    localLastModified, err := time.Parse(time.RFC3339, fileMeta.LastModified)
    if err != nil {
        log.Printf("Leaf-Node %s: Error parsing local last-modified time: %v", ln.Config.ID, err)
        return
    }

    if localLastModified.Before(originLastModified) {
        // Invalid query result
        stats.mu.Lock()
        stats.InvalidQueries++
        stats.mu.Unlock()
        log.Printf("Leaf-Node %s: Invalid query result for '%s'", ln.Config.ID, msg.FileName)
    } else {
        log.Printf("Leaf-Node %s: Valid query result for '%s'", ln.Config.ID, msg.FileName)
    }
}

// startPolling initiates the periodic polling mechanism based on TTR
func (ln *LeafNode) startPolling() {
    ticker := time.NewTicker(time.Duration(ln.Config.TTR) * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            ln.pollOriginServer()
        }
    }
}

// pollOriginServer sends PollRequest messages for files whose TTR has expired
func (ln *LeafNode) pollOriginServer() {
    if !ln.Config.EnablePull {
        return // Do not poll if pull is disabled
    }

    ln.mu.RLock()
    defer ln.mu.RUnlock()

    for fileName, meta := range ln.Files {
        if meta.ConsistencyState == "invalid" {
            continue // Skip already invalid files
        }

        // Generate a unique MessageID for the PollRequest
        messageID := uuid.New().String()

        // Send PollRequestMessage
        pollReq := PollRequestMessage{
            MessageType:          MsgTypePollRequest,
            MessageID:            messageID,
            FileName:             fileName,
            CurrentVersionNumber: meta.VersionNumber,
        }
        err := sendJSONMessage(ln.SuperPeerConn, pollReq)
        if err != nil {
            log.Printf("Leaf-Node %s: Error sending PollRequest for '%s': %v", ln.Config.ID, fileName, err)
            continue
        }
        log.Printf("Leaf-Node %s: Sent PollRequest for '%s' (MessageID: %s)", ln.Config.ID, fileName, messageID)
    }
}

// handlePollResponse processes a PollResponseMessage from the SuperPeer
func (ln *LeafNode) handlePollResponse(msg PollResponseMessage) {
    ln.mu.Lock()
    defer ln.mu.Unlock()

    fileMeta, exists := ln.Files[msg.FileName]
    if !exists {
        log.Printf("Leaf-Node %s: Received PollResponse for unknown file '%s'", ln.Config.ID, msg.FileName)
        return
    }

    if msg.Status == "invalid" && msg.NewVersionNumber > fileMeta.VersionNumber {
        // Update the file's version and mark as invalid
        fileMeta.VersionNumber = msg.NewVersionNumber
        fileMeta.ConsistencyState = "invalid"
        ln.Files[msg.FileName] = fileMeta

        log.Printf("Leaf-Node %s: Received PollResponse. File '%s' is outdated (New Version: %d)", ln.Config.ID, msg.FileName, msg.NewVersionNumber)

        // Notify the user
        fmt.Printf("*** ALERT: The file '%s' is outdated and has been marked as INVALID. ***\n", msg.FileName)
    } else if msg.Status == "valid" {
        // File is still valid
        log.Printf("Leaf-Node %s: Received PollResponse. File '%s' is still valid.", ln.Config.ID, msg.FileName)
    }
}

// RefreshFile allows the user to manually refresh an outdated file
func (ln *LeafNode) RefreshFile(fileName string) {
    ln.mu.RLock()
    meta, exists := ln.Files[fileName]
    ln.mu.RUnlock()

    if !exists {
        fmt.Printf("Leaf-Node %s: No such file '%s' to refresh.\n", ln.Config.ID, fileName)
        return
    }

    if meta.ConsistencyState != "invalid" {
        fmt.Printf("Leaf-Node %s: File '%s' is not outdated and does not need refresh.\n", ln.Config.ID, fileName)
        return
    }

    // Generate a unique MessageID for the RefreshRequest
    messageID := uuid.New().String()

    // Send RefreshRequestMessage
    refreshReq := RefreshRequestMessage{
        MessageType: MsgTypeRefreshRequest,
        MessageID:   messageID,
        FileName:    fileName,
    }
    err := sendJSONMessage(ln.SuperPeerConn, refreshReq)
    if err != nil {
        log.Printf("Leaf-Node %s: Error sending RefreshRequest for '%s': %v", ln.Config.ID, fileName, err)
        return
    }
    log.Printf("Leaf-Node %s: Sent RefreshRequest for '%s' (MessageID: %s)", ln.Config.ID, fileName, messageID)
}

// handleRefreshResponse processes a RefreshResponseMessage from the SuperPeer
func (ln *LeafNode) handleRefreshResponse(msg RefreshResponseMessage) {
    ln.mu.Lock()
    defer ln.mu.Unlock()

    log.Printf("Leaf-Node %s: Received RefreshResponse for '%s'", ln.Config.ID, msg.FileName)

    // Verify that the file exists in the cache
    fileMeta, exists := ln.Files[msg.FileName]
    if !exists {
        log.Printf("Leaf-Node %s: File '%s' not found in metadata", ln.Config.ID, msg.FileName)
        return
    }

    if msg.VersionNumber == -1 {
        // SuperPeer indicates the file does not exist
        log.Printf("Leaf-Node %s: File '%s' does not exist on SuperPeer", ln.Config.ID, msg.FileName)
        fmt.Printf("*** ERROR: The file '%s' does not exist on the SuperPeer. ***\n", msg.FileName)
        return
    }

    // Update the file with new data
    filePath := fmt.Sprintf("./leafnode_shared/%s/cached/%s", ln.Config.ID, msg.FileName)
    err := ioutil.WriteFile(filePath, msg.FileData, 0644)
    if err != nil {
        log.Printf("Leaf-Node %s: Error writing refreshed file '%s': %v", ln.Config.ID, msg.FileName, err)
        return
    }

    // Update metadata
    fileMeta.VersionNumber = msg.VersionNumber
    fileMeta.LastModified = msg.LastModified
    fileMeta.ConsistencyState = "valid"
    ln.Files[msg.FileName] = fileMeta

    log.Printf("Leaf-Node %s: Successfully refreshed file '%s' to version %d", ln.Config.ID, msg.FileName, msg.VersionNumber)
    fmt.Printf("*** INFO: The file '%s' has been refreshed and is now valid. ***\n", msg.FileName)
}

// handleUserCommands handles user inputs from the console
func (ln *LeafNode) handleUserCommands() {
    scanner := bufio.NewScanner(os.Stdin)
    fmt.Println("Enter 'refresh <filename>' to manually refresh an outdated file:")
    for scanner.Scan() {
        input := scanner.Text()
        parts := strings.Split(input, " ")
        if len(parts) != 2 || parts[0] != "refresh" {
            fmt.Println("Invalid command. Usage: refresh <filename>")
            continue
        }
        fileName := parts[1]
        ln.RefreshFile(fileName)
    }
}

// simulateFileModification simulates the modification of a file and broadcasts an invalidation
func (ln *LeafNode) simulateFileModification(fileName string) {
    ln.mu.Lock()
    defer ln.mu.Unlock()

    // Increment version number
    fileMeta, exists := ln.Files[fileName]
    if !exists {
        log.Printf("Leaf-Node %s: Cannot modify unknown file '%s'", ln.Config.ID, fileName)
        return
    }
    fileMeta.VersionNumber++
    fileMeta.LastModified = time.Now().Format(time.RFC3339)
    ln.Files[fileName] = fileMeta

    // Modify the file on disk
    filePath := fmt.Sprintf("./leafnode_shared/%s/owned/%s", ln.Config.ID, fileName)
    newContent := fmt.Sprintf("Modified by LeafNode %s at version %d.", ln.Config.ID, fileMeta.VersionNumber)
    err := ioutil.WriteFile(filePath, []byte(newContent), 0644)
    if err != nil {
        log.Printf("Leaf-Node %s: Error modifying file '%s': %v", ln.Config.ID, fileName, err)
        return
    }

    // Broadcast InvalidationMessage
    invMsg := InvalidationMessage{
        MessageType:    MsgTypeInvalidation,
        MsgID:          uuid.New().String(),
        OriginServerID: ln.Config.ID,
        FileName:       fileName,
        VersionNumber:  fileMeta.VersionNumber,
    }
    err = sendJSONMessage(ln.SuperPeerConn, invMsg)
    if err != nil {
        log.Printf("Leaf-Node %s: Error sending InvalidationMessage for '%s': %v", ln.Config.ID, fileName, err)
        return
    }

    log.Printf("Leaf-Node %s: Simulated modification of '%s' to version %d and broadcasted invalidation", ln.Config.ID, fileName, fileMeta.VersionNumber)
}

// simulateModifications periodically modifies random files
func (ln *LeafNode) simulateModifications() {
    ticker := time.NewTicker(10 * time.Second) // Modify every 10 seconds
    defer ticker.Stop()

    for {
        <-ticker.C
        // Select a random owned file to modify
        ownedFiles := []string{}
        ownedDir := fmt.Sprintf("./leafnode_shared/%s/owned", ln.Config.ID)
        files, err := ioutil.ReadDir(ownedDir)
        if err != nil {
            log.Printf("Leaf-Node %s: Error reading owned directory: %v", ln.Config.ID, err)
            continue
        }
        for _, file := range files {
            if !file.IsDir() {
                ownedFiles = append(ownedFiles, file.Name())
            }
        }
        if len(ownedFiles) == 0 {
            log.Printf("Leaf-Node %s: No owned files to modify", ln.Config.ID)
            continue
        }
        fileName := ownedFiles[rand.Intn(len(ownedFiles))]
        ln.simulateFileModification(fileName)
    }
}
