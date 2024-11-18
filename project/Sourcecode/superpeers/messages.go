// messages.go
package main

import (
	"encoding/json"
	"log"
	"net"
)

// Peer Types
const (
    SuperPeerType = "superpeer"
    LeafNodeType  = "leafnode"
)

// PeerIDMessage is used to identify the type and ID of the connecting peer
type PeerIDMessage struct {
    MessageType string `json:"message_type"`
    PeerType    string `json:"peer_type"`
    PeerID      string `json:"peer_id"`
}

// FileMetadata contains basic information about a file
type FileMetadata struct {
    FileName string `json:"file_name"`
    FileSize int64  `json:"file_size"`
}

// FileRegistrationMessage is sent by LeafNodes to register their files with a SuperPeer
type FileRegistrationMessage struct {
    MessageType string         `json:"message_type"`
    LeafNodeID  string         `json:"leaf_node_id"`
    Files       []FileMetadata `json:"files"`
}

// InvalidationMessage is used to notify peers about file modifications
type InvalidationMessage struct {
    MessageType    string `json:"message_type"`
    MsgID          string `json:"msg_id"`
    OriginServerID string `json:"origin_server_id"`
    FileName       string `json:"file_name"`
    VersionNumber  int    `json:"version_number"`
}

// PollRequestMessage is used by LeafNodes to poll the SuperPeer for file validity
type PollRequestMessage struct {
    MessageType          string `json:"message_type"`
    MessageID            string `json:"message_id"`
    FileName             string `json:"file_name"`
    CurrentVersionNumber int    `json:"current_version_number"`
}

// PollResponseMessage is sent by SuperPeers in response to a PollRequestMessage
type PollResponseMessage struct {
    MessageType      string `json:"message_type"`
    MessageID        string `json:"message_id"`
    FileName         string `json:"file_name"`
    Status           string `json:"status"` // "valid" or "invalid"
    NewVersionNumber int    `json:"new_version_number"`
}

// RefreshRequestMessage is used by LeafNodes to request the latest version of a file
type RefreshRequestMessage struct {
    MessageType string `json:"message_type"`
    MessageID   string `json:"message_id"`
    FileName    string `json:"file_name"`
}

// RefreshResponseMessage is sent by SuperPeers in response to a RefreshRequestMessage
type RefreshResponseMessage struct {
    MessageType   string `json:"message_type"`
    MessageID     string `json:"message_id"`
    FileName      string `json:"file_name"`
    FileData      []byte `json:"file_data"`
    VersionNumber int    `json:"version_number"`
    LastModified  string `json:"last_modified"`
}

// QueryMessage is sent by LeafNodes to query for a file
type QueryMessage struct {
    MessageType string `json:"message_type"`
    MessageID   string `json:"message_id"`
    FileName    string `json:"file_name"`
}

// QueryResponseMessage is sent by SuperPeers in response to a QueryMessage
type QueryResponseMessage struct {
    MessageType    string `json:"message_type"`
    MessageID      string `json:"message_id"`
    FileName       string `json:"file_name"`
    LastModified   string `json:"last_modified"` // RFC3339 formatted time
    OriginServerID string `json:"origin_server_id"`
}

// sendJSONMessage sends a JSON-encoded message over a connection
func sendJSONMessage(conn net.Conn, msg interface{}) error {
    encoder := json.NewEncoder(conn)
    err := encoder.Encode(msg)
    if err != nil {
        log.Printf("Error sending message: %v", err)
    } else {
        // Serialize msg to JSON string for logging
        msgBytes, err := json.Marshal(msg)
        if err != nil {
            log.Printf("Sent message: [Error serializing message for log]")
        } else {
            log.Printf("Sent message: %s", string(msgBytes))
        }
    }
    return err
}


// mapToStruct converts a map to a struct using JSON marshalling and unmarshalling
func mapToStruct(m map[string]interface{}, s interface{}) error {
    // Convert json.Number to appropriate types
    convertNumbers(m)
    data, err := json.Marshal(m)
    if err != nil {
        return err
    }
    return json.Unmarshal(data, s)
}

// convertNumbers converts json.Number types in a map to int or float64
func convertNumbers(m map[string]interface{}) {
    for k, v := range m {
        switch val := v.(type) {
        case map[string]interface{}:
            convertNumbers(val)
        case []interface{}:
            for i := range val {
                if innerMap, ok := val[i].(map[string]interface{}); ok {
                    convertNumbers(innerMap)
                }
            }
        case json.Number:
            if intVal, err := val.Int64(); err == nil {
                m[k] = int(intVal)
            } else if floatVal, err := val.Float64(); err == nil {
                m[k] = floatVal
            }
        }
    }
}
