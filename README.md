# Gnutella P2P File Sharing System

This project implements a Gnutella-inspired Peer-to-Peer (P2P) file sharing system. The system is designed with a Super-Peer and Leaf-Node architecture, allowing decentralized file registration, searching, and downloading. It offers scalability and efficiency in managing large networks by incorporating message caching, file indexing, and basic fault tolerance.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Tests](#tests)
- [Known Issues](#known-issues)
- [Future Improvements](#future-improvements)
- [License](#license)

## Overview

The Gnutella P2P File Sharing System is a decentralized file-sharing network that supports multiple nodes (Super-Peers and Leaf-Nodes) to share files across the network. Nodes can register their files, query for files across the network, and download files directly from other nodes.

## Features

- **Dynamic File Registration**: Leaf nodes register files with their designated super-peer upon startup.
- **File Querying**: Nodes can search for files within the network, utilizing super-peers to propagate queries.
- **Response Propagation**: Efficient query-hit responses with a cache mechanism to prevent duplicate query responses.
- **Support for Multiple Nodes**: Easily configurable for a large number of super-peers and leaf-nodes.
- **Network Resilience**: Handles basic disconnections and reconnections for nodes.

## Architecture

The system uses a two-tier architecture:

- **Super-Peers**: Act as intermediaries, routing file queries between leaf nodes and other super-peers, and maintaining an index of files registered by their leaf nodes.
- **Leaf-Nodes**: Connect to super-peers and register their files. They perform file searches and can download files directly from other leaf nodes.

### Message Types

1. **Peer Identification (`peer_id`)**: Used by peers to announce themselves to a super-peer.
2. **File Registration (`file_registration`)**: Sent by leaf nodes to register files with a super-peer.
3. **File Query (`file_query`)**: Used to search for files across the network.
4. **Query Hit (`query_hit`)**: Response to a `file_query` indicating a file match.

## Installation

### Prerequisites

- [Go](https://golang.org/doc/install) (version 1.16 or higher)
- A terminal or command-line interface for running commands

### Steps

1. **Clone the repository**:
   ```bash
   git clone https://github.com/man0j-012/GnutellaP2P_File_Sharing_System.git
   cd GnutellaP2P_File_Sharing_System
Initialize Go modules (if required):

go mod tidy
Running the System
Start a Leaf-Node or Super-Peer: Use the following command to start a node, specifying the configuration file and node ID:


go run main.go config.json <NodeID>
Replace <NodeID> with the ID of the node, e.g., LN1 for a leaf node or SP1 for a super-peer.

Example Commands to Start Multiple Nodes:

bash
Copy code
# Start Leaf Nodes
go run main.go config.json LN1
go run main.go config.json LN2
go run main.go config.json LN3
# Start Super-Peers
go run main.go config.json SP1
go run main.go config.json SP2
File Registration: Leaf nodes will automatically register their files with their connected super-peer upon startup.

Searching for Files: In a leaf node, you can initiate a file search by entering the file name in the command-line interface. The leaf node sends a file_query message to its super-peer.

Downloading Files: Upon receiving a query_hit message indicating a file match, the user can choose to download the file from the responding leaf node.

Exiting the Program
To exit a node, use CTRL+C in the terminal or close the command prompt window.

Configuration
The config.json file contains configurations for all super-peers and leaf nodes in the network. Each node has a unique ID, address, port, and any neighboring nodes (for super-peers).

Sample Configuration File (config.json):
json
Copy code
{
  "super_peers": [
    {
      "id": "SP1",
      "address": "127.0.0.1",
      "port": 8000,
      "neighbors": ["SP2"],
      "leaf_nodes": ["LN1", "LN2"]
    },
    {
      "id": "SP2",
      "address": "127.0.0.1",
      "port": 8001,
      "neighbors": ["SP1"],
      "leaf_nodes": ["LN3"]
    }
  ],
  "leaf_nodes": [
    {
      "id": "LN1",
      "address": "127.0.0.1",
      "port": 9000,
      "super_peer": "SP1"
    },
    {
      "id": "LN2",
      "address": "127.0.0.1",
      "port": 9001,
      "super_peer": "SP1"
    },
    {
      "id": "LN3",
      "address": "127.0.0.1",
      "port": 9002,
      "super_peer": "SP2"
    }
  ]
}


Tests
To verify the functionality of the system, the following tests can be conducted:

File Registration: Ensure leaf nodes register their files with the designated super-peer correctly.
File Query Propagation: Confirm that file queries are routed correctly through the network of super-peers and reach the intended nodes.
Query Hit Response: Verify that query hits are sent back to the origin node, allowing for file downloads.
Concurrent Clients: Run multiple clients (leaf nodes) simultaneously to evaluate the system’s response times under varying load conditions.

Example Test Cases
Single File Query: Query a unique file name from one leaf node.
Multiple Queries: Simultaneously query multiple files from different leaf nodes.
High-Concurrency Test: Test response times by increasing the number of concurrent clients.

Known Issues
Latency with Increased Load: Response time may increase as the number of concurrent clients grows.
Duplicate File Registrations: The same file might be registered multiple times if re-registered frequently.
Network Partitioning: If connections between super-peers are severed, queries may not propagate to all parts of the network.

Future Improvements
Improved Fault Tolerance: Implement enhanced fault-tolerance mechanisms for node failures and network partitioning.
Blockchain-Based Security: Consider blockchain integration for secure file registration and authentication.
Optimized Query Routing: Develop more efficient routing protocols to reduce response times, especially under high concurrency.
