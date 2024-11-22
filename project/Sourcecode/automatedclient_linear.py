import socket
import json
import threading
import time
import uuid
import sys
from queue import Queue
import csv
import argparse

# Thread-safe queue to collect results
results_queue = Queue()

def parse_arguments():
    parser = argparse.ArgumentParser(description="Automated P2P Client")
    parser.add_argument('--config', type=str, required=True, help='Path to configuration JSON file')
    parser.add_argument('--clients', type=int, default=1, help='Number of concurrent clients to simulate')
    parser.add_argument('--queries', type=int, default=200, help='Number of queries each client will send')
    return parser.parse_args()

def load_config(config_file):
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        return config
    except Exception as e:
        print(f"Error loading configuration file: {e}")
        sys.exit(1)

# Configuration
FILE_NAME = 'file12.txt'           # The file you want to query
CUT_OFF_TIME = 30                  # Increased time in seconds to wait for responses after sending queries

def client_thread(client_id, super_peer, num_queries):
    try:
        super_peer_address = super_peer['address']
        super_peer_port = super_peer['client_port']  # Connect to client port
        super_peer_id = super_peer.get('id', f"SP{super_peer_port}")  # Default ID if not provided
        print(f"[Client {client_id}] Establishing connection to Super-Peer {super_peer_id} at {super_peer_address}:{super_peer_port}...")
        
        # Establish connection to Super-Peer
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((super_peer_address, super_peer_port))
        print(f"[Client {client_id}] Connected to Super-Peer {super_peer_id}.")
        
        # Function to receive QueryHitMessages
        def receive_messages(sock, message_id_map):
            print(f"[Client {client_id}] Receiver thread started and listening for messages.")
            sock_file = sock.makefile('r')
            while True:
                line = sock_file.readline()
                if not line:
                    print(f"[Client {client_id}] No more messages. Connection closed by Super-Peer.")
                    break
                try:
                    msg = json.loads(line.strip())
                    if msg.get("message_type") == "query_hit":
                        msg_id = msg.get("message_id")
                        if msg_id in message_id_map:
                            response_time = (time.perf_counter() - message_id_map[msg_id]['start_time']) * 1000  # Convert to ms
                            results_queue.put({
                                "client_id": client_id,
                                "message_id": msg_id,
                                "response_time_ms": response_time,
                                "leaf_node": msg.get("responding_id")
                            })
                            print(f"[Client {client_id}] Received QueryHitMessage for MessageID {msg_id} from Leaf-Node {msg.get('responding_id')} with response time {response_time:.2f} ms.")
                except json.JSONDecodeError as e:
                    print(f"[Client {client_id}] Failed to decode JSON message: {e}")
        
        # Start receiver thread
        message_id_map = {}
        receiver = threading.Thread(target=receive_messages, args=(sock, message_id_map))
        receiver.daemon = True
        receiver.start()
        print(f"[Client {client_id}] Started receiver thread.")
        
        # Send queries
        for i in range(1, num_queries + 1):
            message_id = f"Client-{client_id}-{uuid.uuid4()}"
            query_msg = {
                "message_type": "file_query",
                "message_id": message_id,
                "origin_id": f"Client-{client_id}",
                "file_name": FILE_NAME,
                "ttl": 5
            }
            message_id_map[message_id] = {'start_time': time.perf_counter()}
            sock.sendall((json.dumps(query_msg) + '\n').encode())
            print(f"[Client {client_id}] Sent Query {i}/{num_queries}: {query_msg}")
            time.sleep(0.01)  # Small delay between queries to prevent overwhelming the network
        
        print(f"[Client {client_id}] All queries sent. Waiting for responses...")
        # Wait for the cutoff time
        time.sleep(CUT_OFF_TIME)
        sock.close()
        print(f"[Client {client_id}] Connection closed.")
    
    except Exception as e:
        print(f"[Client {client_id}] Encountered an error: {e}")

def main():
    args = parse_arguments()
    
    config_file = args.config
    num_clients = args.clients
    num_queries = args.queries

    print(f"Loading configuration from {config_file}...")
    config = load_config(config_file)
    
    super_peers = config.get('super_peers', [])
    if not super_peers:
        print("No super-peers found in configuration.")
        sys.exit(1)
    
    print(f"Starting automated_client.py with {num_clients} client(s) and {num_queries} queries each...")
    threads = []
    for client_id in range(1, num_clients + 1):
        # Assign super-peers in a round-robin fashion
        super_peer = super_peers[(client_id - 1) % len(super_peers)]
        t = threading.Thread(target=client_thread, args=(client_id, super_peer, num_queries))
        t.start()
        threads.append(t)
    
    # Wait for all threads to finish
    for t in threads:
        t.join()
    
    # Collect and process results
    results = []
    while not results_queue.empty():
        results.append(results_queue.get())
    
    # Write results to CSV
    csv_filename = f'results_{num_clients}_clients.csv'
    try:
        with open(csv_filename, 'w', newline='') as csvfile:
            fieldnames = ['client_id', 'message_id', 'response_time_ms', 'leaf_node']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for r in results:
                writer.writerow(r)
        print(f"\nResults have been written to {csv_filename}")
    except Exception as e:
        print(f"Error writing to CSV: {e}")
    
    # Compute average response time
    if results:
        total_response_time = sum(r['response_time_ms'] for r in results)
        average_response_time = total_response_time / len(results)
        print(f"\nAverage Response Time: {average_response_time:.2f} ms over {len(results)} hits")
    else:
        print("\nNo QueryHitMessages received.")

if __name__ == "__main__":
    main()
