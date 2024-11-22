import socket 
import json
import threading
import time
import uuid
import sys
from queue import Queue
import csv
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration Files
CONFIG_FILE = 'config.json'

# Constants
FILE_NAME = 'file12.txt'  # The file you want to query
NUM_QUERIES = 200         # Number of queries each client will send per repetition
CUT_OFF_TIME = 5          # Time in seconds to wait for responses after sending queries
REPEAT_EXPERIMENTS = 200  # Number of times to repeat the experiment

# Thread-safe queue to collect results
results_queue = Queue()

def load_config(config_file):
    """
    Load the configuration from the config.json file.
    Returns dictionaries mapping Super-Peers and Leaf-Nodes.
    """
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    super_peers = config['super_peers']
    leaf_nodes = config['leaf_nodes']
    
    # Create a mapping from Leaf-Node ID to Super-Peer ID
    leaf_to_sp = {leaf['id']: leaf['super_peer'] for leaf in leaf_nodes}
    
    # Create a list of Super-Peers
    super_peer_list = []
    for sp in super_peers:
        super_peer_list.append({
            'id': sp['id'],
            'address': sp['address'],
            'port': sp['port'],
            'neighbors': sp['neighbors'],
            'leaf_nodes': sp['leaf_nodes']
        })
    
    return super_peer_list, leaf_to_sp

def client_thread(client_id, num_queries, super_peer_map, leaf_to_sp):
    """
    Function executed by each client thread.
    Connects to a Super-Peer, sends queries, and records response times.
    """
    try:
        # Determine which Super-Peer this client will connect to
        # For simplicity, distribute clients evenly across Super-Peers
        sp_index = (client_id - 1) % len(super_peer_map)
        super_peer = super_peer_map[sp_index]
        sp_address = super_peer['address']
        sp_port = super_peer['port']
        sp_id = super_peer['id']
        
        logging.info(f"[Client {client_id}] Connecting to Super-Peer {sp_id} at {sp_address}:{sp_port}...")
        
        # Establish connection to Super-Peer
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((sp_address, sp_port))
        logging.info(f"[Client {client_id}] Connected to Super-Peer {sp_id}.")
        
        # Send PeerIDMessage
        peer_id = f"TestClient-{client_id}-{uuid.uuid4()}"
        peer_id_msg = {
            "message_type": "peer_id",
            "peer_type": "leaf_node",
            "peer_id": peer_id
        }
        sock.sendall((json.dumps(peer_id_msg) + '\n').encode())
        logging.info(f"[Client {client_id}] Sent PeerIDMessage.")
        
        # Send FileRegistrationMessage
        registration_msg = {
            "message_type": "file_registration",
            "leaf_node_id": peer_id,
            "files": [
                {"file_name": FILE_NAME, "file_size": 1024}  # Example file metadata
            ]
        }
        sock.sendall((json.dumps(registration_msg) + '\n').encode())
        logging.info(f"[Client {client_id}] Sent FileRegistrationMessage.")
        
        # Function to receive QueryHitMessages
        def receive_messages(sock, message_id_map, client_id, response_category_map):
            logging.info(f"[Client {client_id}] Receiver thread started.")
            sock_file = sock.makefile('r')
            while True:
                line = sock_file.readline()
                if not line:
                    logging.info(f"[Client {client_id}] Connection closed by Super-Peer.")
                    break
                try:
                    msg = json.loads(line.strip())
                    if msg.get("message_type") == "query_hit":
                        msg_id = msg.get("message_id")
                        if msg_id in message_id_map:
                            response_time = (time.time() - message_id_map[msg_id]['start_time']) * 1000  # Convert to ms
                            leaf_node = msg.get("responding_id")
                            # Determine if Leaf-Node is within the same Super-Peer or different
                            category = "Same Super-Peer" if leaf_to_sp.get(leaf_node) == sp_id else "Different Super-Peer"
                            results_queue.put({
                                "client_id": client_id,
                                "message_id": msg_id,
                                "response_time_ms": response_time,
                                "leaf_node": leaf_node,
                                "category": category
                            })
                            logging.info(f"[Client {client_id}] Received QueryHitMessage from {leaf_node} ({category}) with response time {response_time:.2f} ms.")
                except json.JSONDecodeError as e:
                    logging.error(f"[Client {client_id}] Failed to decode JSON message: {e}")
    
        # Start receiver thread
        message_id_map = {}
        receiver = threading.Thread(target=receive_messages, args=(sock, message_id_map, client_id, {}))
        receiver.daemon = True
        receiver.start()
        logging.info(f"[Client {client_id}] Started receiver thread.")
        
        # Send queries
        for i in range(1, num_queries + 1):
            message_id = f"{peer_id}-{uuid.uuid4()}"
            query_msg = {
                "message_type": "file_query",
                "message_id": message_id,
                "origin_id": peer_id,
                "file_name": FILE_NAME,
                "ttl": 5
            }
            message_id_map[message_id] = {'start_time': time.time()}
            sock.sendall((json.dumps(query_msg) + '\n').encode())
            logging.info(f"[Client {client_id}] Sent Query {i}/{num_queries}.")
            time.sleep(0.01)  # Small delay between queries to prevent overwhelming the network
        
        logging.info(f"[Client {client_id}] All queries sent. Waiting for responses...")
        # Wait for the cutoff time
        time.sleep(CUT_OFF_TIME)
        sock.close()
        logging.info(f"[Client {client_id}] Connection closed.")
    
    except Exception as e:
        logging.error(f"[Client {client_id}] An error occurred: {e}")
        # Optionally, handle specific exceptions or perform cleanup

def run_experiment(num_clients, super_peer_map, leaf_to_sp):
    """
    Runs the experiment for a given number of clients.
    """
    threads = []
    for client_id in range(1, num_clients + 1):
        t = threading.Thread(target=client_thread, args=(client_id, NUM_QUERIES, super_peer_map, leaf_to_sp))
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
    file_exists = os.path.isfile(csv_filename)
    
    with open(csv_filename, 'a', newline='') as csvfile:
        fieldnames = ['client_id', 'message_id', 'response_time_ms', 'leaf_node', 'category']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        # Write header only if file does not exist
        if not file_exists:
            writer.writeheader()
        for r in results:
            writer.writerow(r)
    
    # Compute average response time
    if results:
        total_response_time = sum(r['response_time_ms'] for r in results)
        average_response_time = total_response_time / len(results)
        logging.info(f"\nAverage Response Time for {num_clients} client(s): {average_response_time:.2f} ms over {len(results)} hits\n")
    else:
        logging.info("\nNo QueryHitMessages received.\n")

def main():
    # Load configuration
    super_peer_map, leaf_to_sp = load_config(CONFIG_FILE)
    
    # Command-line arguments: number of clients and number of repetitions
    if len(sys.argv) == 3:
        try:
            num_clients = int(sys.argv[1])
            repeat_experiments = int(sys.argv[2])
        except ValueError:
            logging.error("Invalid arguments. Usage: python automatedclient.py <num_clients> <repeat_experiments>")
            sys.exit(1)
    elif len(sys.argv) == 2:
        try:
            num_clients = int(sys.argv[1])
            repeat_experiments = 200  # Default
        except ValueError:
            logging.error("Invalid number of clients. Usage: python automatedclient.py <num_clients> <repeat_experiments>")
            sys.exit(1)
    else:
        logging.error("Usage: python automatedclient.py <num_clients> <repeat_experiments>")
        logging.error("Example: python automatedclient.py 5 200")
        sys.exit(1)
    
    logging.info(f"Starting automated_client.py with {num_clients} client(s) for {repeat_experiments} repetitions...\n")
    
    # Initialize CSV files by writing headers if they don't exist
    for _ in range(1, num_clients + 1):
        csv_filename = f'results_{num_clients}_clients.csv'
        if not os.path.isfile(csv_filename):
            with open(csv_filename, 'w', newline='') as csvfile:
                fieldnames = ['client_id', 'message_id', 'response_time_ms', 'leaf_node', 'category']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
    
    # Run experiments
    for repetition in range(1, repeat_experiments + 1):
        logging.info(f"--- Experiment {repetition}/{repeat_experiments} for {num_clients} client(s) ---")
        run_experiment(num_clients, super_peer_map, leaf_to_sp)

if __name__ == "__main__":
    main()
