import socket
import json
import threading
import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description="Super-Peer")
    parser.add_argument('config_file', type=str, help='Path to configuration JSON file')
    parser.add_argument('super_peer_id', type=str, help='ID of the Super-Peer to start')
    return parser.parse_args()

def handle_leaf_node(conn, addr, super_peer):
    try:
        with conn:
            leaf_file = conn.makefile('r')
            # Expecting FileRegistrationMessage
            line = leaf_file.readline()
            if not line:
                print(f"[{super_peer['id']}] No data received from Leaf Node at {addr}. Closing connection.")
                return
            msg = json.loads(line.strip())
            if msg.get("message_type") != "file_registration":
                print(f"[{super_peer['id']}] Unknown initial message type from {addr}: {msg.get('message_type')}")
                return
            # Process file registration
            leaf_id = msg.get("leaf_node_id")
            files = msg.get("files", [])
            super_peer['leaf_nodes'][leaf_id] = {'files': files, 'address': addr}
            print(f"[{super_peer['id']}] Registered Leaf Node {leaf_id} with files: {files}")
            # Continue handling leaf node messages
            while True:
                line = leaf_file.readline()
                if not line:
                    print(f"[{super_peer['id']}] Leaf Node {leaf_id} disconnected.")
                    break
                # Handle further messages from leaf nodes if necessary
    except Exception as e:
        print(f"[{super_peer['id']}] Error handling Leaf Node at {addr}: {e}")

def handle_client(conn, addr, super_peer):
    try:
        with conn:
            client_file = conn.makefile('r')
            while True:
                line = client_file.readline()
                if not line:
                    print(f"[{super_peer['id']}] Client at {addr} disconnected.")
                    break
                msg = json.loads(line.strip())
                if msg.get("message_type") == "file_query":
                    # Process file query
                    query_id = msg.get("message_id")
                    origin_id = msg.get("origin_id")
                    file_name = msg.get("file_name")
                    ttl = msg.get("ttl", 5)
                    print(f"[{super_peer['id']}] Received file_query from {origin_id}: {msg}")
                    # Search for the file in registered leaf nodes
                    for leaf_id, leaf_info in super_peer['leaf_nodes'].items():
                        for file in leaf_info['files']:
                            if file['file_name'] == file_name:
                                # Send QueryHitMessage back to client
                                response_msg = {
                                    "message_type": "query_hit",
                                    "message_id": query_id,
                                    "responding_id": leaf_id
                                }
                                conn.sendall((json.dumps(response_msg) + '\n').encode())
                                print(f"[{super_peer['id']}] Sent QueryHitMessage to Client {origin_id} for {file_name} from {leaf_id}")
                else:
                    print(f"[{super_peer['id']}] Unknown message type from Client at {addr}: {msg.get('message_type')}")
    except Exception as e:
        print(f"[{super_peer['id']}] Error handling Client at {addr}: {e}")

def start_super_peer(super_peer):
    # Start Leaf Node listener
    leaf_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    leaf_listener.bind((super_peer['address'], super_peer['port']))
    leaf_listener.listen()
    print(f"[{super_peer['id']}] Listening for Leaf Nodes on {super_peer['address']}:{super_peer['port']}")

    # Start Client listener
    client_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_listener.bind((super_peer['address'], super_peer['client_port']))
    client_listener.listen()
    print(f"[{super_peer['id']}] Listening for Clients on {super_peer['address']}:{super_peer['client_port']}")

    def accept_leaf_nodes():
        while True:
            conn, addr = leaf_listener.accept()
            print(f"[{super_peer['id']}] Connected to Leaf Node at {addr}")
            threading.Thread(target=handle_leaf_node, args=(conn, addr, super_peer), daemon=True).start()

    def accept_clients():
        while True:
            conn, addr = client_listener.accept()
            print(f"[{super_peer['id']}] Connected to Client at {addr}")
            threading.Thread(target=handle_client, args=(conn, addr, super_peer), daemon=True).start()

    # Start threads to accept connections
    threading.Thread(target=accept_leaf_nodes, daemon=True).start()
    threading.Thread(target=accept_clients, daemon=True).start()

    # Keep the main thread alive
    while True:
        try:
            threading.Event().wait(1)
        except KeyboardInterrupt:
            print(f"[{super_peer['id']}] Shutting down.")
            break

def main():
    args = parse_arguments()
    config_file = args.config_file
    super_peer_id = args.super_peer_id

    # Load configuration
    with open(config_file, 'r') as f:
        config = json.load(f)

    # Find the super_peer in the config
    super_peers = config.get('super_peers', [])
    super_peer = next((sp for sp in super_peers if sp['id'] == super_peer_id), None)
    if not super_peer:
        print(f"Super-Peer ID {super_peer_id} not found in configuration.")
        return

    # Initialize leaf_nodes dictionary
    super_peer['leaf_nodes'] = {}

    # Start the Super-Peer
    start_super_peer(super_peer)

if __name__ == "__main__":
    main()
