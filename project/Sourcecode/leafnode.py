import socket
import json
import threading
import time
import uuid
import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description="Leaf Node")
    parser.add_argument('--id', type=str, required=True, help='Leaf Node ID')
    parser.add_argument('--address', type=str, default='127.0.0.1', help='Leaf Node IP Address')
    parser.add_argument('--port', type=int, required=True, help='Leaf Node Port')
    parser.add_argument('--super_peer_id', type=str, required=True, help='Assigned Super-Peer ID')
    parser.add_argument('--super_peer_address', type=str, default='127.0.0.1', help='Super-Peer IP Address')
    parser.add_argument('--super_peer_port', type=int, required=True, help='Super-Peer Port')
    parser.add_argument('--file', type=str, required=True, help='File to register')
    return parser.parse_args()

def send_file_registration(sock, leaf_node_id, files):
    registration_msg = {
        "message_type": "file_registration",
        "leaf_node_id": leaf_node_id,
        "files": files
    }
    sock.sendall((json.dumps(registration_msg) + '\n').encode())
    print(f"[{leaf_node_id}] Sent FileRegistrationMessage: {registration_msg}")

def handle_queries(sock, leaf_node_id, available_files):
    sock_file = sock.makefile('r')
    while True:
        line = sock_file.readline()
        if not line:
            print(f"[{leaf_node_id}] Connection closed by Super-Peer.")
            break
        try:
            msg = json.loads(line.strip())
            if msg.get("message_type") == "file_query":
                query_id = msg.get("message_id")
                requested_file = msg.get("file_name")
                origin_id = msg.get("origin_id")
                ttl = msg.get("ttl", 5)
                print(f"[{leaf_node_id}] Received file_query: {msg}")
                
                # Check if the requested file exists
                if requested_file in available_files:
                    response_msg = {
                        "message_type": "query_hit",
                        "message_id": query_id,
                        "responding_id": leaf_node_id
                    }
                    sock.sendall((json.dumps(response_msg) + '\n').encode())
                    print(f"[{leaf_node_id}] Sent QueryHitMessage for MessageID {query_id} to Super-Peer.")
                else:
                    # Optionally send a QueryMissMessage or ignore
                    print(f"[{leaf_node_id}] File {requested_file} not found. Ignoring query.")
        except json.JSONDecodeError as e:
            print(f"[{leaf_node_id}] Failed to decode JSON message: {e}")

def main():
    args = parse_arguments()
    
    leaf_node_id = args.id
    leaf_node_address = args.address
    leaf_node_port = args.port
    super_peer_id = args.super_peer_id
    super_peer_address = args.super_peer_address
    super_peer_port = args.super_peer_port
    file_to_register = args.file
    
    available_files = [file_to_register]  # List of files this leaf node has
    
    try:
        # Establish connection to Super-Peer
        print(f"[{leaf_node_id}] Connecting to Super-Peer {super_peer_id} at {super_peer_address}:{super_peer_port}...")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((super_peer_address, super_peer_port))
        print(f"[{leaf_node_id}] Connected to Super-Peer {super_peer_id}.")
        
        # Send FileRegistrationMessage
        send_file_registration(sock, leaf_node_id, [{"file_name": f, "file_size": 1024} for f in available_files])
        
        # Start thread to handle incoming queries
        query_handler = threading.Thread(target=handle_queries, args=(sock, leaf_node_id, available_files))
        query_handler.daemon = True
        query_handler.start()
        print(f"[{leaf_node_id}] Started query handler thread.")
        
        # Keep the main thread alive
        while True:
            time.sleep(1)
    
    except Exception as e:
        print(f"[{leaf_node_id}] Encountered an error: {e}")
    finally:
        sock.close()
        print(f"[{leaf_node_id}] Connection closed.")

if __name__ == "__main__":
    main()
