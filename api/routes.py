# api/routes.py
from flask import Blueprint, request, jsonify
import requests

routes = Blueprint("routes", __name__)  # Use Blueprint for modularity

@routes.route('/successor', methods=['GET'])
def get_successor():
    """Returns the successor of this node."""
    return jsonify({"successor": chord_node.successor}), 200


@routes.route('/predecessor', methods=['GET'])
def get_predecessor():
    """Returns the predecessor of this node."""
    return jsonify({"predecessor": chord_node.predecessor}), 200


@routes.route('/update_predecessor', methods=['POST'])
def update_predecessor():
    """Updates the predecessor of the current node."""
    data = request.json
    chord_node.predecessor = (data["new_predecessor_ip"], data["new_predecessor_port"])
    return jsonify({"message": "Predecessor updated"}), 200



@routes.route('/overlay', methods=['GET'])
def get_overlay():
    """Returns the topology of the Chord ring in the correct order, including node IDs, successors, and predecessors."""
    nodes = []
    current_ip, current_port, current_id = chord_node.ip, chord_node.port, chord_node.node_id
    visited = set()

    while (current_ip, current_port, current_id) not in visited:
        visited.add((current_ip, current_port, current_id))

        # Fetch successor and predecessor of the current node
        try:
            successor_response = requests.get(f"http://{current_ip}:{current_port}/successor", timeout=2)
            successor_data = successor_response.json().get("successor", (None, None))

            predecessor_response = requests.get(f"http://{current_ip}:{current_port}/predecessor", timeout=2)
            predecessor_data = predecessor_response.json().get("predecessor", (None, None))
        except requests.exceptions.RequestException:
            print(f"⚠️ Warning: Node {current_ip}:{current_port} is unreachable. Skipping...")
            break

        node_info = {
            "ip": current_ip,
            "port": current_port,
            "ID": current_id,
            "successor": {"ip": successor_data[0], "port": successor_data[1]},
            "predecessor": {"ip": predecessor_data[0], "port": predecessor_data[1]}
        }

        nodes.append(node_info)

        # Move to the next node in the ring
        next_ip, next_port = successor_data
        if not next_ip or not next_port or (next_ip, next_port) == (chord_node.ip, chord_node.port):
            break  # Stop if we reach the bootstrap node again (full cycle completed)

        current_ip, current_port = next_ip, next_port
        current_id = chord_node.hash_id(f"{current_ip}:{current_port}")  # Compute node ID

    return jsonify({"overlay": nodes}), 200



@routes.route('/join', methods=['POST'])
def join():
    """Handles new nodes joining the Chord ring."""
    data = request.json
    new_node_ip = data["ip"]
    new_node_port = data["port"]
    new_node_id = data["node_id"]

    # Ensure successor exists
    successor_ip, successor_port = chord_node.successor

#     # Find the correct successor for the new node
#     if chord_node.node_id < new_node_id < chord_node.hash_id(f"{successor_ip}:{successor_port}"):
#         correct_successor = (successor_ip, successor_port)
#     else:
#         # Forward the request to find the correct successor in the ring
#         response = requests.get(f"http://{successor_ip}:{successor_port}/get_successor",
#                                 params={"node_id": new_node_id})
#         correct_successor = response.json().get("successor", (None, None))
#
#     # Find the correct predecessor (previous node in the ring)
#     correct_predecessor = (chord_node.ip, chord_node.port)  # Assume current node is predecessor

    # Inform the new node about its correct successor and predecessor
    response_data = {
        "successor_ip": successor_ip,
        "successor_port": successor_port,
        "predecessor_ip": chord_node.ip,
        "predecessor_port": chord_node.port
    }

    # Update the new node’s predecessor
    chord_node.successor = (new_node_ip, new_node_port)

#     # Notify the correct successor to update its predecessor
#     requests.post(f"http://{correct_successor[0]}:{correct_successor[1]}/update_predecessor",
#                   json={"new_predecessor_ip": new_node_ip, "new_predecessor_port": new_node_port})
#
#     print(f"🔄 Node {new_node_ip}:{new_node_port} (ID {new_node_id}) joined! "
#           f"New Successor: {correct_successor}, New Predecessor: {correct_predecessor}")

    return jsonify(response_data), 200



# Function to set the chord_node instance dynamically
def set_chord_node(node_instance):
    global chord_node
    chord_node = node_instance