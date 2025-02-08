from flask import Flask, request, jsonify
import threading
import requests
from utils import hash_function

app = Flask(__name__)

class ChordNode:
    def __init__(self, ip, port, bootstrap_ip=None, bootstrap_port=None):
        self.ip = ip
        self.port = port
        self.node_id = hash_function(f"{ip}:{port}")
        self.successor = (self.ip, self.port)  # Bootstrap node starts as its own successor

        if (bootstrap_ip is None):
            self.is_bootstrap = True
            self.predecessor = (self.ip, self.port)  # ✅ Bootstrap node predecessor is itself
            print(f"Bootstrap Node started: ID {self.node_id}, IP {self.ip}:{self.port}")
        else:
            self.is_bootstrap = False
            self.predecessor = None
            print(f"Joining Chord via Bootstrap Node {bootstrap_ip}:{bootstrap_port}")
            self.join_ring(bootstrap_ip, bootstrap_port)


    def join_ring(self, bootstrap_ip, bootstrap_port):
        """Join an existing Chord ring via the bootstrap node."""
        response = requests.post(f"http://{bootstrap_ip}:{bootstrap_port}/join",
                                 json={"node_id": self.node_id, "ip": self.ip, "port": self.port})

        if response.status_code == 200:
            data = response.json()
            self.successor = (data["successor_ip"], data["successor_port"])
            self.predecessor = (data["predecessor_ip"], data["predecessor_port"])
            print(f"✅ Joined ring: Successor -> {self.successor}, Predecessor -> {self.predecessor}")

            # Notify successor to update predecessor
            requests.post(f"http://{self.successor[0]}:{self.successor[1]}/update_predecessor",
                          json={"new_predecessor_ip": self.ip, "new_predecessor_port": self.port})



    def find_successor(self, key):
        """Find the responsible node for a given key."""
        if self.successor is None or self.successor == self:
            return self  # Επιστρέφει τον εαυτό του αν είναι ο μόνος κόμβος

        if self.node_id < key <= self.successor.node_id:
            return self.successor

        # Αν ο successor είναι ίδιος με τον τρέχοντα κόμβο, επιστρέφουμε τον εαυτό μας
        if self.successor == self:
            return self

        return self.successor.find_successor(key)


    def insert(self, key, value):
        """Insert a song into the DHT."""
        key_hash = self.hash_function(key)
        responsible_node = self.find_successor(key_hash)
        responsible_node.data_store[key] = value

    def query(self, key):
        """Find which node has a given song."""
        key_hash = self.hash_function(key)
        responsible_node = self.find_successor(key_hash)
        return responsible_node.data_store.get(key, "Not found")

    def delete(self, key):
        """Delete a song from the DHT."""
        key_hash = self.hash_function(key)
        responsible_node = self.find_successor(key_hash)
        if key in responsible_node.data_store:
            del responsible_node.data_store[key]
            return "Deleted"
        return "Not found"

    def depart(self):
        """Graceful departure: transfer data to the successor."""
        if self.successor and self.successor != self:
            for key, value in self.data_store.items():
                self.successor.insert(key, value)
        self.predecessor.successor = self.successor
        self.successor.predecessor = self.predecessor

    def get_all_songs(self):
        """Επιστρέφει όλα τα τραγούδια που είναι αποθηκευμένα στον κόμβο και στους successors."""
        songs = {f"Node {self.node_id}": self.data_store}  # Αποθήκευση τοπικών δεδομένων

        current = self.successor
        while current and current != self:
            songs[f"Node {current.node_id}"] = current.data_store
            current = current.successor

        return songs


# Flask API Endpoints

@app.route("/", methods=["GET"])
def initial_route():
    return jsonify({"message": f"Port {node.port} is working"}), 200



@app.route('/is_bootstrap', methods=['GET'])
def check_bootstrap():
    """Check if this node is the bootstrap node."""
    return jsonify({"is_bootstrap": node.is_bootstrap}), 200


@app.route("/query/<string:title>", methods=["GET"])
def query(title):
    """Find which node has a given song or return all stored data."""
    try:
        if title == "*":
            return jsonify({"songs_per_node": node.get_all_songs()})  # ✅ Επιστροφή όλων των τραγουδιών

        location = node.query(title)
        return jsonify({"location": location})
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route("/delete/<string:title>", methods=["DELETE"])
def delete(title):
    """Διαγραφή ενός τραγουδιού από το DHT."""
    try:
        message = node.delete(title)
        return jsonify({"message": message})  # ΠΑΝΤΑ επιστρέφει JSON
    except Exception as e:
        return jsonify({"error": str(e)}), 500  # Επιστρέφει JSON error αν υπάρξει σφάλμα



@app.route("/depart", methods=["POST"])
def depart():
    """Ένας κόμβος αποχωρεί από το δίκτυο."""
    node.depart()
    return jsonify({"message": "Node departed gracefully"}), 200



@app.route("/insert", methods=["POST"])
def insert():
    """Εισαγωγή ενός τραγουδιού στο δίκτυο."""
    data = request.json
    key = data.get("title")
    value = data.get("location")
    node.insert(key, value)
    return jsonify({"message": "Song inserted"}), 200


@app.route('/join', methods=['POST'])
def handle_join():
    """Handles new nodes joining the Chord ring."""
    data = request.json
    new_node_ip = data["ip"]
    new_node_port = data["port"]
    new_node_id = data["node_id"]

    # Find current successor
    successor_ip, successor_port = node.successor

    # Update successor pointer for new node
    response_data = {
        "successor_ip": successor_ip,
        "successor_port": successor_port,
        "predecessor_ip": node.ip,
        "predecessor_port": node.port
    }

    # Update my successor to be the new node
    node.successor = (new_node_ip, new_node_port)

    print(f"🔄 Node {new_node_ip}:{new_node_port} with id {new_node_id} joined! New successor: {node.successor}")

    return jsonify(response_data), 200



@app.route('/update_predecessor', methods=['POST'])
def update_predecessor():
    """Updates the predecessor of the current node."""
    data = request.json
    node.predecessor = (data["new_predecessor_ip"], data["new_predecessor_port"])
    return jsonify({"message": "Predecessor updated"}), 200




@app.route('/successor', methods=['GET'])
def get_successor():
    """Returns the successor of this node."""
    return jsonify({"successor": node.successor}), 200



@app.route('/predecessor', methods=['GET'])
def get_predecessor():
    """Returns the predecessor of this node."""
    return jsonify({"predecessor": node.predecessor}), 200



@app.route('/overlay', methods=['GET'])
def get_overlay():
    """Returns the topology of the Chord ring in the correct order, including node IDs, successors, and predecessors."""
    nodes = []
    current_ip, current_port, current_id = node.ip, node.port, node.node_id
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
        if not next_ip or not next_port or (next_ip, next_port) == (node.ip, node.port):
            break  # Stop if we reach the bootstrap node again (full cycle completed)

        current_ip, current_port = next_ip, next_port
        current_id = hash_function(f"{current_ip}:{current_port}")  # Compute node ID

    return jsonify({"overlay": nodes}), 200





if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True, help="Port number for this node")
    parser.add_argument("--bootstrap_ip", type=str, default=None, help="IP of the bootstrap node")
    parser.add_argument("--bootstrap_port", type=int, default=None, help="Port of the bootstrap node")
    args = parser.parse_args()

    node = ChordNode("127.0.0.1", args.port, args.bootstrap_ip, args.bootstrap_port)
    app.run(host="127.0.0.1", port=args.port)
