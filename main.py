# server.py
from flask import Flask, jsonify, request
from server_nodes.server import MultiMasterKeyValueStore

app = Flask(__name__)
kv_store = MultiMasterKeyValueStore("node1",
                                    ["http://localhost:8082", "http://localhost:8083", "http://localhost:8084"])


@app.route('/put', methods=['POST'])
def put():
    data = request.get_json()
    key = data.get('key')
    value = data.get('value')

    kv_store.put(key, value)
    return jsonify({"status": "success"})


@app.route('/get/<key>', methods=['GET'])
def get(key):
    result = kv_store.get(key)
    return jsonify(result)


@app.route('/replicate', methods=['POST'])
def replicate():
    data = request.get_json()
    operation = data.get('operation', {})
    result = kv_store.replicate_operation(operation)
    return jsonify(result)


if __name__ == '__main__':
    app.run(port=8082)  # Change the port for each replica
