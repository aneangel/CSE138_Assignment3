# server.py
from flask import Flask, jsonify, request
from server_nodes.server import MultiMasterKeyValueStore

app = Flask(__name__)
nodes = ["http://localhost:8082", "http://localhost:8083", "http://localhost:8084"]
kv_store = MultiMasterKeyValueStore(node_id="http://localhost:8090", nodes=nodes)


@app.route('/kvs/<key>', methods=['PUT'])
def put(key):
    data = request.get_json()
    value = data.get('value')
    causal_metadata = data.get('causal-metadata')

    # Check causal dependencies using MultiMasterKeyValueStore
    if causal_metadata is not None and not kv_store.dependencies_satisfied(causal_metadata):
        return jsonify({"error": "Causal dependencies not satisfied; try again later"}), 503

    # Perform the put operation
    result = kv_store.put(key, value)

    return jsonify(result), 201


@app.route('/kvs/<key>', methods=['GET'])
def get(key):
    causal_metadata = request.get_json().get('causal-metadata')

    # Check causal dependencies using MultiMasterKeyValueStore
    if causal_metadata is not None and not kv_store.dependencies_satisfied(causal_metadata):
        return jsonify({"error": "Causal dependencies not satisfied; try again later"}), 503

    # Perform the get operation
    result = kv_store.get(key)

    return jsonify(result), 200


@app.route('/replicate', methods=['POST'])
def replicate():
    data = request.get_json()
    operation = data.get('operation', {})
    result = kv_store.replicate_operation(operation)

    return jsonify(result), 200


# Add your other necessary routes and functions here

if __name__ == '__main__':
    app.run(port=8090)
