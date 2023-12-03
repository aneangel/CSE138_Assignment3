from flask import Flask, request, jsonify, abort
from enum import Enum
import requests
import time
import os
from collections import defaultdict, Counter
import asyncio

app = Flask(__name__)

# Get environment variables
VIEW_ENV = os.environ.get('VIEW', '')
VIEW = VIEW_ENV.split(',')
CURRENT_ADDRESS = os.environ.get('SOCKET_ADDRESS', '')

# Constants
REPLICA_OPERATION = Enum('REPLICA_OPERATION', ['ADD', 'DELETE'])
CONNECTION_TIMEOUT = 5
LONG_POLLING_WAIT = 3

# Initialize global state
KV_STORE = {}
VECTOR_CLOCK = defaultdict(int)


# Error Handling
@app.errorhandler(Exception)
def handle_exception(error: Exception) -> [{'error': str}, int]:
    return {'error': error.description}, error.code


def validate_key_length(key: str) -> None:
    """
    Validates the length of the key.

    Keyword arguments:
    key -- the key to be validated
    """
    if len(key) > 50:
        abort(400, "Key is too long")


def validate_key_exists(key: str) -> None:
    """
    Validates that the key exists in the KV Store.

    Keyword arguments:
    key -- the key to be validated
    """
    if key not in KV_STORE:
        abort(404, "Key does not exist")


# Helper Functions
def broadcastReplicaState(broadcastAddress: str,
                          operation: REPLICA_OPERATION) -> None:
    """
    Brodcast the request to add/delete the new replica IP:PORT to all the other
    replicas.

    Keyword arguments:
    address -- the IP:PORT of the replica to be added/deleted
    operation -- the operation to be performed on the replica ("ADD" OR "DELETE")
    """
    if operation not in REPLICA_OPERATION:
        abort(
            500,
            f"Invalid replica brodcast operation: {operation} not in {REPLICA_OPERATION}")

    # Construct request parameters
    request = ""
    endpoint = ""
    if operation == REPLICA_OPERATION.ADD:
        request = "PUT"
        endpoint = "addToReplicaView"
    elif operation == REPLICA_OPERATION.DELETE:
        request = "DELETE"
        endpoint = "deleteFromReplicaView"

    json = {"socket-address": broadcastAddress},

    timeout = (CONNECTION_TIMEOUT, None)

    # Broadcast request to all other replicas
    for address in VIEW:
        if address == CURRENT_ADDRESS:
            continue

        try:
            response = requests.request(
                request, f"http://{address}/{endpoint}", json, timeout)
            response.raise_for_status()

        except requests.exceptions.Timeout as e:
            # Replica is down; remove from view
            # TODO: more robust detection mechanism
            VIEW.remove(address)
            broadcastReplicaState(address, REPLICA_OPERATION.DELETE)
            continue

        except requests.exceptions.ConnectionError as e:
            # TODO: Handle this case; wait for it to come back up? Shutdown
            # replica?
            abort(500, f"Current replica {CURRENT_ADDRESS} is down.")

        except Exception as e:
            print(
                f"Unexpected error will long-polling Replica {address}: {e}")
            abort(500, e)


def broadCastPutKeyReplica(key, value, causalMetadata):
    """
    Broadcast the PUT request to all the other replicas in the view.

    Keyword arguments:
    key -- the key to be added
    value -- the value to be added
    causalMetadata -- the causal metadata to be broadcasted
    """
    # TODO: detection mechanism for when a replica is down
    for address in VIEW:
        if address == CURRENT_ADDRESS:
            continue

        while True:
            try:
                response = requests.put(
                    f"http://{address}/kvs/updateVectorClock",
                    json={
                        "key": key,
                        "value": value,
                        "causalMetadata": causalMetadata},
                    timeout=(CONNECTION_TIMEOUT, None))

                response.raise_for_status()

                if response.status_code == 200 or response.status_code == 201:
                    print(f"Replica {address} successfully updated.")
                    VECTOR_CLOCK[address] += 1
                    return

                print(
                    f"Recieved status code '{response.status_code}' from Replica {address}.  Continuing long-polling...")

            except requests.Timeout:
                print(
                    f"Request timeout; waiting for updates from Replica {address}. Continuing long-polling...")

                time.sleep(LONG_POLLING_WAIT)

            except requests.exceptions.ConnectionError as e:
                # TODO: Handle this case; wait for it to come back up? Shutdown
                # replica?
                abort(500, f"Current replica {CURRENT_ADDRESS} is down.")

            except Exception as e:
                print(
                    f"Unexpected error whill long-polling Replica {address}: {e}")
                abort(500, e)


# Helper routes
@app.route('/deleteFromReplicaView', methods=['PUT'])
def deleteFromReplicaView():
    data = request.get_json()
    deletedAddress = data['socket-address']

    if deletedAddress not in VIEW:
        return {"error": "View has no such replica"}, 404

    VIEW.remove(deletedAddress)
    return {"result": "deleted"}, 200


@app.route('/addToReplicaView', methods=['DELETE'])
def addToReplicaView():
    data = request.get_json()
    newAddress = data['socket-address']

    if newAddress in VIEW:
        return {"result": "already present"}, 200

    VIEW.append(newAddress)
    return {'result': 'added'}, 201


@app.route('/kvs/updateVectorClock', methods=['PUT'])
def addKeyToReplica():
    data = request.get_json()
    key = data['key']
    value = data['value']
    causalMetadata = data['causalMetadata']

    if Counter(VECTOR_CLOCK) != Counter(causalMetadata):
        return {"error": "invalid metadata"}, 503

    isNewKey = key not in KV_STORE

    KV_STORE[key] = value
    VECTOR_CLOCK[CURRENT_ADDRESS] += 1

    if isNewKey:
        return {'result': 'created', 'causal-metadata': VECTOR_CLOCK}, 201

    return {'result': "replaced", "causal-metadata": VECTOR_CLOCK}, 200


# View Operations
@app.route('/view', methods=['PUT'])
def addReplica():
    """"Adds a new replica to the view."""
    data = request.get_json()
    newAddress = data['socket-address']

    if newAddress in VIEW:
        return {"result": "already present"}, 200

    broadcastReplicaState(newAddress, REPLICA_OPERATION.ADD)

    VIEW.append(newAddress)

    return {"result": "added"}, 201


@app.route('/view', methods=['GET'])
def getReplica():
    """Retrieve the view from a replica."""
    return {"view": VIEW}, 200


@app.route("/view", methods=['DELETE'])
def deleteReplica():
    """Removes an existing replica from the view."""
    data = request.get_json()
    deletedAddress = data['socket-address']

    if deletedAddress not in VIEW:
        return {"error": "View has no such replica"}, 404

    VIEW.remove(deletedAddress)
    broadcastReplicaState(deletedAddress, REPLICA_OPERATION.DELETE)
    return {"result": "deleted"}, 200


# Key-Value Store Operations
@app.route('/kvs/<key>', methods=['PUT'])
def addKey(key):
    validate_key_length(key)

    data = request.get_json()
    value = data['value']
    causalMetadata = data['causal-metadata']

    if Counter(VECTOR_CLOCK) != Counter(causalMetadata):
        return {"error": "Causal dependencies not satisfied; try again later"}, 503

    isNewKey = key not in KV_STORE

    KV_STORE[key] = value
    VECTOR_CLOCK[CURRENT_ADDRESS] += 1

    broadCastPutKeyReplica(key, value, causalMetadata)

    if isNewKey:
        return {'result': 'created', 'causal-metadata': VECTOR_CLOCK}, 201

    return {'result': "replaced", "causal-metadata": VECTOR_CLOCK}, 200


@app.route('/kvs/<key>', methods=['GET'])
def getKey(key):
    validate_key_exists(key)

    data = request.get_json()
    causalMetadata = data['causal-metadata']

    if Counter(VECTOR_CLOCK) != Counter(
            causalMetadata) and causalMetadata is not None:
        return {"error": "Causal dependencies not satisfied; try again later",
                "vector clock": VECTOR_CLOCK}, 503

    return {'result': 'found',
            'value': KV_STORE[key], 'causal-metadata': VECTOR_CLOCK}, 200


@app.route('/kvs/<key>', methods=['DELETE'])
def deleteKey(key):
    validate_key_exists(key)

    data = request.get_json()
    causalMetadata = data['causal-metadata']

    del KV_STORE[key]

    return {'result': 'deleted', 'causal-metadata': '<V>'}


# Main
if __name__ == '__main__':
    print(int(CURRENT_ADDRESS.split(':')[1]))
    print(CURRENT_ADDRESS.split(':')[0])
    app.run(debug=True, port=int(CURRENT_ADDRESS.split(
        ':')[1]), host=CURRENT_ADDRESS.split(':')[0])
