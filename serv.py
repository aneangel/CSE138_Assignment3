from flask import Flask, request, jsonify, abort
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
CONNECTION_TIMEOUT = 5
LONG_POLLING_WAIT = 3

# Initialize global state
KV_STORE = {}
VECTOR_CLOCK = defaultdict(int)


# Error Handling
@app.errorhandler(Exception)
def handle_exception(error: Exception) -> [{'error': str}, int]:
    if (not hasattr(error, "code") or not hasattr(error, "description")):
        app.logger.error("Unexpected error: %s", str(error))
        return {'error': "unexpected server error"}, 500

    app.logger.error("Unexpected error: %s", error.description)
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
    global KV_STORE

    if key not in KV_STORE:
        abort(404, "Key does not exist")


# Helper Functions
def broadcastReplicaState(broadcastAddress: str,
                          request: str) -> None:
    """
    Brodcast the request to add/delete the new replica IP:PORT to all the other
    replicas.

    Keyword arguments:
    address -- the IP:PORT of the replica to be added/deleted
    operation -- the operation to be performed on the replica ("ADD" OR "DELETE")
    """
    global CURRENT_ADDRESS, CONNECTION_TIMEOUT, VIEW

    app.logger.debug("current view: %s", VIEW)

    if (len(VIEW) == 1):
        # Sanity Check
        assert CURRENT_ADDRESS == VIEW[0], "Only one address in view; should be current address not %s" % VIEW[0]

        app.logger.info(
            "View only contains current address; skipping broadcast replica state")
        return

    app.logger.info(
        f"Broadcast replica state; {request} address {broadcastAddress}")

    if request != "PUT" and request != "DELETE":
        abort(
            500,
            f"Invalid replica brodcast operation: {request}")

    # Broadcast request to all other replicas
    for address in VIEW:
        if address == CURRENT_ADDRESS:
            continue

        if request == "DELETE" and address == broadcastAddress:
            continue

        try:
            app.logger.info("Broadcasting replica state to %s", address)

            response = requests.request(
                request,
                f"http://{address}/view/nobroadcast",
                headers={"Content-Type": "application/json"},
                json={"socket-address": broadcastAddress}
            )

            response.raise_for_status()

            app.logger.info(
                f"Successfully broadcasted replica state to {address}")

        except requests.exceptions.ConnectionError as e:
            # Replica is down; remove from view
            # TODO: more robust detection mechanism
            app.logger.error("Could not reach replica %s", address)
            app.logger.info("Replica %s is down; removing from view", address)
            VIEW.remove(address)
            broadcastReplicaState(address, "DELETE")
            continue

        except requests.exceptions.RequestException as e:
            abort(e.response.status_code,
                  f"Unexpected error while brodcasting replica state to {address}: {e}")

        except Exception as e:
            app.logger.error("Unexpected error: %s", e)
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
    global CURRENT_ADDRESS, CONNECTION_TIMEOUT, LONG_POLLING_WAIT, VECTOR_CLOCK, VIEW
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
@app.route('/kvs/updateVectorClock', methods=['PUT'])
def addKeyToReplica():
    global KV_STORE, VECTOR_CLOCK

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
    global VIEW

    data = request.get_json()
    newAddress = data['socket-address']

    if newAddress in VIEW:
        return {"result": "already present"}, 200

    broadcastReplicaState(newAddress, "PUT")

    VIEW.append(newAddress)

    return {"result": "added"}, 201


@app.route('/view', methods=['GET'])
def getReplica():
    """Retrieve the view from a replica."""
    global VIEW

    return {"view": VIEW}, 200


@app.route("/view", methods=['DELETE'])
def deleteReplica():
    """Removes an existing replica from the view."""
    global VIEW

    data = request.get_json()
    deletedAddress = data['socket-address']

    if deletedAddress not in VIEW:
        return {"error": "View has no such replica"}, 404

    broadcastReplicaState(deletedAddress, "DELETE")

    VIEW.remove(deletedAddress)

    return {"result": "deleted"}, 200


@app.route('/view/nobroadcast', methods=['DELETE'])
def deleteFromReplicaView():
    global VIEW

    app.logger.info("current view: %s", VIEW)

    data = request.get_json()
    deletedAddress = data['socket-address']

    if deletedAddress not in VIEW:
        return {"error": "View has no such replica"}, 404

    app.logger.info("about to delete %s", deletedAddress)

    VIEW.remove(deletedAddress)
    return {"result": "deleted"}, 200


# Key-Value Store Operations
@app.route('/kvs/<key>', methods=['PUT'])
def addKey(key):
    global KV_STORE, VECTOR_CLOCK

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


@app.route('/view/nobroadcast', methods=['PUT'])
def addToReplicaView():
    global VIEW

    data = request.get_json()
    newAddress = data['socket-address']

    if newAddress in VIEW:
        return {"result": "already present"}, 200

    VIEW.append(newAddress)
    return {'result': 'added'}, 201


@app.route('/kvs/<key>', methods=['GET'])
def getKey(key):
    global KV_STORE, VECTOR_CLOCK

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
    global KV_STORE

    validate_key_exists(key)

    data = request.get_json()
    causalMetadata = data['causal-metadata']

    del KV_STORE[key]

    return {'result': 'deleted', 'causal-metadata': '<V>'}


# Main
if __name__ == '__main__':
    app.run(debug=True, port=int(CURRENT_ADDRESS.split(
        ':')[1]), host=CURRENT_ADDRESS.split(':')[0])
