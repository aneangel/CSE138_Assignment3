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
KV_STORE = defaultdict(
    lambda: {
        'value': None,
        'vectorClock': defaultdict(int)
    }
)


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
def is_causually_after(vectorClock1, vectorClock2):
    """
    Checks if causalMetadata1 is causually after causalMetadata2.

    Keyword arguments:
    causalMetadata1 -- the first causal metadata
    causalMetadata2 -- the second causal metadata
    """
    for ip in vectorClock1:
        if vectorClock1[ip] < vectorClock2[ip]:
            app.logger.info(
                f"is_casually_after condition failed: vectorclock1 ip '{vectorClock1[ip]}' < vectorclock2 ip '{vectorClock2[ip]}'")

            return False

    app.logger.info("is_casually_after conditions satisfied")
    return True


def handleUnreachableReplica(address: str) -> None:
    """
    Handles the case when a replica is unreachable.

    Keyword arguments:
    address -- the IP:PORT of the unreachable replica
    """
    global VIEW

    app.logger.error(
        "Could not reach replica %s, removing from view", address)

    VIEW.remove(address)
    broadcastReplicaState(address, "DELETE")


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
            # TODO: more robust detection mechanism
            handleUnreachableReplica(address)
            continue

        except requests.exceptions.RequestException as e:
            abort(e.response.status_code,
                  f"Unexpected error while broadcasting replica state to {address}: {e}")

        except Exception as e:
            app.logger.error("Unexpected error: %s", e)
            print(
                f"Unexpected error will long-polling Replica {address}: {e}")
            abort(500, e)

    app.logger.info("Replica state broadcasted successfully")


def broadCastPutKeyReplica(key, value, vectorClock):
    """
    Broadcast the PUT request to all the other replicas in the view.

    Keyword arguments:
    key -- the key to be added
    value -- the value to be added
    causalMetadata -- the causal metadata to be broadcasted
    """
    global CURRENT_ADDRESS, CONNECTION_TIMEOUT, LONG_POLLING_WAIT, VIEW

    app.logger.info(
        f"Broadcasting PUT '{key}':'{value}' with vectorClock {dict.__repr__(vectorClock)} ")

    for address in VIEW:
        if address == CURRENT_ADDRESS:
            continue

        while True:
            try:
                app.logger.info("Broadcasting kv pair to %s", address)

                response = requests.put(
                    f"http://{address}/kvs/{key}/nobroadcast",
                    headers={"Content-Type": "application/json"},
                    json={
                        "value": value,
                        "vectorClock": vectorClock},
                    timeout=(CONNECTION_TIMEOUT, None)
                )

                response.raise_for_status()

                if response.status_code == 200 or response.status_code == 201:
                    app.logger.info(f"Replica {address} successfully updated.")
                    vectorClock[address] += 1
                    break

                app.logger.info(
                    f"Recieved status code '{response.status_code}' from Replica {address}.  Continuing long-polling...")

            except requests.Timeout:
                app.logger.info(
                    f"Request timeout; waiting for updates from Replica {address}. Continuing long-polling...")

                time.sleep(LONG_POLLING_WAIT)

            except requests.exceptions.ConnectionError as e:
                # TODO: more robust detection mechanism
                handleUnreachableReplica(address)
                break

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 503:
                    app.logger.info(
                        f"Replica {address} is not ready; continuing long-polling...")
                    time.sleep(LONG_POLLING_WAIT)
                    continue

                abort(e.response.status_code,
                      f"Unexpected error while broadcasting replica state to {address}: {e}")

            except requests.exceptions.RequestException as e:
                abort(e.response.status_code,
                      f"Unexpected error while broadcasting replica state to {address}: {e}")

            except Exception as e:
                app.logger.error("Unexpected error: %s", e)
                print(
                    f"Unexpected error will long-polling Replica {address}: {e}")
                abort(500, e)

    app.logger.info("Replica state broadcasted successfully")


# Helper routes
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


@app.route('/view/nobroadcast', methods=['PUT'])
def addToReplicaView():
    global VIEW

    data = request.get_json()
    newAddress = data['socket-address']

    if newAddress in VIEW:
        return {"result": "already present"}, 200

    VIEW.append(newAddress)
    return {'result': 'added'}, 201


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
        abort(404, "View has no such replica")

    broadcastReplicaState(deletedAddress, "DELETE")

    VIEW.remove(deletedAddress)

    return {"result": "deleted"}, 200


@app.route('/view/nobroadcast', methods=['DELETE'])
def deleteFromReplicaView():
    global VIEW

    app.logger.debug("current view: %s", VIEW)

    data = request.get_json()
    deletedAddress = data['socket-address']

    if deletedAddress not in VIEW:
        abort(404, "View has no such replica")

    app.logger.info(f"deleting {deletedAddress} from current view")

    VIEW.remove(deletedAddress)
    return {"result": "deleted"}, 200


# Key-Value Store Operations
@app.route('/kvs/<key>', methods=['PUT'])
def addKey(key):
    global KV_STORE

    validate_key_length(key)

    app.logger.debug(f"begin PUT '{key}'")

    data = request.get_json()
    value = data['value']
    causalMetadata = data['causal-metadata']

    incomingVectorClock = causalMetadata[key]['vectorClock'] if causalMetadata and key in causalMetadata else None

    if incomingVectorClock is not None:
        app.logger.debug(
            f"PUT '{key}':'{value}' with vectorClock {dict.__repr__(incomingVectorClock)}")

    entry = KV_STORE[key]
    currentVectorClock = entry['vectorClock']

    app.logger.debug(
        f"Current entry vectorClock: {dict.__repr__(currentVectorClock)}")

    if incomingVectorClock is not None and not is_causually_after(incomingVectorClock, currentVectorClock):
        abort(503, "Causal dependencies not satisfied; try again later")

    # TODO: check if this is correct; no casual dependencies means always new?
    isNewKey = incomingVectorClock is None or key not in KV_STORE

    app.logger.debug(f"Key '{key}' is new: {isNewKey}")

    entry['value'] = value
    entry['vectorClock'][CURRENT_ADDRESS] += 1

    broadCastPutKeyReplica(key, value, entry['vectorClock'])

    if isNewKey:
        return {'result': 'created', 'causal-metadata': entry['vectorClock']}, 201

    return {'result': "replaced", "causal-metadata": entry['vectorClock']}, 200


@app.route('/kvs/<key>/nobroadcast', methods=['PUT'])
def addKeyToReplica(key):
    global KV_STORE

    data = request.get_json()
    value = data['value']
    incomingVectorClock = data['vectorClock']

    app.logger.info(
        f"Replicate PUT '{key}':'{value}' with vectorClock {dict.__repr__(incomingVectorClock)}")

    doesCurrentKeyExist = key in KV_STORE

    entry = KV_STORE[key]
    currentVectorClock = entry['vectorClock']

    app.logger.info(
        f"Current entry vectorClock: {dict.__repr__(currentVectorClock)}")

    if doesCurrentKeyExist and not is_causually_after(incomingVectorClock,  currentVectorClock):
        abort(503, "Causal dependencies not satisfied; try again later")

    entry['value'] = value
    currentVectorClock[CURRENT_ADDRESS] += 1

    return {'result': "successfully propigated PUT"}, 200


@app.route('/kvs/<key>', methods=['GET'])
def getKey(key):
    global KV_STORE, vectorClock

    validate_key_exists(key)

    data = request.get_json()
    causalMetadata = data['causal-metadata']

    # if Counter(vectorClock) != Counter(
    #         causalMetadata) and causalMetadata is not None:
    #     abort(503, f"Causal dependencies not satisfied; try again later")

    # return {'result': 'found',
    #         'value': KV_STORE[key], 'causal-metadata': vectorClock}, 200


@app.route('/kvs/<key>', methods=['DELETE'])
def deleteKey(key):
    global KV_STORE

    validate_key_exists(key)

    data = request.get_json()
    causalMetadata = data['causal-metadata']

    # del KV_STORE[key]

    # return {'result': 'deleted', 'causal-metadata': '<V>'}


@app.route('/kvs', methods=['GET'])
def getKVStore():
    global KV_STORE

    return {'causal-metadata': KV_STORE}, 200


# Main
if __name__ == '__main__':
    app.run(debug=True, port=int(CURRENT_ADDRESS.split(
        ':')[1]), host=CURRENT_ADDRESS.split(':')[0])
