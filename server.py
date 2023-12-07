from flask import Flask, request, jsonify, abort
import requests
import time
import os
from lib.KVStore import KVStore, VectorClock

replica = Flask(__name__)

# Get environment variables
VIEW_ENV = os.environ.get('VIEW', '')
VIEW = VIEW_ENV.split(',')
CURRENT_ADDRESS = os.environ.get('SOCKET_ADDRESS', '')

# Constants
CONNECTION_TIMEOUT = 5
LONG_POLLING_WAIT = 3

# Initialize KV Store
gloabl_kv_store = KVStore()


# Error Handling
@replica.errorhandler(Exception)
def handle_exception(error: Exception) -> [{'error': str}, int]:
    if (not hasattr(error, "code") or not hasattr(error, "description")):
        replica.logger.error("Unexpected error: %s", str(error))
        return {'error': "unexpected server error"}, 500

    replica.logger.error("Unexpected error: %s", error.description)
    return {'error': error.description}, error.code


def validate_key_length(key: str) -> None:
    """
    Validates the length of the key.

    Keyword arguments:
    key -- the key to be validated
    """
    if len(key) > 50:
        abort(400, "Key is too long")


def validate_value(value: str):
    """
    Validates the length of the value.

    Keyword arguments:
    value -- the value to be validated
    """
    if value is None:
        abort(400, "Improperly formated request: 'value' is missing in body")

    if len(value) > 1000:
        abort(400, "Value is too long")


def validate_key_exists(key: str) -> None:
    """
    Validates that the key exists in the KV Store.

    Keyword arguments:
    key -- the key to be validated
    """
    global global_kv_store

    if key not in global_kv_store:
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
            replica.logger.info(
                f"is_casually_after condition failed: vectorclock1 ip '{vectorClock1[ip]}' < vectorclock2 ip '{vectorClock2[ip]}'")

            return False

    replica.logger.info("is_casually_after conditions satisfied")
    return True


def handleUnreachableReplica(address: str) -> None:
    """
    Handles the case when a replica is unreachable.

    Keyword arguments:
    address -- the IP:PORT of the unreachable replica
    """
    global VIEW

    replica.logger.error(
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

    replica.logger.debug("current view: %s", VIEW)

    if (len(VIEW) == 1):
        # Sanity Check
        assert CURRENT_ADDRESS == VIEW[0], "Only one address in view; should be current address not %s" % VIEW[0]

        replica.logger.info(
            "View only contains current address; skipping broadcast replica state")
        return

    replica.logger.info(
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
            replica.logger.info("Broadcasting replica state to %s", address)

            response = requests.request(
                request,
                f"http://{address}/view/nobroadcast",
                headers={"Content-Type": "replicalication/json"},
                json={"socket-address": broadcastAddress}
            )

            response.raise_for_status()

            replica.logger.info(
                f"Successfully broadcasted replica state to {address}")

        except requests.exceptions.ConnectionError as e:
            # TODO: more robust detection mechanism
            handleUnreachableReplica(address)
            continue

        except requests.exceptions.RequestException as e:
            abort(e.response.status_code,
                  f"Unexpected error while broadcasting replica state to {address}: {e}")

        except Exception as e:
            replica.logger.error("Unexpected error: %s", e)
            print(
                f"Unexpected error will long-polling Replica {address}: {e}")
            abort(500, e)

    replica.logger.info("Replica state broadcasted successfully")


def broadCastPutKeyReplica(key, value):
    """
    Broadcast the PUT request to all the other replicas in the view.

    Keyword arguments:
    key -- the key to be added
    value -- the value to be added
    causalMetadata -- the causal metadata to be broadcasted
    """
    global CURRENT_ADDRESS, CONNECTION_TIMEOUT, LONG_POLLING_WAIT, VIEW

    replica.logger.info(
        f"Broadcasting PUT '{key}':'{value}' with vectorClock {dict.__repr__(vectorClock)} ")

    for address in VIEW:
        if address == CURRENT_ADDRESS:
            continue

        while True:
            try:
                replica.logger.info("Broadcasting kv pair to %s", address)

                response = requests.put(
                    f"http://{address}/kvs/{key}/nobroadcast",
                    headers={"Content-Type": "replicalication/json"},
                    json={
                        "value": value,
                        "vectorClock": vectorClock},
                    timeout=(CONNECTION_TIMEOUT, None)
                )

                response.raise_for_status()

                if response.status_code == 200 or response.status_code == 201:
                    replica.logger.info(
                        f"Replica {address} successfully updated.")
                    vectorClock[address] += 1
                    break

                replica.logger.info(
                    f"Recieved status code '{response.status_code}' from Replica {address}.  Continuing long-polling...")

            except requests.Timeout:
                replica.logger.info(
                    f"Request timeout; waiting for updates from Replica {address}. Continuing long-polling...")

                time.sleep(LONG_POLLING_WAIT)

            except requests.exceptions.ConnectionError as e:
                # TODO: more robust detection mechanism
                handleUnreachableReplica(address)
                break

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 503:
                    replica.logger.info(
                        f"Replica {address} is not ready; continuing long-polling...")
                    time.sleep(LONG_POLLING_WAIT)
                    continue

                abort(e.response.status_code,
                      f"Unexpected error while broadcasting replica state to {address}: {e}")

            except requests.exceptions.RequestException as e:
                abort(e.response.status_code,
                      f"Unexpected error while broadcasting replica state to {address}: {e}")

            except Exception as e:
                replica.logger.error("Unexpected error: %s", e)
                print(
                    f"Unexpected error will long-polling Replica {address}: {e}")
                abort(500, e)

    replica.logger.info("Replica state broadcasted successfully")


# Helper routes
# View Operations
@replica.route('/view', methods=['PUT'])
def addReplica():
    """"Adds a new replica to the view."""
    global VIEW

    data = request.get_json()
    newAddress = data['socket-address']

    if newAddress in VIEW:
        return {"result": "already present"}, 200

    broadcastReplicaState(newAddress, "PUT")

    VIEW.replicaend(newAddress)

    return {"result": "added"}, 201


@replica.route('/view/nobroadcast', methods=['PUT'])
def addToReplicaView():
    global VIEW

    data = request.get_json()
    newAddress = data['socket-address']

    if newAddress in VIEW:
        return {"result": "already present"}, 200

    VIEW.replicaend(newAddress)
    return {'result': 'added'}, 201


@replica.route('/view', methods=['GET'])
def getReplica():
    """Retrieve the view from a replica."""
    global VIEW

    return {"view": VIEW}, 200


@replica.route("/view", methods=['DELETE'])
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


@replica.route('/view/nobroadcast', methods=['DELETE'])
def deleteFromReplicaView():
    global VIEW

    replica.logger.debug("current view: %s", VIEW)

    data = request.get_json()
    deletedAddress = data['socket-address']

    if deletedAddress not in VIEW:
        abort(404, "View has no such replica")

    replica.logger.info(f"deleting {deletedAddress} from current view")

    VIEW.remove(deletedAddress)
    return {"result": "deleted"}, 200


# Key-Value Store Operations
@replica.route('/kvs/<key>', methods=['PUT'])
def addKey(key):
    global global_kv_store

    replica.logger.debug(f"begin PUT '{key}'")  # DEBUG

    # Parse request
    data = request.get_json()
    value = data.get('value')
    causalMetadata_dict = data.get('causal-metadata')

    validate_key_length(key)
    validate_value(value)

    # Retrieve current entry in KV Store
    # TODO: check if this is correct; no casual dependencies means always new?
    if causalMetadata_dict is None:
        replica.logger.debug(f"Key '{key}' is new: {isNewKey}")

        # update KV Store
        gloabl_kv_store.set(key, value)

        broadCastPutKeyReplica(key, value)

        if isNewKey:
            return {'result': 'created', 'causal-metadata': entry['vectorClock']}, 201

        return {'result': "replaced", "causal-metadata": entry['vectorClock']}, 200

    incomingKVStore = KVStore(causalMetadata_dict)
    incomingVectorClock = incomingKVStore.getVectorClock(key)

    replica.logger.debug("Incoming vectorClock: {incomingVectorClock}")

    # Check if causal dependencies are satisfied
    if incomingVectorClock is not None and not is_causually_after(incomingVectorClock, currentVectorClock):
        abort(503, "Causal dependencies not satisfied; try again later")
    # DEBUG
    replica.logger.debug(
        f"Current entry vectorClock: {dict.__repr__(currentVectorClock)}")

    # TODO: check if this is correct; no casual dependencies means always new?
    isNewKey = incomingVectorClock is None or key not in global_kv_store

    replica.logger.debug(f"Key '{key}' is new: {isNewKey}")

    # update KV Store
    entry['value'] = value
    entry['vectorClock'][CURRENT_ADDRESS] += 1

    broadCastPutKeyReplica(key, value, entry['vectorClock'])

    if isNewKey:
        return {'result': 'created', 'causal-metadata': entry['vectorClock']}, 201

    return {'result': "replaced", "causal-metadata": entry['vectorClock']}, 200


@replica.route('/kvs/<key>/nobroadcast', methods=['PUT'])
def addKeyToReplica(key):
    global global_kv_store

    data = request.get_json()
    value = data['value']
    incomingVectorClock = data['vectorClock']

    replica.logger.info(
        f"Replicate PUT '{key}':'{value}' with vectorClock {dict.__repr__(incomingVectorClock)}")

    doesCurrentKeyExist = key in global_kv_store

    entry = global_kv_store[key]
    currentVectorClock = entry['vectorClock']

    replica.logger.info(
        f"Current entry vectorClock: {dict.__repr__(currentVectorClock)}")

    if doesCurrentKeyExist and not is_causually_after(incomingVectorClock,  currentVectorClock):
        abort(503, "Causal dependencies not satisfied; try again later")

    entry['value'] = value
    currentVectorClock[CURRENT_ADDRESS] += 1

    return {'result': "successfully propigated PUT"}, 200


@replica.route('/kvs/<key>', methods=['GET'])
def getKey(key):
    global global_kv_store, vectorClock

    validate_key_exists(key)

    data = request.get_json()
    causalMetadata = data['causal-metadata']

    # if Counter(vectorClock) != Counter(
    #         causalMetadata) and causalMetadata is not None:
    #     abort(503, f"Causal dependencies not satisfied; try again later")

    # return {'result': 'found',
    #         'value': global_kv_store[key], 'causal-metadata': vectorClock}, 200


@replica.route('/kvs/<key>', methods=['DELETE'])
def deleteKey(key):
    global global_kv_store

    validate_key_exists(key)

    data = request.get_json()
    causalMetadata = data['causal-metadata']

    # del global_kv_store[key]

    # return {'result': 'deleted', 'causal-metadata': '<V>'}


@replica.route('/kvs', methods=['GET'])
def getKVStore():
    global global_kv_store

    return {'causal-metadata': global_kv_store}, 200


# Main
if __name__ == '__main__':
    replica.run(debug=True, port=int(CURRENT_ADDRESS.split(
        ':')[1]), host=CURRENT_ADDRESS.split(':')[0])
