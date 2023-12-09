from flask import Flask, request, jsonify, abort
from threading import Thread
from collections import defaultdict
import logging
import requests
import time
import os
import sys
from lib.KVStore import KVStore, VectorClock

replica = Flask(__name__)

# Get environment variables
VIEW_ENV = os.environ.get('VIEW', '')
VIEW = VIEW_ENV.split(',')
CURRENT_ADDRESS = os.environ.get('SOCKET_ADDRESS', '')

# Constants
CONNECTION_TIMEOUT = 5
LONG_POLLING_WAIT = 1

# Initialize KV Store
global_kv_store = KVStore(CURRENT_ADDRESS)

# Initialize logger
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
replica.logger.handlers.clear()
replica.logger.addHandler(handler)
replica.logger.setLevel(logging.DEBUG)


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

    if key not in global_kv_store or global_kv_store.dict[key] is None:
        abort(404, "Key does not exist")


def poll(address) -> None:
    """
    Polls offline replicas to check if they are back up.
    """
    global VIEW

    replica.logger.info(
        f"Cannot reach {address}, starting polling for replica {address}")

    while True:
        try:
            replica.logger.debug(
                f"Attempting to reach replica {address}")

            # Check if replica is back up
            response = requests.put(
                f"http://{address}/kvs",
                headers={"Content-Type": "application/json"},
                json={"kv-store": global_kv_store.dict,
                      "causal-metadata": global_kv_store.vectorClock},
                timeout=(CONNECTION_TIMEOUT, None))
            response.raise_for_status()

            replica.logger.info(
                f"Successfully reached replica {address} again!")

            # Add replica back to view
            VIEW.append(address)
            addresses = VIEW.copy()
            addresses.remove(CURRENT_ADDRESS)
            addresses.remove(address)

            replica.logger.debug(f'Broadcasting view {address} to {addresses}')

            def putRequest(add): return requests.put(
                f"http://{add}/view",
                params={"nobroadcast": True},
                headers={"Content-Type": "application/json"},
                json={"socket-address": address}
            )

            brodcast(addresses, putRequest)
            return

        except requests.exceptions.ConnectionError as e:
            replica.logger.debug(
                f"Could not reach replica {address} again")
            time.sleep(LONG_POLLING_WAIT)

        except requests.exceptions.RequestException as e:
            print(
                f"Unexpected error while trying to reach '{address}' with error code {e.status_code}: {e}")

        except Exception as e:
            replica.logger.error("Unexpected error: %s", e)
            abort(
                500, f"Unexpected error will long-polling Replica {address}: {e}")
        finally:
            time.sleep(LONG_POLLING_WAIT)


# Helper Functions
def handleUnreachableReplica(deleteAddress: str, request) -> None:
    """
    Handles the case when a replica is unreachable.

    Keyword arguments:
    address -- the IP:PORT of the unreachable replica
    """
    global VIEW

    replica.logger.error(
        f"Could not reach replica {deleteAddress}, removing from view")

    VIEW.remove(deleteAddress)

    addresses = VIEW.copy()
    addresses.remove(CURRENT_ADDRESS)

    def delRequest(address): return requests.delete(
        f"http://{address}/view",
        params={"nobroadcast": True},
        headers={"Content-Type": "application/json"},
        json={"socket-address": deleteAddress}
    )

    brodcast(addresses, delRequest)

    # Start polling for replica to come back up
    t = Thread(target=poll, args=(deleteAddress,))
    t.daemon = True
    t.start()


def brodcast(addresses, request):
    replica.logger.debug(f"Brodcast request to '{addresses}'")

    if addresses is None or len(addresses) == 0:
        replica.logger.info("No addresses to broadcast to, skipping")
        return

    for address in addresses:
        if address not in VIEW:
            replica.logger.info(
                f"Address '{address}' no longer in view, must be unavailable. Skipping")
            continue

        try:
            replica.logger.debug(f"Broadcasting to {address}")

            response = request(address)
            response.raise_for_status()

            replica.logger.info(
                f"Successfully broadcasted update to {address}")

        except requests.exceptions.ConnectionError as e:
            # TODO: more robust detection mechanism
            handleUnreachableReplica(address, request)
            continue

        except requests.exceptions.RequestException as e:
            abort(e.response.status_code,
                  f"Unexpected error while broadcasting replica state to {address}: {e}")

        except Exception as e:
            replica.logger.error("Unexpected error: %s", e)
            print(
                f"Unexpected error will long-polling Replica {address}: {e}")
            abort(500, e)

    replica.logger.info("Boadcasted completed")


# View Operations
@replica.route('/view', methods=['PUT'])
def addReplica():
    """"Adds a new replica to the view."""
    global VIEW

    nobroadcast = request.args.get('nobroadcast', False)
    data = request.get_json()
    newAddress = data['socket-address']

    replica.logger.debug(
        f"PUT request received, view is currently '{VIEW}' adding '{newAddress}'")

    if newAddress in VIEW:
        return {"result": "already present"}, 200

    if not nobroadcast:
        addresses = VIEW.copy()
        addresses.remove(CURRENT_ADDRESS)

        def putRequest(address): return requests.put(
            f"http://{address}/view",
            params={"nobroadcast": True},
            headers={"Content-Type": "application/json"},
            json={"socket-address": newAddress}
        )

        brodcast(addresses, putRequest)

    VIEW.append(newAddress)

    return {"result": "added"}, 201


@replica.route('/view', methods=['GET'])
def getReplica():
    """Retrieve the view from a replica."""
    global VIEW

    return {"view": VIEW}, 200


@replica.route("/view", methods=['DELETE'])
def deleteReplica():
    """Removes an existing replica from the view."""
    global VIEW

    nobroadcast = request.args.get('nobroadcast', False)
    data = request.get_json()
    deletedAddress = data['socket-address']

    if deletedAddress not in VIEW:
        abort(404, "View has no such replica")

    if not nobroadcast:
        addresses = VIEW.copy()
        addresses.remove(CURRENT_ADDRESS)
        addresses.remove(deletedAddress)

        def deleteRequest(address): return requests.delete(
            f"http://{address}/view",
            params={"nobroadcast": True},
            headers={"Content-Type": "application/json"},
            json={"socket-address": deletedAddress}
        )

        brodcast(addresses, deleteRequest)

    VIEW.remove(deletedAddress)

    return {"result": "deleted"}, 200


# Key-Value Store Operations
@replica.route('/kvs/<key>', methods=['PUT'])
def addKey(key):
    global global_kv_store

    replica.logger.debug("PUT request received")

    # Parse request
    nobroadcast = request.args.get('nobroadcast', False)
    data = request.get_json()
    value = data.get('value')
    dict_incomingVectorClock = data.get('causal-metadata', {})
    incomingVectorClock = VectorClock(dict_incomingVectorClock or {})

    replica.logger.debug("PUT parameters parsed")

    validate_key_length(key)
    validate_value(value)

    isNewKey = key not in global_kv_store

    replica.logger.debug("PUT about to update kv_store")

    # Update KV Store
    updateSuccessfull = global_kv_store.update(key, value, incomingVectorClock)
    if not updateSuccessfull:
        abort(503, "Causal dependencies not satisfied; try again later")

    if not nobroadcast:
        addresses = VIEW.copy()
        addresses.remove(CURRENT_ADDRESS)

        def putRequest(address): return requests.put(
            f"http://{address}/kvs/{key}",
            headers={"Content-Type": "application/json"},
            params={"nobroadcast": True},
            json={
                "value": value,
                "causal-metadata": global_kv_store.vectorClock},
            timeout=(CONNECTION_TIMEOUT, None)
        )

        brodcast(addresses, putRequest)

    if isNewKey:
        return {'result': 'created', 'causal-metadata': global_kv_store.vectorClock}, 201

    return {'result': "replaced", "causal-metadata": global_kv_store.vectorClock}, 200


@replica.route('/kvs/<key>', methods=['GET'])
def getKey(key):
    global global_kv_store, vectorClock

    data = request.get_json()
    causalMetadata = data.get('causal-metadata', {})
    incomingVectorClock = VectorClock(causalMetadata or {})

    validate_key_exists(key)

    # TODO: move inside KVStore
    # Check if causal dependencies are satisfied
    if not incomingVectorClock.is_casually_after(global_kv_store.vectorClock):
        abort(503, "Causal dependencies not satisfied; try again later")

    value = global_kv_store.get(key, incomingVectorClock)

    return {'result': 'found', "value": value, "causal-metadata": global_kv_store.vectorClock}, 200


@replica.route('/kvs/<key>', methods=['DELETE'])
def deleteKey(key):
    global global_kv_store

    # Parse request
    nobroadcast = request.args.get('nobroadcast', False)
    data = request.get_json()
    dict_incomingVectorClock = data.get('causal-metadata', {})
    incomingVectorClock = VectorClock(dict_incomingVectorClock)

    validate_key_length(key)

    updateSuccessfull = global_kv_store.update(key, None, incomingVectorClock)
    if not updateSuccessfull:
        abort(503, "Causal dependencies not satisfied; try again later")

    if not nobroadcast:
        addresses = VIEW.copy()
        addresses.remove(CURRENT_ADDRESS)

        def putRequest(address): return requests.put(
            f"http://{address}/kvs/{key}",
            headers={"Content-Type": "application/json"},
            params={"nobroadcast": True},
            json={
                "value": None,
                "causal-metadata": global_kv_store.vectorClock},
            timeout=(CONNECTION_TIMEOUT, None)
        )

        brodcast(addresses, putRequest)

    return {'result': "deleted", "causal-metadata": global_kv_store.vectorClock}, 200


@replica.route('/kvs', methods=['GET'])
def getKVStore():
    global global_kv_store

    return str(global_kv_store), 200


@replica.route('/kvs', methods=['PUT'])
def putKVStore():
    global global_kv_store
    data = request.get_json()
    dict_incoming_kv_store = data.get('kv-store', {})
    dict_incomingVectorClock = data.get('causal-metadata', {})
    incomingVectorClock = VectorClock(dict_incomingVectorClock)

    global_kv_store.dict = dict_incoming_kv_store

    # TODO: Not correct
    global_kv_store.vectorClock = incomingVectorClock

    return "Replica up to date", 200


# Main
if __name__ == '__main__':
    port = int(CURRENT_ADDRESS.split(':')[1])
    host = CURRENT_ADDRESS.split(':')[0]

    replica.run(debug=True, port=port, host=host)
