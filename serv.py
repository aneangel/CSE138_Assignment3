from flask import Flask, request, jsonify, abort
import requests
import time
import os
from collections import defaultdict, Counter
import asyncio

app = Flask(__name__)

# get environment variables
viewEnv = os.environ.get('VIEW', '')
view = viewEnv.split(',')
currentAddress = os.environ.get('SOCKET_ADDRESS', '')

# Initialize KV Store
kvStore = {}

# Initialize Vector Clock
vectorClock = defaultdict(int)


# vectorClockMatch = asyncio.Event()


# Helper Functions
def broadcastAddReplica(newAddress):
    """
    Broadcast the request to add the new replica IP:PORT to all the other
    existing replicas' view.

    Keyword arguments:
    newAddress -- the IP:PORT of the new replica to be added
    """
    for address in view:
        if address == newAddress or address == currentAddress:
            return

        urlExisting = f"http://{address}/addToReplicaView"
        reqBodyExisting = {"socket-address": newAddress}

        try:
            responseExisting = requests.put(
                urlExisting, json=reqBodyExisting)
            responseExisting.raise_for_status()

        except requests.exceptions.RequestException as e:
            return e


def broadCastDeleteReplica(deletedAddress):
    """
    Broadcast the request for deleting an IP:PORT from all the replicas'
    view storage.

    Keyword arguments:
    deletedAddress -- the IP:PORT of the replica to be deleted
    """
    for address in view:
        if address == currentAddress:
            return

        url = f"http://{address}/deleteFromReplicaView"
        reqBody = {"socket-address": deletedAddress}

        try:
            response = requests.delete(url, json=reqBody)
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            return e


def validate_key_length(key):
    """
    Validates the length of the key.

    Keyword arguments:
    key -- the key to be validated
    """
    if len(key) > 50:
        abort(400, "Key is too long")


def validate_key_exists(key):
    """
    Validates that the key exists in the KV Store.

    Keyword arguments:
    key -- the key to be validated
    """
    if key not in kvStore:
        abort(404, "Key does not exist")


def broadCastPutKeyReplica(key, value, causalMetadata):
    """
    Broadcast the PUT request to all the other replicas in the view.

    Keyword arguments:
    key -- the key to be added
    value -- the value to be added
    causalMetadata -- the causal metadata to be broadcasted
    """
    for address in view:
        if address == currentAddress:
            return

        url = f"http://{address}/kvs/updateVectorClock"
        reqBody = {"key": key, "value": value,
                   "causalMetadata": causalMetadata}

        # try:
        #     response = requests.put(url, json=reqBody)
        #     response.raise_for_status()
        #
        # except requests.exceptions.RequestException as e:
        #     print(f"Error updating vector clock at Replica {address}: {e}")

        # Set a timeout value for the long-polling rquest
        timeout = 30  # We can adjust later

        try:
            # Using a while loop to repeatedly make long-polling requests until a response is received
            while True:
                response = requests.put(url, json=reqBody, timeout=timeout)

                if response.status_code == 200 or response.status_code == 201:
                    # Process the response as needed
                    print(f"Replica {address} successfully updated.")
                    vectorClock[address] += 1
                    break  # Exit the loop when a successful response is received
                # elif response.status_code == 204:
                #     # No updates, continue long-polling
                #     print(f"No updates from Replica {address}. Continuing long-polling.")
                #     time.sleep(1)
                else:
                    # Handle other status codes if necessary
                    print(
                        f"Error updating Replica {address}. Status code: {response.status_code}")
                    # break # getting rid of this break makes code run forever, but keeping it here I don't think
                    continue
                    # adds to the

        except requests.Timeout:
            # Handle timeout and continue long-polling
            print(
                f"Timeout waiting for updates from Replica {address}. Continuing long-polling.")
        except Exception as e:
            # Handle other exceptions if needed
            print(f"Error: {e}")


# DEUBG Routes
@app.route('/deleteFromReplicaView', methods=['PUT'])
def deleteFromReplicaView():
    data = request.get_json()
    deletedAddress = data['socket-address']

    if deletedAddress in view:
        view.remove(deletedAddress)
        return {"result": "deleted"}, 200

    return {"error": "View has no such replica"}, 404


@app.route('/addToReplicaView', methods=['DELETE'])
def addToReplicaView():
    data = request.get_json()
    newAddress = data['socket-address']

    if newAddress in view:
        return {"result": "already present"}, 200

    view.append(newAddress)
    return {'result': 'added'}


# Helper route
@app.route('/kvs/updateVectorClock', methods=['PUT'])
def addKeyToReplica():
    global currentAddress, kvStore, vectorClock

    data = request.get_json()
    key = data['key']
    value = data['value']
    causalMetadata = data['causalMetadata']

    if Counter(vectorClock) == Counter(causalMetadata):
        if key in kvStore:
            kvStore[key] = value
            vectorClock[currentAddress] += 1

            return {'result': "replaced", "causal-metadata": vectorClock}, 200

        kvStore[key] = value
        vectorClock[currentAddress] += 1

        return {'result': 'created', 'causal-metadata': vectorClock}, 201

    return {"error": "invalid metadata"}, 503

    #     # implement http long-polling here


# View Operations
@app.route('/view', methods=['PUT'])
def addReplica():
    """"Adds a new replica to the view"""
    data = request.get_json()
    newAddress = data['socket-address']

    if newAddress in view:
        return {"result": "already present"}, 200

    view.append(newAddress)

    broadcastAddReplica(newAddress=newAddress)

    return {"result": "added"}, 201


@app.route('/view', methods=['GET'])
def getReplica():
    """Retrieve the view from a replica"""
    return {"view": view}, 200


@app.route("/view", methods=['DELETE'])
def deleteReplica():
    """Removes an existing replica from the view"""
    data = request.get_json()
    deletedAddress = data['socket-address']

    if deletedAddress in view:
        view.remove(deletedAddress)
        broadCastDeleteReplica(deletedAddress=deletedAddress)
        return {"result": "deleted"}, 200

    return {"error": "View has no such replica"}, 404


@app.route('/kvs/<key>', methods=['PUT'])
def addKey(key):
    validate_key_length(key=key)

    data = request.get_json()
    value = data['value']
    causalMetadata = data['causal-metadata']

    if Counter(vectorClock) == Counter(causalMetadata):
        if key in kvStore:
            kvStore[key] = value
            vectorClock[currentAddress] += 1

            # Broadcast put to other replicas here
            broadCastPutKeyReplica(key, value, causalMetadata)

            return {'result': "replaced", "causal-metadata": vectorClock}, 200

        kvStore[key] = value
        vectorClock[currentAddress] += 1

        # Broadcast put to other replicas here
        broadCastPutKeyReplica(key, value, causalMetadata)

        return {'result': 'created', 'causal-metadata': vectorClock}, 201

        # timeout = 30  # timeout value
        # start_time = time.time()
        #
        # while time.time() - start_time < timeout:
        #     time.sleep(1)  # delay before the next long-polling attempt
        #
        #     if Counter(vectorClock) == Counter(causalMetadata):
        #         # If vector clocks match, then proceed with the update
        #         if key in kvStore:
        #             kvStore[key] = value
        #             vectorClock[currentAddress] += 1
        #
        #             return {'result': "replaced", "causal-metadata": vectorClock}, 200
        #         else:
        #             kvStore[key] = value
        #             vectorClock[currentAddress] += 1
        #
        #             return {'result': 'created', 'causal-metadata': vectorClock}, 201

        # If the timeout is reached and vector clocks still don't match, return an error
    return {"error": "Causal dependencies not satisfied; try again later"}, 503


@app.route('/kvs/<key>', methods=['GET'])
def getKey(key):
    global kvStore, vectorClock

    # validate_key_exists(key=key)

    data = request.get_json()
    causalMetadata = data['causal-metadata']

    if Counter(vectorClock) == Counter(causalMetadata) or causalMetadata is None:
        return {'result': 'found', 'value': kvStore[key], 'causal-metadata': vectorClock}, 200

    return {"error": "Causal dependencies not satisfied; try again later", "vector clock": vectorClock}, 503


@app.route('/kvs/<key>', methods=['DELETE'])
def deleteKey(key):
    validate_key_exists(key=key)

    data = request.get_json()
    causalMetadata = data['causal-metadata']

    del kvStore[key]

    return {'result': 'deleted', 'causal-metadata': '<V>'}


# Main
if __name__ == '__main__':
    print(int(currentAddress.split(':')[1]))
    print(currentAddress.split(':')[0])
    app.run(debug=True, port=int(currentAddress.split(
        ':')[1]), host=currentAddress.split(':')[0])
