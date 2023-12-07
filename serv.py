from flask import Flask, request, jsonify, abort
import requests
import os
from collections import defaultdict, Counter
import time

app = Flask(__name__) \
 \
    # get environment variables
viewEnv = os.environ.get('VIEW', '')
view = viewEnv.split(',')
currentAddress = os.environ.get('SOCKET_ADDRESS', '')

# Initialize KV Store
kvStore = {}

# Initialize Vector Clock
vectorClock = defaultdict(int)
for v in view:
    vectorClock[v]


def broadcastAddReplica(newAddress):
    global currentAddress, view

    # broadcasting the request to add the new replica IP:PORT to all the other existing replicas' view 
    for address in view:

        if address != newAddress and address != currentAddress:

            urlExisting = f"http://{address}/addToReplicaView"
            reqBodyExisting = {"socket-address": newAddress}

            try:
                responseExisting = requests.put(urlExisting, json=reqBodyExisting)
                responseExisting.raise_for_status()

            except requests.exceptions.RequestException as e:
                return e


def broadCastDeleteReplica(deletedAddress):
    global currentAddress, view

    # broadcasting the request for deleting an IP:PORT from all the replicas' view storage
    for address in view:
        if address != currentAddress:
            url = f"http://{address}/deleteFromReplicaView"
            reqBody = {"socket-address": deletedAddress}

            try:
                response = requests.delete(url, json=reqBody)
                response.raise_for_status

            except requests.exceptions.RequestException as e:
                return e


@app.route('/deleteFromReplicaView', methods=['PUT'])
def addToReplicaView():
    global view

    data = request.get_json()
    deletedAddress = data['socket-address']

    if deletedAddress in view:
        view.remove(deletedAddress)
        return {"result": "deleted"}, 200
    else:
        return {"error": "View has no such replica"}, 404


@app.route('/addToReplicaView', methods=['DELETE'])
def deleteFromReplicaView():
    global view

    data = request.get_json()
    newAddress = data['socket-address']

    if newAddress in view:
        return {"result": "already present"}, 200
    else:
        view.append(newAddress)
        return {'result': 'added'}


@app.route('/view', methods=['PUT'])
def addReplica():
    data = request.get_json()
    newAddress = data['socket-address']

    if newAddress in view:
        return {"result": "already present"}, 200
    else:
        view.append(newAddress)

        broadcastAddReplica(newAddress=newAddress)

        return {"result": "added"}, 201


@app.route('/view', methods=['GET'])
def getReplica():
    global view

    return {"view": view}, 200


@app.route("/view", methods=['DELETE'])
def deleteReplica():
    global view

    data = request.get_json()
    deletedAddress = data['socket-address']

    if deletedAddress in view:
        view.remove(deletedAddress)
        broadCastDeleteReplica(deletedAddress=deletedAddress)
        return {"result": "deleted"}, 200
    else:
        return {"error": "View has no such replica"}, 404


def validate_key_length(key):
    if len(key) > 50:
        abort(400, "Key is too long")


def validate_key_exists(key):
    global kvStore

    if key not in kvStore:
        abort(404, "Key does not exist")


def broadCastPutKeyReplica(key, causalMetadata, value):
    global currentAddress, view

    for address in view:

        if address != currentAddress:
            url = f"http://{address}/kvs/addKeyToReplica/{key}"
            reqBody = {"value": value, "causal-metadata": causalMetadata}

            # this is where we would need to implement the request that uses HTTP long-polling here
            requests.put(url, json=reqBody)


def vectorsMatch(vectorClock1, vectorClock2, address):
    if address not in vectorClock1 and address not in vectorClock2:
        return True
    elif (address not in vectorClock1 and address in vectorClock2) or (address in vectorClock1 and address not in vectorClock2):
        return False
    else:
        return vectorClock1[address] == vectorClock2[address]


@app.route('/kvs/addKeyToReplica/<key>')
def addKeyToReplica(key):
    global currentAddress, kvStore, vectorClock

    data = request.get_json()
    value = data['value']
    causalMetadata = data['causal-metadata']

    while True:
        if vectorsMatch(vectorClock1=vectorClock, vectorClock2=causalMetadata, address=currentAddress) or causalMetadata is None:
            if key in kvStore:
                kvStore[key] = value
                vectorClock[currentAddress] += 1

                return {'result': "replaced", "causal-metadata": vectorClock}, 200
            else:
                kvStore[key] = value
                vectorClock[currentAddress] += 1

                return {'result': 'created', 'causal-metadata': vectorClock}, 201
        else:
            time.sleep(2)


@app.route('/kvs/<key>', methods=['PUT'])
def addKey(key):
    global currentAddress, kvStore, vectorClock

    validate_key_length(key=key)

    data = request.get_json()
    value = data['value']
    causalMetadata = data['causal-metadata']

    if vectorsMatch(vectorClock1=vectorClock, vectorClock2=causalMetadata, address=currentAddress) or causalMetadata is None:
        if key in kvStore:
            kvStore[key] = value
            vectorClock[currentAddress] += 1

            # Broadcast put to other replicas here
            broadCastPutKeyReplica(key, causalMetadata=causalMetadata, value=value)

            return {'result': "replaced", "causal-metadata": vectorClock}, 200
        else:
            kvStore[key] = value
            vectorClock[currentAddress] += 1

            # Broadcast put to other replicas here
            broadCastPutKeyReplica(key, causalMetadata=causalMetadata, value=value)

            return {'result': 'created', 'causal-metadata': vectorClock}, 201

    else:
        return {"error": "Causal dependencies not satisfied; try again later"}, 503


@app.route('kvs/<key>', methods=['GET'])
def getKey(key):
    global kvStore, vectorClock

    validate_key_exists(key=key)

    data = request.get_json()
    causalMetadata = data['causal-metadata']

    if vectorsMatch(vectorClock1=vectorClock, vectorClock2=causalMetadata, address=currentAddress) or causalMetadata is None:
        return {'result': 'found', 'value': kvStore[key], 'causal-metadata': vectorClock}, 200

    else:
        return {"error": "Causal dependencies not satisfied; try again later"}, 503


def broadcastDelKeyReplica(key, causalMetadata):
    global currentAddress, view

    for address in view:

        if address != currentAddress:
            url = f"http://{address}/kvs/delKeyFromReplica/{key}"
            reqBody = {"causal-metadata": causalMetadata}

            # this is where we would need to implement the request that uses HTTP long-polling here
            requests.delete(url, json=reqBody)


@app.route('/kvs/delKeyFromReplica/<key>')
def delKeyFromReplica(key):
    global currentAddress, kvStore, vectorClock

    data = request.get_json()
    causalMetadata = data['causal-metadata']

    # responsible for the HTTP long-polling
    while True:
        if vectorsMatch(vectorClock1=vectorClock, vectorClock2=causalMetadata, address=currentAddress) or causalMetadata is None:
            validate_key_exists(key=key)

            del kvStore[key]
            vectorClock[currentAddress] += 1

            return {'result': 'deleted', 'causal-metadata': vectorClock}
        else:
            time.sleep(2)


@app.route('/kvs/<key>', methods=['DELETE'])
def deleteKey(key):
    global kvStore, currentAddress, vectorClock

    validate_key_exists(key=key)

    data = request.get_json()
    causalMetadata = data['causal-metadata']

    if vectorsMatch(vectorClock1=vectorClock, vectorClock2=causalMetadata, address=currentAddress) or causalMetadata is None:

        del kvStore[key]
        vectorClock[currentAddress] += 1

        # broadcast del to other replicas here
        broadcastDelKeyReplica(key=key, causalMetadata=causalMetadata)

        return {'result': 'deleted', 'causal-metadata': vectorClock}

    else:
        return {"error": "Causal dependencies not satisfied; try again later"}, 503


if __name__ == '__main__':
    app.run(debug=True)
