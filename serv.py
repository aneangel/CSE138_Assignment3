from flask import Flask, request, jsonify, abort
import requests
import os
from collections import defaultdict, Counter
import asyncio

app = Flask(__name__)\

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

# vectorClockMatch = asyncio.Event()


def broadcastAddReplica(newAddress):

    # broadcasting the request to add the new replica IP:PORT to all the other existing replicas' view 
    for address in view:

        if address != newAddress and address != currentAddress:

            urlExisting = f"http://{address}/addToReplicaView"
            reqBodyExisting = {"socket-address":newAddress}

            try:
                responseExisting = requests.put(urlExisting, json=reqBodyExisting)
                responseExisting.raise_for_status()

            except requests.exceptions.RequestException as e:
                return e


def broadCastDeleteReplica(deletedAddress):

    # broadcasting the request for deleting an IP:PORT from all the replicas' view storage
    for address in view:
        if address != currentAddress:
            url = f"http://{address}/deleteFromReplicaView"
            reqBody = {"socket-address":deletedAddress}

            try:
                response = requests.delete(url, json=reqBody)
                response.raise_for_status

            except requests.exceptions.RequestException as e:
                return e
        


@app.route('/deleteFromReplicaView', methods=['PUT'])
def addToReplicaView():
    data = request.get_json()
    deletedAddress = data['socket-address']

    if deletedAddress in view:
        view.remove(deletedAddress)
        return {"result":"deleted"}, 200
    else:
        return {"error":"View has no such replica"}, 404
    

@app.route('/addToReplicaView', methods=['DELETE'])
def deleteFromReplicaView():
    data = request.get_json()
    newAddress = data['socket-address']

    if newAddress in view:
        return {"result":"already present"}, 200
    else:
        view.append(newAddress)
        return {'result':'added'}



@app.route('/view', methods=['PUT'])
def addReplica():
    data = request.get_json()
    newAddress = data['socket-address']

    if newAddress in view:
        return {"result":"already present"}, 200
    else:
        view.append(newAddress)

        broadcastAddReplica(newAddress=newAddress)

        return {"result":"added"}, 201



@app.route('/view', methods = ['GET'])
def getReplica():
    return {"view": view}, 200


@app.route("/view", methods = ['DELETE'])
def deleteReplica():
    data = request.get_json()
    deletedAddress = data['socket-address']

    if deletedAddress in view:
        view.remove(deletedAddress)
        broadCastDeleteReplica(deletedAddress=deletedAddress)
        return {"result":"deleted"}, 200
    else:
        return {"error":"View has no such replica"}, 404


def validate_key_length(key):
    if len(key) > 50:
        abort(400, "Key is too long")

def validate_key_exists(key):
    if key not in kvStore:
        abort(404, "Key does not exist")



def broadCastPutKeyReplica(key, causalMetadata, value):
    for address in view:

        if address != currentAddress:

            url = f"http://{address}/kvs/addKeyToReplica/{key}"
            reqBody = {"value":value, "causal-metadata":causalMetadata}

            response = requests.put(url, json=reqBody)

            if response == 503:
                 # this is where we would need to implement the HTTP long-polling

            


@app.route('/kvs/addKeyToReplica/<key>')
def addKeyToReplica(key):
    data = request.get_json()
    value = data['value']
    causalMetadata = data['causal-metadata']

    if Counter(vectorClock) == Counter(causalMetadata):
        if key in kvStore:
            kvStore[key] = value
            vectorClock[currentAddress] += 1

            return {'result': "replaced", "causal-metadata": vectorClock}, 200
        else:
            kvStore[key] = value
            vectorClock[currentAddress] += 1

            return {'result': 'created', 'causal-metadata': vectorClock}, 201
    
    else:
        return {"error": "Causal dependencies not satisfied; try again later"}, 503




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
            broadCastPutKeyReplica(key, causalMetadata=causalMetadata, value=value)

            return {'result': "replaced", "causal-metadata": vectorClock}, 200
        else:
            kvStore[key] = value
            vectorClock[currentAddress] += 1

            # Broadcast put to other replicas here
            broadCastPutKeyReplica(key, causalMetadata=causalMetadata, value=value)

            return {'result': 'created', 'causal-metadata': vectorClock}, 201
    
    else:
        # will need to do some sort of long-polling here too and then once it's sorted, broadcast the request to other replicas

        return {"error": "Causal dependencies not satisfied; try again later"}, 503





@app.route('kvs/<key>', methods=['GET'])
def getKey(key):
    validate_key_exists(key=key)

    data = request.get_json()
    causalMetadata = data['causal-metadata']

    if Counter(vectorClock) == Counter(causalMetadata):
        return {'result': 'found', 'value': kvStore[key], 'causal-metadata': vectorClock}, 200
    
    # else:






@app.route('/kvs/<key>', methods=['DELETE'])
def deleteKey(key):
    validate_key_exists(key=key)

    data = request.get_json()
    causalMetadata = data['causal-metadata']

    del kvStore[key]

    return {'result': 'deleted', 'causal-metadata': '<V>'}



if __name__ == '__main__':
    app.run(debug=True)
