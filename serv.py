from flask import Flask, request, jsonify, abort
import requests


app = Flask(__name__)

view = []
kvStore = {}
vectorClock = [0] * (len(view) + 1)

def broadcastAddReplica(currentAddress, newAddress):
    
    # url for the new replica
    urlNew = f"http://{newAddress}/view"

    # the request body for adding the IP:PORT of the current Flask App to the new replica
    reqBodyCurrent = {"socket-address":currentAddress}

    # request body for adding the new replica's address to the existing replicas
    reqBodyExisting = {"socket-address":newAddress}

    # adding the IP:PORT of the current Flask APP to the new replica
    try:
        responseNew = requests.put(urlNew, json=reqBodyCurrent)
        responseNew.raise_for_status()

    except requests.exceptions.RequestException as e:

        print(f"Error broadcasting to {address}: {e}")

    # broadcasting the request to add the new replica IP:PORT to all the other existing replicas' view 
    # and adding the existing replicas' IP:PORT to the new replica's view
    for address in view:

        if address != newAddress:

            urlExisting = f"http://{address}/view"
            reqBodyNew = {"socket-address":address}

            try:
                responseExisting = requests.put(urlExisting, json=reqBodyExisting)
                responseNew = requests.put(urlNew, json=reqBodyNew)
                responseExisting.raise_for_status()
                responseNew.raise_for_status()

            except requests.exceptions.RequestException as e:
                print(f"Error broadcasting to {address}: {e}")


def broadCastDeleteReplica(deletedAddress):

    # broadcasting the request for deleting an IP:PORT from all the replicas' view storage
    for address in view:
        url = f"http://{address}/view"
        reqBody = {"socket-address":deletedAddress}

        try:
            response = requests.delete(url, json=reqBody)
            response.raise_for_status

        except requests.exceptions.RequestException as e:
                print(f"Error broadcasting to {address}: {e}")

        



@app.route('/view', methods=['PUT'])
def addReplica():
    data = request.get_json()
    newAddress = data['socket-address']

    if newAddress in view:
        return {"result", "already present"}, 200
    else:
        view.append(newAddress)
        
        # Get current flask APP's IP:PORT
        ip = request.environ.get("HTTP_X_REAL_IP", request.remote_addr)
        port = request.environ["SERVER_PORT"]
        currentAddress = {ip:port}

        broadcastAddReplica(currentAddress=currentAddress, newAddress=newAddress)
        return {"result", "added"}, 201



@app.route('/view', methods = ['GET'])
def getReplica():
    data = request.get_json()
    return {"view", view}, 200


@app.route("/view", methods = ['DELETE'])
def deleteReplica():
    data = request.get_json()
    deletedAddress = data['socket-address']

    if deletedAddress in view:
        view.remove(deletedAddress)
        broadCastDeleteReplica(deletedAddress=deletedAddress)
        return {"result", "deleted"}, 200
    else:
        return {"error", "View has no such replica"}, 404


def validate_key_length(key):
    if len(key) > 50:
        abort(400, "Key is too long")

def validate_key_exists(key):
    if key not in kvStore:
        abort(404, "Key does not exist")


@app.route('/kvs/<key>', methods=['PUT'])
def addKey(key):
    validate_key_length(key=key)

    data = request.get_json()
    value = data['value']
    causalMetadata = data['causal-metadata']

    if key in kvStore:
        kvStore[key] = value
        return {'result': "replaced", "causal-metadata": "<V>"}, 200
    else:
        kvStore[key] = value
        return {'result': 'created', 'causal-metadata': '<V>'}, 201





@app.route('kvs/<key>', methods=['GET'])
def getKey(key):
    validate_key_exists(key=key)

    data = request.get_json()
    causalMetadata = data['causal-metadata']

    return {'result': 'found', 'value': kvStore[key], 'causal-metadata': '<V>'}, 200





@app.route('/kvs/<key>', methods=['DELETE'])
def deleteKey(key):
    validate_key_exists(key=key)

    data = request.get_json()
    causalMetadata = data['causal-metadata']

    del kvStore[key]

    return {'result': 'deleted', 'causal-metadata': '<V>'}



if __name__ == '__main__':
    app.run(debug=True)
