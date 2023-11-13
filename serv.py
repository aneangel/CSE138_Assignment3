from flask import Flask, request, jsonify
import requests


app = Flask(__name__)

view = set()

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
        view.add(newAddress)
        
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

    



if __name__ == '__main__':
    app.run(debug=True)
