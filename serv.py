from flask import Flask, request, jsonify


app = Flask(__name__)

replicas = []

@app.route('/view', methods=['PUT'])
def addReplica():
    data = request.get_json()
    # print(data['socket-address'])
    replicas.append(data['socket-address'])



@app.route('/view', methods = ['GET'])
def getReplica():
    data = request.get_json()
    response = {
        "view" : replicas
    }
    return jsonify(response)


@app.route("/view", methods = ['DELETE'])
def deleteReplica():
    data = request.get_json()
    replicas.remove(data['socket-address'])

    
    


if __name__ == '__main__':
    app.run(debug=True)
