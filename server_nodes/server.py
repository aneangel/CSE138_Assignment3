# main.py
import requests


class MultiMasterKeyValueStore:
    def __init__(self, node_id, nodes):
        self.node_id = node_id
        self.nodes = nodes
        self.vector_clock = {node: 0 for node in nodes}
        self.data = {}

    def put(self, key, value):
        self.vector_clock[self.node_id] += 1
        write_operation = {"type": "PUT", "node_id": self.node_id, "key": key, "value": value,
                           "vector_clock": dict(self.vector_clock)}
        self.replicate_operation(write_operation)

    def get(self, key):
        read_operation = {"type": "GET", "node_id": self.node_id, "key": key, "vector_clock": dict(self.vector_clock)}
        result = self.replicate_operation(read_operation)
        return result.get("value")

    def replicate_operation(self, operation):
        responses = []
        for node in self.nodes:
            if node != self.node_id:
                response = self.send_operation(node, operation)
                responses.append(response)
                self.vector_clock = self.merge_vector_clocks(self.vector_clock, response.get("vector_clock", {}))

        return self.apply_operation(operation, responses)

    def apply_operation(self, operation, responses):
        if operation["type"] == "PUT":
            self.data[operation["key"]] = {"value": operation["value"], "vector_clock": operation["vector_clock"]}

        # For simplicity, let's assume a last-write-wins conflict resolution strategy here
        if operation["type"] == "GET" and responses:
            return max(responses, key=lambda x: x.get("vector_clock", {}).get(self.node_id, 0), default={})
        else:
            return {}

    def send_operation(self, node, operation):
        response = requests.post(f'{node}/replicate', json={"operation": operation})
        return response.json()

    def merge_vector_clocks(self, clock1, clock2):
        merged_clock = {}
        for node in set(clock1.keys()).union(clock2.keys()):
            merged_clock[node] = max(clock1.get(node, 0), clock2.get(node, 0))
        return merged_clock

    def dependencies_satisfied(self, causal_metadata):
        for node, version in causal_metadata.items():
            if self.vector_clock[node] < version:
                return False
        return True
