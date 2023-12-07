from collections import defaultdict


class VectorClock():
    """
    A vector clock implementation.

    The vector clock is a dictionary of replica names to integers. Each integer
    is the number of times that replica has performed an operation on the key
    associated with the vector clock.
    """

    def __init__(self, d=None):
        self.__vector_clock = defaultdict(int, d)

    def __getitem__(self, replica):
        return self.__vector_clock[replica]

    def increment(self, replica):
        self.__vector_clock[replica] += 1

    def is_casually_after(self, other):
        for replica in self.__vector_clock:
            if self.__vector_clock[replica] < other.__vector_clock[replica]:
                return False

        return True

    def __repr__(self):
        return dict.__repr__(self.__vector_clock)

# class Entry():

#     def __init__(self, value=None, vector_clock=None):
#         self.value = value
#         self.vector_clock = VectorClock(vector_clock)


class KVStore():
    """
    A replicated key-value store instance. 

    The store is a dictionary of key-value pairs, where each key is a string and
    each value is an object storing the value and the vector clock for that key.

    Operations may fail if casual consistency is violated.
    """

    def __init__(self, replica, kv_store_dict=None):
        self.__store = defaultdict(
            lambda: {
                'value': None,
                'vectorClock': VectorClock
            }, kv_store_dict
        )

        self.__replica = replica

    def update(self, key, value):
        self.__store[key]['value'] = value
        self.__store[key]['vectorClock'][key][self.__replica].increment()

    def reflectSend(self, key, replica):
        self.__store[key]['vectorClock'][replica][self.__replica].increment()

    def getValue(self, key):
        return self.__store[key]['value']

    def getVectorClock(self, key):
        return self.__store[key]['vectorClock']

    @property
    def causalMetadata(self):
        return {key: self.__store[key]['vectorClock'] for key in self.__store}

    def __repr__(self):
        return dict.__repr__(self.__store)
