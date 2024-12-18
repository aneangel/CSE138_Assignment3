from collections import defaultdict
import logging

log = logging.getLogger('server.kv_store')


class VectorClock(defaultdict):
    """
    A vector clock implementation.

    The vector clock is a dictionary of address names to integers. Each integer
    is the number of times that address has performed an operation on the key
    associated with the vector clock.
    """

    def __init__(self, d={}):
        super(VectorClock, self).__init__(int, d)

    def update(self, secondClock=None):
        """
        Combine two vector clocks by taking the maximum value for each node's entry.

        Keyword arguments:
        secondClock -- Second vector clock to combine.
        """
        # TODO: Logic is not quite right

        if secondClock is None:
            return

        # TODO: should throw error if not causally after ?

        # Take the maximum value for each node's entry
        _vector_clock = defaultdict(int)
        for node in set(self.keys()) | set(secondClock.keys()):
            maxClock = max(
                self[node], secondClock[node])

            # TODO: maybe there isn't a problem with mutating in place?
            _vector_clock[node] = maxClock

        self = _vector_clock

    def is_casually_after(self, other):
        return True
        # TODO: Logic is not quite right

        for address in self:
            if self[address] < other[address]:
                log.debug(
                    f"Address {address} is not causally after {other[address]}")
                return False

        return True

    def __repr__(self):
        return dict.__repr__(self)

    def __str__(self):
        return dict.__str__(self)


class KVStore():
    """
    A addressted key-value store instance. 

    The store is a dictionary of key-value pairs, where each key is a string and
    each value is an object storing the value and the vector clock for that key.

    Operations may fail if casual consistency is violated.
    """

    def __init__(self, address, kv_store_dict={}):
        self.dict = kv_store_dict
        self.currentAddress = address
        self.vectorClock = VectorClock()

    def update(self, key, value, incomingVectorClock):
        log.debug(f"Updating {key} to {value} with {incomingVectorClock}")

        if incomingVectorClock is not None and not incomingVectorClock.is_casually_after(self.vectorClock):
            return False

        self.vectorClock.update(incomingVectorClock)

        if value is None:
            del self.dict[key]
        else:
            self.dict[key] = value

        self.vectorClock[self.currentAddress] += 1

        return True

    def get(self, key, incomingVectorClock):
        log.debug(f"Getting {key} with {incomingVectorClock}")

        if incomingVectorClock is not None and not incomingVectorClock.is_casually_after(self.vectorClock):
            return False

        return self.dict[key]

    def __contains__(self, key):
        return key in self.dict

    def __str__(self):
        return f"dict: {self.dict}\ncasual metdata: {self.vectorClock}"
