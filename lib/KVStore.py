from collections import defaultdict


class VectorClock(defaultdict):
    """
    A vector clock implementation.

    The vector clock is a dictionary of address names to integers. Each integer
    is the number of times that address has performed an operation on the key
    associated with the vector clock.
    """

    def __init__(self, d={}):
        super(VectorClock, self).__init__(int, d)

    def incrementClock(self, currentReplica):
        self[currentReplica] += 1

    def combineClocks(self, secondClock=None):
        """
        Combine two vector clocks by taking the maximum value for each node's entry.

        Args:
            clock1 (defaultdict): First vector clock.
            clock2 (defaultdict): Second vector clock.

        Returns:
            defaultdict: Combined vector clock.
            :param secondClock:
            :param currentReplica:
        """

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
        for address in self:
            if self[address] < other[address]:
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
        self.__dict = kv_store_dict
        self.currentAddress = address
        self.vectorClock = VectorClock()

    def update(self, key, value, toCombine, currentReplica, incomingVectorClock=None):
        if incomingVectorClock is not None and not incomingVectorClock.is_casually_after(self.vectorClock):
            return False

        self.__dict[key] = value

        # checks whether it should only combine
        if toCombine:
            self.vectorClock.combineClocks(incomingVectorClock)
        # checks whether to just increment
        else:
            self.vectorClock.incrementClock(currentReplica=currentReplica)

        return True

    def get(self, key):
        return self.__dict[key]

    @property
    def dict(self):
        return self.__dict

    def __contains__(self, key):
        return key in self.__dict

    def __str__(self):
        return f"dict: {self.__dict}\ncasual metdata: {self.vectorClock}"
