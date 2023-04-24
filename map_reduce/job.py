# abstacct class for map reduce job, defines the following : map,reduce and hash and sort functions

from abc import ABC, abstractmethod

class MapReduce(ABC):
    def __init__(self, num_mappers: int, num_reducers: int) -> None:
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        

    @abstractmethod
    def map(self, key: str, value: str) :
        pass

    @abstractmethod
    def reduce(self, key ,values ) :
        pass

    @abstractmethod
    def hash(self, key) -> int:
        pass

    @abstractmethod
    def compare(self, key1: str, key2: str) -> bool:
        pass

    @abstractmethod
    def pre_process_map(self, key: str, value: str):
        pass

    @abstractmethod
    def pre_process_reduce(self, key, values):
        pass
