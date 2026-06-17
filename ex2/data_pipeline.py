import abc
from abc import ABC, abstractmethod
import typing

class DataProcessor(ABC):
    @abstractmethod
    def validate(self):
        pass
    @abstractmethod
    def ingest(self):
        pass
    def output(self):
        pass
