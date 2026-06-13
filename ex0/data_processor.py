from abc import ABC, abstractmethod
import typing


class DataProcessor(ABC):
    @abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass
