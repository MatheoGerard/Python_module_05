from abc import ABC, abstractmethod
import typing


class DataProcessor(ABC):
    @abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output() -> tuple[int, str]:


class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if type(data) == int or type(data) == float or type(data) == lis

class TextProcessor(DataProcessor):


class LogProcessor(DataProcessor):


if __name__ == "__main__":
    print("je suis le main")
    print("je viens de te rajouter pour tester lazygit")
