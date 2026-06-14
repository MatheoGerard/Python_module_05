from abc import ABC, abstractmethod
import typing


class DataProcessor(ABC):
    @abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass


#    def output() -> tuple[int, str]:


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        self.data: list[str] = []

    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, (float, int)):
            return True
        elif isinstance(data, list) and all(
            isinstance(x, (int, float)) for x in data
        ):
            return True
        else:
            return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if isinstance(data, (int, float)):
            self.data.append(str(data))
        elif isinstance(data, list):
            for x in data:
                self.data.append(str(x))
        else:
            raise TypeError("Got exception: Improper numeric data")


class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, str):
            return True
        elif isinstance(data, list) and all(isinstance(x, str) for x in data):
            return True
        else:
            return False


class LogProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, dict) and all(
            isinstance(k, str) and isinstance(v, str) for k, v in data.items()
        ):
            return True
        elif isinstance(data, list) and all(
            isinstance(x, dict)
            and all(
                isinstance(k, str) and isinstance(v, str) for k, v in x.items()
            )
            for x in data
        ):
            return True
        else:
            return False


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===")
    print()
    num_proc: NumericProcessor = NumericProcessor()
    print("Testing Numeric Processor...")
    num_value: int | float = 42
    text_value: str = "Hello"
    print(
        f"Trying to validate input '{num_value}': {num_proc.validate(num_value)}"
    )
    print(
        f"Trying to validate input '{text_value}': {num_proc.validate(text_value)}"
    )
    foo: str = "foo"
    try:
        print(
            f"Test invalid ingestion of string '{foo}' without prior validation: "
        )
        num_proc.ingest(foo)
    except TypeError as e:
        print(e)
