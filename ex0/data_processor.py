from abc import ABC, abstractmethod
import typing


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.rank: int = 0
        self.data: list[str] = []

    @abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        tuple_to_return: tuple[int, str] = (self.rank, self.data[0])
        self.data.remove(tuple_to_return[1])
        self.rank += 1
        return tuple_to_return


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

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
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, str):
            return True
        elif isinstance(data, list) and all(isinstance(x, str) for x in data):
            return True
        else:
            return False

    def ingest(self, data: str | list[str]) -> None:
        if isinstance(data, (str)):
            self.data.append(data)
        elif isinstance(data, list):
            for x in data:
                self.data.append(x)
        else:
            raise TypeError("Got exception: Improper numeric data")


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

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

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if isinstance(data, dict):
            self.data.append(f"{data['log_level']}: {data['log_message']}")
        elif isinstance(data, list):
            for x in data:
                self.data.append(f"{x['log_level']}: {x['log_message']}")
        else:
            raise TypeError("Got exception: Improper numeric data")


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===")
    print()
    num_proc: NumericProcessor = NumericProcessor()
    print("Testing Numeric Processor...")
    num_value: int | float = 42
    text_value: str = "Hello"
    print(
        f"Trying to validate input '{num_value}': "
        f"{num_proc.validate(num_value)}"
    )
    print(
        f"Trying to validate input '{text_value}': "
        f"{num_proc.validate(text_value)}"
    )
    foo: str = "foo"
    try:
        print(
            f"Test invalid ingestion of string '{foo}' "
            "without prior validation: "
        )
        num_proc.ingest(foo)
    except TypeError as e:
        print(e)
    data_num_list: list[int | float] = [1, 2, 3, 4, 5]
    print(f"Processing data: {data_num_list}")
    nb_to_extract: int = 3
    print(f"Extracting {nb_to_extract} values...")
    num_proc.ingest(data_num_list)
    for _ in range(0, nb_to_extract):
        curr_proc_num_value: tuple[int, str] = num_proc.output()
        print(
            f"Numeric value {curr_proc_num_value[0]}: {curr_proc_num_value[1]}"
        )
    print()
    print("Testing Text Processor...")
    text_proc: TextProcessor = TextProcessor()
    print(
        f"Trying to validate input '{num_value}': "
        f"{text_proc.validate(num_value)}"
    )
    data_text_list: list[str] = ["Hello", "Nexus", "World"]
    nb_to_extract = 1
    print(f"Extracting {nb_to_extract} values...")
    text_proc.ingest(data_text_list)
    curr_proc_text_value: tuple[int, str] = text_proc.output()
    print(f"Text value {curr_proc_text_value[0]}: {curr_proc_text_value[1]}")
    print()
    print("Testing Log Processor...")
    log_proc: LogProcessor = LogProcessor()
    print(
        f"Trying to validate input '{text_value}': "
        f"{log_proc.validate(text_value)}"
    )
    data_dict_list: list[dict[str, str]] = [
        {"log_level": "NOTICE", "log_message": "Connection to server"},
        {"log_level": "ERROR", "log_message": "Unauthorized access!!"},
    ]
    nb_to_extract = 2
    print(f"Extracting {nb_to_extract} values...")
    log_proc.ingest(data_dict_list)
    for _ in range(0, nb_to_extract):
        curr_proc_log_value: tuple[int, str] = log_proc.output()
        print(
            f"Numeric value {curr_proc_log_value[0]}: {curr_proc_log_value[1]}"
        )
