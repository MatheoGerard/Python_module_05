from abc import ABC, abstractmethod
import typing


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.rank: int = 0
        self.data: list[str] = []
        self.total_processed = 0

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
        elif isinstance(data, list) and all(isinstance(x, (int, float)) for x in data):
            return True
        else:
            return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if isinstance(data, (int, float)):
            self.data.append(str(data))
            self.total_processed += 1
        elif isinstance(data, list):
            for x in data:
                self.data.append(str(x))
                self.total_processed += 1
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
            self.total_processed += 1
        elif isinstance(data, list):
            for x in data:
                self.data.append(x)
                self.total_processed += 1
        else:
            raise TypeError("Got exception: Improper numeric data")


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.proc = None

    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, dict) and all(
            isinstance(k, str) and isinstance(v, str) for k, v in data.items()
        ):
            return True
        elif isinstance(data, list) and all(
            isinstance(x, dict)
            and all(isinstance(k, str) and isinstance(v, str) for k, v in x.items())
            for x in data
        ):
            return True
        else:
            return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if isinstance(data, dict):
            self.data.append(f"{data['log_level']}: {data['log_message']}")
            self.total_processed += 1
        elif isinstance(data, list):
            for x in data:
                self.data.append(f"{x['log_level']}: {x['log_message']}")
                self.total_processed += 1
        else:
            raise TypeError("Got exception: Improper numeric data")


class ExportPlugin(typing.Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None: ...


class CSVExport(ExportPlugin):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        csv_string = ",".join(name_val for _, name_val in data)
        print(csv_string)


class JSONExport(ExportPlugin):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class DataStream:
    def __init__(self) -> None:
        self.num_proc: NumericProcessor | None = None
        self.text_proc: TextProcessor | None = None
        self.log_proc: LogProcessor | None = None
        self.proc_list: list[DataProcessor | None] = [
            self.num_proc,
            self.text_proc,
            self.log_proc,
        ]

    def register_processor(self, proc: DataProcessor) -> None:
        if isinstance(proc, NumericProcessor):
            self.num_proc = proc
            self.proc_list.append(proc)
        elif isinstance(proc, TextProcessor):
            self.text_proc = proc
            self.proc_list.append(proc)
        elif isinstance(proc, LogProcessor):
            self.log_proc = proc
            self.proc_list.append(proc)
        else:
            raise Exception("This type of proc is not supported")

    def process_stream(self, stream: list[typing.Any]) -> None:
        for x in stream:
            if self.num_proc and self.num_proc.validate(x):
                self.num_proc.ingest(x)
                self.proc = self.num_proc
            elif self.text_proc and self.text_proc.validate(x):
                self.text_proc.ingest(x)
            elif self.log_proc and self.log_proc.validate(x):
                self.log_proc.ingest(x)
            else:
                print(f"DataStream error - Can't process element in stream: {x}")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        out_to_process: list[tuple[int, str]] = []
        for proc in self.proc_list:
            if proc:
                for _ in range(0, min(nb, len(proc.data))):
                    out_to_process.append(proc.output())
                plugin.process_output(out_to_process)
                out_to_process.clear()

    def print_processors_stats(self) -> None:
        if not self.num_proc and not self.text_proc and not self.log_proc:
            print("No processor found, no data")
        if self.num_proc:
            print(
                f"Numeric Processor: total {self.num_proc.total_processed} "
                f"items processed, remaining {len(self.num_proc.data)} on "
                "processor"
            )
        if self.text_proc:
            print(
                f"Text Processor: total {self.text_proc.total_processed} "
                f"items processed, remaining {len(self.text_proc.data)} on "
                "processor"
            )
        if self.log_proc:
            print(
                f"Log Processor: total {self.log_proc.total_processed} "
                f"items processed, remaining {len(self.log_proc.data)} on "
                "processor"
            )


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")
    print()
    print("Initialize Data Stream...")
    data_stream: DataStream = DataStream()
    print("== DataStream statistics ==")
    data_stream.print_processors_stats()
    print()
    print("Registering Processors")
    num_proc: NumericProcessor = NumericProcessor()
    text_proc: TextProcessor = TextProcessor()
    log_proc: LogProcessor = LogProcessor()
    data_stream.register_processor(num_proc)
    data_stream.register_processor(text_proc)
    data_stream.register_processor(log_proc)
    print()
    data: list[typing.Any] = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead",
            },
            {"log_level": "INFO", "log_message": "User wil is connected"},
        ],
        42,
        ["Hi", "five"],
    ]
    print(f"Send first batch of data on stream: {data}")
    data_stream.process_stream(data)
    print("== DataStream statistics ==")
    data_stream.print_processors_stats()
    print()
    print("Send 3 processed data from each processor to a CSV plugin:")
    plugin_csv: CSVExport = CSVExport()
    data_stream.output_pipeline(3, plugin_csv)
    print()
    print("== DataStream statistics ==")
    data_stream.print_processors_stats()
    print()
    print(
        "Send another batch of data: [21, ['I love AI', 'LLMs are wonderful', 'Stay healthy'], [{'log_level': 'ERROR', 'log_message': '500 server crash'}, {'log_level': 'NOTICE', 'log_message': 'Certificate expires in 10 days'}], [32, 42, 64, 84, 128, 168], 'World hello']"
    )
    data2: list[typing.Any] = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {"log_level": "ERROR", "log_message": "500 server crash"},
            {"log_level": "NOTICE", "log_message": "Certificate expires in 10 days"},
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello",
    ]
    print()
    data_stream.process_stream(data2)
    print("== DataStream statistics ==")
    data_stream.print_processors_stats()
