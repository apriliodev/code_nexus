from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        super().__init__()
        self.liste: list = []
        self.rank = 0
        self.total = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        element = self.liste.pop(0)
        actual_rank = self.rank
        self.rank += 1
        return (actual_rank, element)


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.name = "Numeric Processor"

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        else:
            if isinstance(data, list):
                for donnee in data:
                    if not isinstance(donnee, (int, float)):
                        return False
                return True
        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise Exception("Improper numeric data")
        if isinstance(data, (int, float)):
            self.liste.append(str(data))
            self.total += 1
        else:
            if isinstance(data, list):
                for donnee in data:
                    self.liste.append(str(donnee))
                self.total += len(data)


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.name = "Text Processor"

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        else:
            if isinstance(data, list):
                for donnee in data:
                    if not isinstance(donnee, str):
                        return False
                return True
        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise Exception("Improper text data")
        if isinstance(data, str):
            self.liste.append(data)
            self.total += 1
        elif isinstance(data, list):
            for donnee in data:
                self.liste.append(donnee)
            self.total += len(data)


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.name = "Log Processor"

    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            for key, value in data.items():
                if (not isinstance(key, str)
                        or not isinstance(value, str)):
                    return False
            return True

        if isinstance(data, list):
            for donnee in data:
                if not isinstance(donnee, dict):
                    return False
                if isinstance(donnee, dict):
                    for key, value in donnee.items():
                        if (not isinstance(key, str)
                                or not isinstance(value, str)):
                            return False
            return True
        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise Exception("Improper log data")
        if isinstance(data, dict):
            self.liste.append(f"{data['log_level']}: {data['log_message']}")
            self.total += 1
        if isinstance(data, list):
            for donnee in data:
                if isinstance(donnee, dict):
                    self.liste.append(
                        f"{donnee['log_level']}: {donnee['log_message']}")
            self.total += len(data)


class DataStream():
    def __init__(self) -> None:
        self.liste: list = []
        pass

    def register_processor(self, proc: DataProcessor) -> None:
        self.liste.append(proc)

    def process_stream(self, data: list):
        for d in data:
            found = False
            for liste in self.liste:
                if liste.validate(d):
                    liste.ingest(d)
                    found = True
                    break
            if not found:
                print(
                    f"DataStream error - Can't process element in stream: {d}"
                )

    def print_processors_stats(self):
        if not self.liste:
            print("No processor found, no data")
        for proc in self.liste:
            print(
                f"{proc.name}: total {proc.total} items processed, "
                f"remaining {len(proc.liste)} on processor"
            )


def data_stream():
    stream = DataStream()
    numeric = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()
    batch = [
        'Hello world',
        [3.14, -1, 2.71],
        [
            {'log_level': 'WARNING', 'log_message':
             'Telnet access! Use ssh instead'},
            {'log_level': 'INFO', 'log_message': 'User wil is connected'}
        ],
        42,
        ['Hi', 'five']
    ]
    print("=== Code Nexus - Data Stream ===")
    print("\nInitialize Data Stream...")
    stream.print_processors_stats()
    print("\nRegistering Numeric Processor")
    stream.register_processor(numeric)
    print(f"\nSend first batch of data on stream: {batch}")
    stream.process_stream(batch)
    print("\nRegistering other data processors")
    print("Send the same batch again")
    stream.register_processor(text)
    stream.register_processor(log)
    stream.process_stream(batch)
    print("== DataStream statistics ==")
    stream.print_processors_stats()
    print(
        "\nConsume some elements from the data processors: "
        "Numeric 3, Text 2, Log 1"
    )
    for i in range(3):
        numeric.output()
    for i in range(2):
        text.output()
    log.output()
    print("== DataStream statistics ==")
    stream.print_processors_stats()


if __name__ == "__main__":
    data_stream()
