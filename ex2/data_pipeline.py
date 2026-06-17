import abc
from abc import ABC, abstractmethod
import typing
from typing import Any, Protocol


class DataProcessor(ABC):
    def __init__(self) -> None:
        super().__init__()
        self.liste = []
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


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


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
                if not isinstance(key, str) or not isinstance(value, str):
                    return False
            return True

        if isinstance(data, list):
            for donnee in data:
                if not isinstance(donnee, dict):
                    return False
                if isinstance(donnee, dict):
                    for key, value in donnee.items():
                        if not isinstance(key, str) or not isinstance(value, str):
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
        self.liste = []
        pass

    def register_processor(self, proc: DataProcessor) -> None:
        self.liste.append(proc)

    def process_stream(self, data: list):
        for donnee in data:
            found = False
            for liste in self.liste:
                if liste.validate(donnee):
                    liste.ingest(donnee)
                    found = True
                    break
            if not found:
                print(
                    f"DataStream error - Can't process element in stream: {donnee}")

    def print_processors_stats(self):
        if not self.liste:
            print("No processor found, no data")
        for proc in self.liste:
            print(
                f"{proc.name}: total {proc.total} items processed, "
                f"remaining {len(proc.liste)} on processor"
            )

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self.liste:
            liste = []
            for i in range(nb):
                if len(proc.liste) == 0:
                    break
                res = proc.output()
                liste.append(res)
            plugin.process_output(liste)


class CSVExporter():
    def process_output(self, data: list[tuple[int, str]]) -> None:
        valeurs = [item[1] for item in data]
        csv_lines = ",".join(valeurs)
        print(f"CSV Output:\n{csv_lines}")


class JSONExporter():
    def process_output(self, data: list[tuple[int, str]]) -> None:
        json_dict = {}
        for item in data:
            json_dict[f"item_{item[0]}"] = item[1]

        pairs: list = []
        for k, v in json_dict.items():
            pairs.append(f'"{k}": "{v}"')
        json_str = "{" + ",".join(pairs) + "}"
        print(f"JSON Output:\n{json_str}")


def data_pipeline():
    stream = DataStream()
    numeric = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()
    csv = CSVExporter()
    json = JSONExporter()
    batch1 = [
        'Hello world',
        [3.14, -1, 2.71],
        [
            {'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead'},
            {'log_level': 'INFO', 'log_message': 'User wil is connected'}
        ],
        42,
        ['Hi', 'five']
    ]

    batch2 = [
        21,
        ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        [
            {'log_level': 'ERROR', 'log_message': '500 server crash'},
            {'log_level': 'NOTICE', 'log_message': 'Certificate expires in 10 days'}
        ],
        [32, 42, 64, 84, 128, 168],
        'World hello'
    ]
    print("=== Code Nexus - Data Pipeline ===")
    print("\nInitialize Data Stream...")
    print("\n== DataStream statistics ==")
    stream.print_processors_stats()
    print("\nRegistering Processors")
    print(f"\nSend first batch of data on stream: {batch1}")
    stream.register_processor(numeric)
    stream.register_processor(text)
    stream.register_processor(log)
    stream.process_stream(batch1)
    print("\n== DataStream statistics ==")
    stream.print_processors_stats()
    print("\nSend 3 processed data from each processor to a CSV plugin:")
    stream.output_pipeline(3, csv)
    print("\n== DataStream statistics ==")
    stream.print_processors_stats()
    print(f"Send another batch of data: {batch2}")
    stream.process_stream(batch2)
    print("\n== DataStream statistics ==")
    stream.print_processors_stats()
    print("\nSend 5 processed data from each processor to a JSON plugin:")
    stream.output_pipeline(5, json)
    print("\n== DataStream statistics ==")
    stream.print_processors_stats()



if __name__ == "__main__":
    data_pipeline()
