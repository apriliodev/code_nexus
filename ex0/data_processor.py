from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        super().__init__()
        self.liste: list = []
        self.rank = 0

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
        else:
            if isinstance(data, list):
                for donnee in data:
                    self.liste.append(str(donnee))


class TextProcessor(DataProcessor):
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
        elif isinstance(data, list):
            for donnee in data:
                self.liste.append(donnee)


class LogProcessor(DataProcessor):
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
        if isinstance(data, list):
            for donnee in data:
                if isinstance(donnee, dict):
                    self.liste.append(
                        f"{donnee['log_level']}: {donnee['log_message']}")


def data_processor() -> None:
    numeric = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()

    print("=== Code Nexus - Data Processor ===")
    res = numeric.validate(42)
    print("\nTesting Numeric Processor...")
    print(f"Trying to validate input ’42’: {res}")
    res = numeric.validate("Hello")
    print(f"Trying to validate input ’Hello’: {res}")
    print("Test invalid ingestion of string ’foo’ without prior validation:")
    try:
        numeric.ingest("foo")
    except Exception as e:
        print(f"Got exception: {e}")
    print("Processing data: [1, 2, 3, 4, 5]")
    numeric.ingest([1, 2, 3, 4, 5])
    print("Extracting 3 values...")
    for i in range(3):
        result = numeric.output()
        print(f"Numeric value {result[0]}: {result[1]}")

    print("\nTesting Text Processor...")
    res = text.validate(42)
    print(f"Trying to validate input '42': {res}")
    print("Processing data: [’Hello’, ’Nexus’, ’World’]")
    text.ingest(['Hello', 'Nexus', 'World'])
    print("Extracting 1 value...")
    result = text.output()
    print(f"Text value {result[0]}: {result[1]}")

    print("\nTesting Log Processor...")
    res = log.validate("Hello")
    print(f"Trying to validate input ’Hello’: {res}")
    print("Processing data: [{’log_level’: ’NOTICE’, ’log_message’: ’")
    print("Connection to server’}, {’log_level’: ’ERROR’, ’log_message’: ’")
    print("Unauthorized access!!’}]")
    log.ingest([{'log_level': 'NOTICE', 'log_message':
                 'Connection to server'}, {
               'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}])
    print("Extracting 2 values...")
    for i in range(2):
        result = log.output()
        print(f"Log entry {result[0]}: {result[1]}")


if __name__ == "__main__":
    data_processor()
