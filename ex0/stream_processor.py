from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, list) and all(isinstance
           (x, (int, float)) for x in data):
            return True
        return False

    def process(self, data: Any) -> str:
        if not self.validate(data):
            return "Invalid data"
        count = len(data)
        total = sum(data)
        avg = total / count if count > 0 else 0.0
        return f"Processed {count} numeric values, sum={total}, avg={avg}\n"


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        return isinstance(data, str)

    def process(self, data: Any) -> str:
        if not self.validate(data):
            return "Invalid data"
        char_count = len(data)
        word_count = len(data.split())
        return f"Processed text: {char_count} characters, {word_count} words\n"


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        return isinstance(data, str) and ":" in data

    def process(self, data: Any) -> str:
        if not self.validate(data):
            return "Invalid data"

        parts = data.split(":", 1)
        level = parts[0].strip()
        message = parts[1].strip()

        if "ERROR" in level:
            prefix = "ALERT"
        else:
            prefix = "INFO"

        return f"[{prefix}] {level} level detected: {message}\n"


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    print("Initializing Numeric Processor...")
    num_proc = NumericProcessor()
    data_num = [1, 2, 3, 4, 5]
    print(f"Processing data: {data_num}")
    if num_proc.validate(data_num):
        print("Validation: Numeric data verified")
        result = num_proc.process(data_num)
        print(num_proc.format_output(result))

    print("Initializing Text Processor...")
    text_proc = TextProcessor()
    data_text = "Hello Nexus World"
    print(f"Processing data: \"{data_text}\"")
    if text_proc.validate(data_text):
        print("Validation: Text data verified")
        result = text_proc.process(data_text)
        print(text_proc.format_output(result))

    print("Initializing Log Processor...")
    log_proc = LogProcessor()
    data_log = "ERROR: Connection timeout"
    print(f"Processing data: \"{data_log}\"")
    if log_proc.validate(data_log):
        print("Validation: Log entry verified")
        result = log_proc.process(data_log)
        print(log_proc.format_output(result))

    print("=== Polymorphic Processing Demo ===\n")
    print("Processing multiple data types through same interface...\n")

    processors = [num_proc, text_proc, log_proc]

    datasets = [
        [2, 2, 2],
        "Hello World 42",
        "INFO: System ready"
    ]
    datasets = [
        [2, 2, 2],
        "Hello World!",
        "INFO: System ready"
    ]

    for i, (proc, data) in enumerate(zip(processors, datasets), 1):
        res = proc.process(data)
        print(f"Result {i}: {res}")

    print("Foundation systems online. Nexus ready for advanced streams.")
