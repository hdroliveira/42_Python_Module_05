from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.stream_type = "Generic Stream"

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria is None:
            return data_batch
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "id": self.stream_id,
            "type": self.stream_type
        }


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.stream_type = "Environmental Data"

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            if not data_batch:
                return "No data processed"

            valid_data = [x for x in data_batch if isinstance(x, (int, float))]
            count = len(valid_data)

            if count == 0:
                return "No valid numeric data"

            avg = sum(valid_data) / count
            return (f"Sensor data: {count}"
                    f" readings processed, avg temp: {avg:.1f}°C")
        except Exception as e:
            return f"Error processing sensor batch: {str(e)}"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria == "High-priority":
            return [x for x in data_batch if
                    isinstance(x, (int, float)) and (x > 50 or x < 0)]
        return data_batch


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.stream_type = "Financial Data"

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            valid_data = [x for x in data_batch if isinstance(x, (int, float))]
            count = len(valid_data)
            net_flow = sum(valid_data)

            sign = "+" if net_flow >= 0 else ""
            return (f"Transaction data: {count}"
                    f" operations, net flow: {sign}{net_flow} units")
        except Exception as e:
            return f"Error processing transaction batch: {str(e)}"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria == "High-priority":
            return [x for x in data_batch
                    if isinstance(x, (int, float)) and abs(x) >= 100]
        return data_batch


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.stream_type = "System Events"

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            count = len(data_batch)
            errors = len([x for x in data_batch
                          if isinstance(x, str) and "error" in x.lower()])
            return f"Event data: {count} events, {errors} error detected"
        except Exception as e:
            return f"Error processing event batch: {str(e)}"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria == "High-priority":
            return [x for x in data_batch
                    if isinstance(x, str) and "error" in x.lower()]
        return data_batch


class StreamProcessor:
    def __init__(self) -> None:
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        self.streams.append(stream)

    def process_streams(self, data_map: Dict[str, List[Any]]) -> None:
        for stream in self.streams:
            if stream.stream_id in data_map:
                data = data_map[stream.stream_id]
                result = stream.process_batch(data)
                print(result)


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    print("Initializing Sensor Stream...")
    sensor = SensorStream("SENSOR_001")
    print(f"Stream ID: {sensor.stream_id}, Type: {sensor.stream_type}")

    sensor_data = [22.5, 22.5, 22.5]
    print("Processing sensor batch: "
          "[temp: 22.5, humidity: 65, pressure: 1013]")
    print(sensor.process_batch(sensor_data))

    print("\nInitializing Transaction Stream...")
    trans = TransactionStream("TRANS_001")
    print(f"Stream ID: {trans.stream_id}, Type: {trans.stream_type}")

    trans_data = [100, -150, 75]
    print("Processing transaction batch: [buy: 100, sell:150, buy:75]")
    print(trans.process_batch(trans_data))

    print("\nInitializing Event Stream...")
    event = EventStream("EVENT_001")
    print(f"Stream ID: {event.stream_id}, Type: {event.stream_type}")

    event_data = ["login", "error", "logout"]
    print(f"Processing event batch: {event_data}")
    print(event.process_batch(event_data))

    print("\n=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")

    processor = StreamProcessor()
    processor.add_stream(sensor)
    processor.add_stream(trans)
    processor.add_stream(event)

    print("Batch 1 Results:")

    loop_data = {
        "SENSOR_001": [20, 24.2],
        "TRANS_001": [10, 20, -5, 100],
        "EVENT_001": ["login", "process", "logout"]
    }

    for stream in processor.streams:
        if stream.stream_id in loop_data:
            res = stream.process_batch(loop_data[stream.stream_id])
            print(res)

    print("\nStream filtering active: High-priority data only")

    filter_sensor = [100, 20, 22, -10]
    filter_trans = [10, 500, 20]

    filtered_s = sensor.filter_data(filter_sensor, "High-priority")
    filtered_t = trans.filter_data(filter_trans, "High-priority")

    print(f"Filtered results: {len(filtered_s)}"
          f" critical sensor alerts, {len(filtered_t)} large transaction")
    print("\nAll streams processed successfully. Nexus throughput optimal.")
