import time
import threading

class SnowflakeGenerator:
    def __init__(self, machine_id=1, datacenter_id=1):
        # Epoch start — Jan 1 2024 00:00:00 UTC in milliseconds
        self.epoch = 1704067200000

        self.machine_id = machine_id        # 0-31 (5 bits)
        self.datacenter_id = datacenter_id  # 0-31 (5 bits)
        self.sequence = 0
        self.last_timestamp = -1
        self.lock = threading.Lock()

        # Bit shift positions
        self.machine_id_shift = 12
        self.datacenter_id_shift = 17
        self.timestamp_shift = 22

        # Max values
        self.max_sequence = 4095       # 12 bits → 2^12 - 1
        self.max_machine_id = 31       # 5 bits  → 2^5 - 1
        self.max_datacenter_id = 31    # 5 bits  → 2^5 - 1

        if machine_id > self.max_machine_id:
            raise ValueError(f"machine_id must be between 0 and {self.max_machine_id}")
        if datacenter_id > self.max_datacenter_id:
            raise ValueError(f"datacenter_id must be between 0 and {self.max_datacenter_id}")

    def _current_ms(self):
        return int(time.time() * 1000)

    def _wait_next_ms(self, last_timestamp):
        timestamp = self._current_ms()
        while timestamp <= last_timestamp:
            timestamp = self._current_ms()
        return timestamp

    def generate(self):
        with self.lock:
            timestamp = self._current_ms()

            if timestamp < self.last_timestamp:
                raise Exception(
                    f"Clock moved backwards. Refusing to generate ID for "
                    f"{self.last_timestamp - timestamp}ms"
                )

            if timestamp == self.last_timestamp:
                # Same millisecond — increment sequence
                self.sequence = (self.sequence + 1) & self.max_sequence
                if self.sequence == 0:
                    # Sequence exhausted — wait for next millisecond
                    timestamp = self._wait_next_ms(self.last_timestamp)
            else:
                # New millisecond — reset sequence
                self.sequence = 0

            self.last_timestamp = timestamp

            snowflake_id = (
                ((timestamp - self.epoch) << self.timestamp_shift) |
                (self.datacenter_id << self.datacenter_id_shift) |
                (self.machine_id << self.machine_id_shift) |
                self.sequence
            )

            return snowflake_id

    def parse(self, snowflake_id):
        """Extract components from a Snowflake ID"""
        timestamp_ms = (snowflake_id >> self.timestamp_shift) + self.epoch
        datacenter_id = (snowflake_id >> self.datacenter_id_shift) & self.max_datacenter_id
        machine_id = (snowflake_id >> self.machine_id_shift) & self.max_machine_id
        sequence = snowflake_id & self.max_sequence

        return {
            "snowflake_id": snowflake_id,
            "timestamp_ms": timestamp_ms,
            "created_at": time.strftime(
                '%Y-%m-%d %H:%M:%S',
                time.gmtime(timestamp_ms / 1000)
            ),
            "datacenter_id": datacenter_id,
            "machine_id": machine_id,
            "sequence": sequence
        }