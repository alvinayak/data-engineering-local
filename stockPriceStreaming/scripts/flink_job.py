import json
import psycopg2
from datetime import datetime, timezone
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Duration, Time
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction

DB_CONFIG = {
    "host": "pgvector",
    "port": 5432,
    "database": "vectordb",
    "user": "admin",
    "password": "password123"
}

def write_ohlc(result):
    import sys
    sys.path.append('/opt/flink/usrlib')
    from snowflake import SnowflakeGenerator
    snowflake = SnowflakeGenerator(machine_id=2, datacenter_id=1)

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    change_pct = ((result['close'] - result['open']) / result['open']) * 100

    cursor.execute("""
        INSERT INTO ohlc_results
            (symbol, window_start, window_end, open, high, low, close,
             event_count, first_message_id, last_message_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, window_start) DO UPDATE SET
            high = GREATEST(ohlc_results.high, EXCLUDED.high),
            low = LEAST(ohlc_results.low, EXCLUDED.low),
            close = EXCLUDED.close,
            event_count = EXCLUDED.event_count,
            last_message_id = EXCLUDED.last_message_id
        WHERE ohlc_results.last_message_id != EXCLUDED.last_message_id
    """, (
        result['symbol'], result['window_start'], result['window_end'],
        result['open'], result['high'], result['low'], result['close'],
        result['count'], result['first_message_id'], result['last_message_id']
    ))

    if abs(change_pct) >= 2.0:
        anomaly_id = snowflake.generate()
        cursor.execute("""
            INSERT INTO anomalies
                (id, symbol, window_start, price_change_pct, open, close)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """, (
            anomaly_id, result['symbol'], result['window_start'],
            change_pct, result['open'], result['close']
        ))
        print(f"ANOMALY: {result['symbol']} moved {change_pct:.2f}%")

    conn.commit()
    cursor.close()
    conn.close()


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
env.enable_checkpointing(60000)


class StockTimestampAssigner:
    def extract_timestamp(self, raw, record_timestamp):
        try:
            event = json.loads(raw)
            dt = datetime.fromisoformat(
                event['event_time'].replace('Z', '+00:00')
            )
            return int(dt.timestamp() * 1000)
        except:
            return record_timestamp


watermark_strategy = WatermarkStrategy \
    .for_bounded_out_of_orderness(Duration.of_seconds(10)) \
    .with_timestamp_assigner(StockTimestampAssigner())

kafka_source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:29092") \
    .set_topics("stock-prices") \
    .set_group_id("flink-stock-consumer") \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

stream = env.from_source(
    kafka_source,
    watermark_strategy,
    "Kafka Stock Source"
)


class OHLCWindowFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        events = [json.loads(e) for e in elements]
        events.sort(key=lambda e: e['event_time'])
        open_p = events[0]['price']
        close_p = events[-1]['price']
        high_p = max(e['price'] for e in events)
        low_p = min(e['price'] for e in events)
        count = len(events)
        first_id = events[0]['message_id']
        last_id = events[-1]['message_id']
        window_start = datetime.fromtimestamp(
            context.window().start / 1000, tz=timezone.utc
        ).strftime('%Y-%m-%d %H:%M:%S')
        window_end = datetime.fromtimestamp(
            context.window().end / 1000, tz=timezone.utc
        ).strftime('%Y-%m-%d %H:%M:%S')
        result = {
            'symbol': key,
            'window_start': window_start,
            'window_end': window_end,
            'open': open_p,
            'high': high_p,
            'low': low_p,
            'close': close_p,
            'count': count,
            'first_message_id': first_id,
            'last_message_id': last_id
        }
        print(f"Window closed: {result}")
        write_ohlc(result)
        yield str(result)


stream \
    .key_by(lambda raw: json.loads(raw)['symbol']) \
    .window(TumblingEventTimeWindows.of(Time.minutes(1))) \
    .allowed_lateness(10000) \
    .process(OHLCWindowFunction()) \
    .print()

print("Job graph built, submitting...")
print(env.get_execution_plan())
env.execute("Stock Price OHLC Pipeline")