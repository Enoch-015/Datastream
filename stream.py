import asyncio
import json
import time
import psycopg2
from aiokafka import AIOKafkaProducer
from psycopg2.extras import RealDictCursor

# PostgreSQL connection info
PG_HOST = "floo3.postgres.database.azure.com"
PG_DB = "postgres"
PG_USER = "floo"
PG_PASS = "Agents1234"
PG_TABLE = "yellow_taxi_data"

# Kafka info
KAFKA_BROKER = "kafka:9092"  # Or your Docker container
KAFKA_TOPIC = "taxi_topic"

# Batch size and streaming interval (30 rows every minute)
BATCH_SIZE = 30
STREAM_INTERVAL = 60  # 60 seconds (1 minute)

# Connect to PostgreSQL asynchronously (we'll use asyncpg for better async support)
import asyncpg

async def get_postgres_connection():
    return await asyncpg.connect(
        user=PG_USER,
        password=PG_PASS,
        database=PG_DB,
        host=PG_HOST
    )

# Connect to Kafka
async def get_kafka_producer():
    producer = AIOKafkaProducer(
        loop=asyncio.get_event_loop(),
        bootstrap_servers=KAFKA_BROKER
    )
    await producer.start()
    return producer

async def stream_data_to_kafka():
    # Get PostgreSQL connection
    conn = await get_postgres_connection()
    
    # Get Kafka producer
    producer = await get_kafka_producer()
    
    # Get total number of rows (optional: if you want to track progress)
    total_rows = await conn.fetchval(f"SELECT COUNT(*) FROM {PG_TABLE}")
    print(f"Total rows to process: {total_rows}")

    offset = 0
    while offset < total_rows:
        print(f"Processing batch from offset {offset}...")

        # Fetch the batch from PostgreSQL asynchronously
        rows = await conn.fetch(f"SELECT * FROM {PG_TABLE} LIMIT {BATCH_SIZE} OFFSET {offset}")
        
        # Process the fetched rows
        tasks = []
        for row in rows:
            # Convert row to dictionary for JSON serialization
            data = dict(row)
            tasks.append(producer.send(KAFKA_TOPIC, value=data))  # Send to Kafka asynchronously

        # Wait for all Kafka sends to finish
        await asyncio.gather(*tasks)

        # Commit the batch and move the offset
        offset += len(rows)
        
        # Optional: log progress and rate limit
        print(f"Batch {offset // BATCH_SIZE} sent to Kafka, sleeping for {STREAM_INTERVAL} seconds...")
        await asyncio.sleep(STREAM_INTERVAL)

    # Clean up connections
    await conn.close()
    await producer.stop()

    print("Done streaming data to Kafka.")

# Run the streaming job asynchronously
async def main():
    await stream_data_to_kafka()

# Start the asynchronous job
if __name__ == "__main__":
    asyncio.run(main())
