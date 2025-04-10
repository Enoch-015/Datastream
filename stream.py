import psycopg2
import subprocess
import time
import csv
import io
import os
from tqdm import tqdm
import time 

# Azure PostgreSQL database connection parameters
db_params = {
    'host': 'floo3.postgres.database.azure.com',
    'database': 'postgres',
    'user': 'floo',
    'password': 'Agents1234',
    'port': '5432',
    'sslmode': 'require'  # Required for Azure PostgreSQL
}

# Kafka parameters
kafka_topic = 'taxi_topic'
kafka_bootstrap_server = 'kafka:9092'

# Batch size and total records to stream
BATCH_SIZE = 30
TOTAL_RECORDS = 2700000

def stream_data_to_kafka():
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Set up a query to fetch data with a cursor for memory efficiency
        # Replace 'your_table' with your actual table name
        print
        cursor.execute("SELECT * FROM yellow_taxi_data")
        
        # Initialize counters and progress bar
        records_processed = 0
        progress_bar = tqdm(total=TOTAL_RECORDS, desc="Streaming data")
        
        while records_processed < TOTAL_RECORDS:
            # Fetch a batch of records
            batch = cursor.fetchmany(BATCH_SIZE)
            
            # Break if no more records
            if not batch:
                print(f"Reached end of table after {records_processed} records.")
                break
            
            # Convert batch to CSV format
            output = io.StringIO()
            csv_writer = csv.writer(output)
            for row in batch:
                csv_writer.writerow(row)
            
            # Send to Kafka using the kafka-console-producer
            kafka_cmd = [
                'docker', 'exec', '-i', 'kafka', 
                'kafka-console-producer.sh', 
                '--bootstrap-server', kafka_bootstrap_server, 
                '--topic', kafka_topic
            ]
            
            process = subprocess.Popen(
                kafka_cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            stdout, stderr = process.communicate(input=output.getvalue())
            
            if process.returncode != 0:
                print(f"Error sending batch to Kafka: {stderr}")
                continue
            
            # Update counters and progress bar
            records_processed += len(batch)
            progress_bar.update(len(batch))
            
            # Optional: Add a small delay to control the stream rate
            time.sleep(0.1)
        
        progress_bar.close()
        print(f"Completed streaming {records_processed} records to Kafka.")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    stream_data_to_kafka()