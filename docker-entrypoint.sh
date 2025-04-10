#!/bin/bash


echo "Kafka is ready!"

# Create the Kafka topic using the Kafka CLI
echo "Creating Kafka topic 'taxi_topic'..."
docker exec kafka kafka-topics.sh --create \
  --if-not-exists \
  --bootstrap-server kafka:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic taxi_topic

# Wait for PostgreSQL to be ready (using the service name 'postgres')
echo "Waiting for PostgreSQL..."
until PGPASSWORD=Agents1234 psql -h floo3.postgres.database.azure.com -U floo -d postgres -p 5432 -c "SELECT 1" > /dev/null 2>&1; do
  echo "Waiting for PostgreSQL to be available..."
  sleep 5
done
echo "PostgreSQL is ready!"

# Stream data to Kafka using the local Kafka CLI before submitting Flink job
echo "Starting data streaming to Kafka..."
cat data_stream.csv | docker exec kafka kafka-console-producer.sh --bootstrap-server kafka:9092 --topic taxi_topic

echo "Data streaming complete!"

# Submit the Flink job by executing a command in the jobmanager container
echo "All services are up. Submitting Flink job..."
docker exec jobmanager flink run -c TaxiTripStreamJob /opt/flink/usrlib/taxi-trip-stream-1.0-SNAPSHOT.jar

echo done

# Keep the container running
tail -f /dev/null
