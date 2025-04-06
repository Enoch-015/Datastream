#!/bin/bash

# Wait for the jobmanager to be ready
until $(curl --output /dev/null --silent --head --fail http://jobmanager:8081); do
  echo "Waiting for JobManager..."
  sleep 5
done

# Wait for Kafka to be ready
echo "Waiting for Kafka..."
sleep 10
until timeout 5 bash -c "cat < /dev/null > /dev/tcp/kafka/9092"; do
  echo "Waiting for Kafka to be available..."
  sleep 5
done

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL..."
until PGPASSWORD=postgres psql -h postgres -U postgres -d taxi_db -c '\q'; do
  echo "Waiting for PostgreSQL to be available..."
  sleep 5
done

echo "All services are up. Submitting Flink job..."

# Submit the Flink job
flink run -d -c TaxiTripStreamJob /opt/flink/usrlib/taxi-trip-stream.jar

# Keep the container running
tail -f /dev/null