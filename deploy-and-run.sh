#!/bin/bash

# Build the project
echo "Building JAR file..."
cd /workspaces/Datastream
mvn clean package

# Copy JAR to JobManager
echo "Copying JAR to JobManager..."
docker exec jobmanager mkdir -p /opt/flink/usrlib/
docker cp /workspaces/Datastream/target/taxi-trip-stream-1.0-SNAPSHOT.jar jobmanager:/opt/flink/usrlib/

# Submit the Flink job
echo "Submitting Flink job..."
docker exec jobmanager flink run -c TaxiTripStreamJob /opt/flink/usrlib/taxi-trip-stream-1.0-SNAPSHOT.jar

# Check if job is running
echo "Checking job status..."
docker exec jobmanager flink list

# Send data to Kafka (uncomment when ready to process data)
echo "Ready to send data to Kafka"
echo "Run the following command to start processing data:"
echo "cat data_stream.csv | while read line; do echo \$line | docker exec -i kafka kafka-console-producer.sh --bootstrap-server kafka:9092 --topic taxi_topic; done"