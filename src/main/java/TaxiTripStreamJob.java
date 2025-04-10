import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;  // Updated import
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;

public class TaxiTripStreamJob {

    // Azure PostgreSQL connection details
    private static final String DB_URL = "jdbc:postgresql://floo3.postgres.database.azure.com:5432/postgres";
    private static final String DB_USERNAME = "floo";
    private static final String DB_PASSWORD = "Agents1234";
    private static final String DB_DRIVER = "org.postgresql.Driver";

    public static void main(String[] args) throws Exception {
        // Create necessary database tables if not exist
        createSchemaIfNotExists();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000); // Checkpoint every 60s

        // -------------------------------
        // 1. Kafka Source Setup
        // -------------------------------
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("reconnect.backoff.ms", "1000");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("taxi_topic")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .setProperties(kafkaProps)
            .build();

        DataStream<String> rawStream = env.fromSource(
            kafkaSource,
            WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withIdleness(Duration.ofMinutes(1)),
            "Kafka Source"
        );

        // -------------------------------
        // 2. Parse CSV records to TaxiTrip objects
        // -------------------------------
        DataStream<TaxiTrip> timestampedTrips = rawStream
            .map(rawMsg -> {
                System.out.println("Parsing CSV: " + rawMsg);
                TaxiTrip trip = TaxiTrip.fromCsv(rawMsg);
                return trip;
            })
            .filter(trip -> trip != null)
            .name("Parse-CSV");

        // -------------------------------
        // 3. Sink raw TaxiTrip data into taxi_trips table
        // -------------------------------
        timestampedTrips.addSink(
            JdbcSink.sink(
                "INSERT INTO taxi_trips (vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, " +
                "rate_code_id, store_and_fwd_flag, pu_location_id, do_location_id, payment_type, fare_amount, extra, " +
                "mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, " +
                "airport_fee, trip_duration) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (stmt, trip) -> {
                    stmt.setInt(1, trip.getVendorID());
                    stmt.setTimestamp(2, java.sql.Timestamp.valueOf(trip.getPickupDatetime()));
                    stmt.setTimestamp(3, java.sql.Timestamp.valueOf(trip.getDropoffDatetime()));
                    stmt.setInt(4, trip.getPassengerCount());
                    stmt.setDouble(5, trip.getTripDistance());
                    stmt.setInt(6, trip.getRatecodeID());
                    stmt.setString(7, trip.getStoreAndFwdFlag());
                    stmt.setInt(8, trip.getPuLocationID());
                    stmt.setInt(9, trip.getDoLocationID());
                    stmt.setInt(10, trip.getPaymentType());
                    stmt.setDouble(11, trip.getFareAmount());
                    stmt.setDouble(12, trip.getExtra());
                    stmt.setDouble(13, trip.getMtaTax());
                    stmt.setDouble(14, trip.getTipAmount());
                    stmt.setDouble(15, trip.getTollsAmount());
                    stmt.setDouble(16, trip.getImprovementSurcharge());
                    stmt.setDouble(17, trip.getTotalAmount());
                    stmt.setDouble(18, trip.getCongestionSurcharge());
                    stmt.setDouble(19, trip.getAirportFee());
                    stmt.setLong(20, trip.getTripDuration());
                },
                JdbcExecutionOptions.builder()
                    .withBatchSize(100)
                    .withBatchIntervalMs(1000)
                    .withMaxRetries(5)
                    .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl(DB_URL)
                    .withDriverName(DB_DRIVER)
                    .withUsername(DB_USERNAME)
                    .withPassword(DB_PASSWORD)
                    .build()
            )
        ).name("JDBC-Taxi-Trips-Sink");

        // -------------------------------
        // 4. Transform TaxiTrip into YellowTaxiStreamRecord and sink into yellow_taxi_stream table
        // -------------------------------
        DataStream<YellowTaxiStreamRecord> yellowStream = timestampedTrips
            .map(YellowTaxiStreamRecord::fromTrip)
            .name("Transform-To-YellowTaxiStream");

        yellowStream.addSink(
            JdbcSink.sink(
                "INSERT INTO yellow_taxi_stream (vendor_name, pickup_datetime, dropoff_datetime, passenger_count, " +
                "trip_distance, rate_code_name, store_and_fwd_flag, pu_location_id, do_location_id, payment_type_name, " +
                "fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, " +
                "congestion_surcharge, airport_fee) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (stmt, record) -> {
                    stmt.setString(1, record.vendorName);
                    stmt.setTimestamp(2, java.sql.Timestamp.valueOf(record.pickupDatetime));
                    stmt.setTimestamp(3, java.sql.Timestamp.valueOf(record.dropoffDatetime));
                    stmt.setInt(4, record.passengerCount);
                    stmt.setDouble(5, record.tripDistance);
                    stmt.setString(6, record.rateCodeName);
                    stmt.setString(7, record.storeAndFwdFlag);
                    stmt.setInt(8, record.puLocationID);
                    stmt.setInt(9, record.doLocationID);
                    stmt.setString(10, record.paymentTypeName);
                    stmt.setDouble(11, record.fareAmount);
                    stmt.setDouble(12, record.extra);
                    stmt.setDouble(13, record.mtaTax);
                    stmt.setDouble(14, record.tipAmount);
                    stmt.setDouble(15, record.tollsAmount);
                    stmt.setDouble(16, record.improvementSurcharge);
                    stmt.setDouble(17, record.totalAmount);
                    stmt.setDouble(18, record.congestionSurcharge);
                    stmt.setDouble(19, record.airportFee);
                },
                JdbcExecutionOptions.builder()
                    .withBatchSize(20)
                    .withBatchIntervalMs(2000)
                    .withMaxRetries(3)
                    .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl(DB_URL)
                    .withDriverName(DB_DRIVER)
                    .withUsername(DB_USERNAME)
                    .withPassword(DB_PASSWORD)
                    .build()
            )
        ).name("JDBC-Yellow-Taxi-Stream-Sink");

        // -------------------------------
        // 5. Hourly Aggregation using Tumbling Windows and sink into hourly_aggregates table
        // -------------------------------
        DataStream<HourlyAggregate> hourlyAggregates = timestampedTrips
            .keyBy(TaxiTrip::getPickupHour)
            .window(org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows.of(Time.hours(1)))
            .allowedLateness(Time.minutes(5))
            .apply(new TaxiTripHourlyAggregateWindowFunction());

        hourlyAggregates.addSink(
            JdbcSink.sink(
                "INSERT INTO hourly_aggregates (hour, window_start, window_end, average_fare, trip_count) VALUES (?, ?, ?, ?, ?)",
                (stmt, agg) -> {
                    stmt.setInt(1, agg.getHour());
                    stmt.setTimestamp(2, new java.sql.Timestamp(agg.getWindowStart()));
                    stmt.setTimestamp(3, new java.sql.Timestamp(agg.getWindowEnd()));
                    stmt.setDouble(4, agg.getAverageFare());
                    stmt.setInt(5, agg.getTripCount());
                },
                JdbcExecutionOptions.builder()
                    .withBatchSize(10)
                    .withBatchIntervalMs(1000)
                    .withMaxRetries(5)
                    .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl(DB_URL)
                    .withDriverName(DB_DRIVER)
                    .withUsername(DB_USERNAME)
                    .withPassword(DB_PASSWORD)
                    .build()
            )
        ).name("JDBC-Hourly-Aggregates-Sink");

        // Optionally, print streams for debugging
        rawStream.print("Raw-Message");
        timestampedTrips.print("Final-Trip");
        hourlyAggregates.print("Hourly-Aggregate");

        env.execute("Taxi Trip Streaming Job");
    }

    private static void createSchemaIfNotExists() {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
             Statement stmt = conn.createStatement()) {

            // Create taxi_trips table (raw data)
            String createTaxiTripsTable = "CREATE TABLE IF NOT EXISTS taxi_trips (" +
                "id SERIAL PRIMARY KEY, vendor_id INTEGER, pickup_datetime TIMESTAMP, " +
                "dropoff_datetime TIMESTAMP, passenger_count INTEGER, trip_distance DOUBLE PRECISION, " +
                "rate_code_id INTEGER, store_and_fwd_flag VARCHAR(1), pu_location_id INTEGER, " +
                "do_location_id INTEGER, payment_type INTEGER, fare_amount DOUBLE PRECISION, " +
                "extra DOUBLE PRECISION, mta_tax DOUBLE PRECISION, tip_amount DOUBLE PRECISION, " +
                "tolls_amount DOUBLE PRECISION, improvement_surcharge DOUBLE PRECISION, " +
                "total_amount DOUBLE PRECISION, congestion_surcharge DOUBLE PRECISION, " +
                "airport_fee DOUBLE PRECISION, trip_duration BIGINT" +
                ")";
            stmt.executeUpdate(createTaxiTripsTable);

            // Create hourly_aggregates table
            String createHourlyAggregatesTable = "CREATE TABLE IF NOT EXISTS hourly_aggregates (" +
                "id SERIAL PRIMARY KEY, hour INTEGER, window_start TIMESTAMP, window_end TIMESTAMP, " +
                "average_fare DOUBLE PRECISION, trip_count INTEGER" +
                ")";
            stmt.executeUpdate(createHourlyAggregatesTable);

            // Create yellow_taxi_stream table
            String createYellowTaxiStreamTable = "CREATE TABLE IF NOT EXISTS yellow_taxi_stream (" +
                "id SERIAL PRIMARY KEY, vendor_name VARCHAR(50), pickup_datetime TIMESTAMP, " +
                "dropoff_datetime TIMESTAMP, passenger_count INTEGER, trip_distance DOUBLE PRECISION, " +
                "rate_code_name VARCHAR(50), store_and_fwd_flag VARCHAR(1), pu_location_id INTEGER, " +
                "do_location_id INTEGER, payment_type_name VARCHAR(50), fare_amount DOUBLE PRECISION, " +
                "extra DOUBLE PRECISION, mta_tax DOUBLE PRECISION, tip_amount DOUBLE PRECISION, " +
                "tolls_amount DOUBLE PRECISION, improvement_surcharge DOUBLE PRECISION, " +
                "total_amount DOUBLE PRECISION, congestion_surcharge DOUBLE PRECISION, " +
                "airport_fee DOUBLE PRECISION" +
                ")";
            stmt.executeUpdate(createYellowTaxiStreamTable);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
