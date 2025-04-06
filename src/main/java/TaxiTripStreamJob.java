import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class TaxiTripStreamJob {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure Kafka consumer properties for Docker environment
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092"); // Changed to Docker container name
        properties.setProperty("group.id", "flink-taxi-group");

        // Create a Kafka consumer that reads raw CSV strings.
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("taxi_topic", new SimpleStringSchema(), properties);
        // Here you can assign watermarks if event-time processing is required.
        consumer.assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        // Create a DataStream from Kafka.
        DataStream<String> stream = env.addSource(consumer);

        // Parse the CSV into a TaxiTrip POJO and filter out any parsing errors.
        DataStream<TaxiTrip> trips = stream
            .map(TaxiTrip::fromCsv)
            .filter(trip -> trip != null);

        // Enrich the data: calculate trip duration.
        DataStream<TaxiTrip> enrichedTrips = trips.map(trip -> {
            trip.calculateDuration();
            return trip;
        });

        // Filter out any trips with outlier values.
        DataStream<TaxiTrip> filteredTrips = enrichedTrips.filter(trip ->
            trip.getTripDistance() < 1000 && trip.getTotalAmount() < 500);

        // Assign timestamps and watermarks based on pickup time.
        DataStream<TaxiTrip> timestampedTrips = filteredTrips.assignTimestampsAndWatermarks(
            WatermarkStrategy.<TaxiTrip>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((trip, ts) -> trip.getPickupTimeMillis())
        );

        // Save raw trip data to PostgreSQL
        timestampedTrips.addSink(
            JdbcSink.sink(
                "INSERT INTO taxi_trips (vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, " +
                "rate_code_id, store_and_fwd_flag, pu_location_id, do_location_id, payment_type, fare_amount, extra, " +
                "mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, " +
                "airport_fee, trip_duration) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (statement, trip) -> {
                    statement.setInt(1, trip.getVendorID());
                    statement.setTimestamp(2, java.sql.Timestamp.valueOf(trip.getPickupDatetime()));
                    statement.setTimestamp(3, java.sql.Timestamp.valueOf(trip.getDropoffDatetime()));
                    statement.setInt(4, trip.getPassengerCount());
                    statement.setDouble(5, trip.getTripDistance());
                    statement.setInt(6, trip.getRatecodeID());
                    statement.setString(7, trip.getStoreAndFwdFlag());
                    statement.setInt(8, trip.getPuLocationID());
                    statement.setInt(9, trip.getDoLocationID());
                    statement.setInt(10, trip.getPaymentType());
                    statement.setDouble(11, trip.getFareAmount());
                    statement.setDouble(12, trip.getExtra());
                    statement.setDouble(13, trip.getMtaTax());
                    statement.setDouble(14, trip.getTipAmount());
                    statement.setDouble(15, trip.getTollsAmount());
                    statement.setDouble(16, trip.getImprovementSurcharge());
                    statement.setDouble(17, trip.getTotalAmount());
                    statement.setDouble(18, trip.getCongestionSurcharge());
                    statement.setDouble(19, trip.getAirportFee());
                    statement.setLong(20, trip.getTripDuration());
                },
                JdbcExecutionOptions.builder()
                    .withBatchSize(1000)
                    .withBatchIntervalMs(200)
                    .withMaxRetries(5)
                    .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl("jdbc:postgresql://postgres:5432/taxi_db") // Using container hostname
                    .withDriverName("org.postgresql.Driver")
                    .withUsername("postgres")
                    .withPassword("postgres")
                    .build()
            )
        );

        // Compute hourly aggregates (e.g., average fare) using a tumbling window.
        DataStream<HourlyAggregate> hourlyAggregates = timestampedTrips
            .keyBy(TaxiTrip::getPickupHour)
            .window(TumblingEventTimeWindows.of(Time.hours(1)))
            .apply(new TaxiTripHourlyAggregateWindowFunction());

        // Save hourly aggregates to PostgreSQL
        hourlyAggregates.addSink(
            JdbcSink.sink(
                "INSERT INTO hourly_aggregates (hour, window_start, window_end, average_fare, trip_count) VALUES (?, ?, ?, ?, ?)",
                (statement, aggregate) -> {
                    statement.setInt(1, aggregate.getHour());
                    statement.setTimestamp(2, new java.sql.Timestamp(aggregate.getWindowStart()));
                    statement.setTimestamp(3, new java.sql.Timestamp(aggregate.getWindowEnd()));
                    statement.setDouble(4, aggregate.getAverageFare());
                    statement.setInt(5, aggregate.getTripCount());
                },
                JdbcExecutionOptions.builder()
                    .withBatchSize(100)
                    .withBatchIntervalMs(200)
                    .withMaxRetries(5)
                    .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl("jdbc:postgresql://postgres:5432/taxi_db") // Using container hostname
                    .withDriverName("org.postgresql.Driver")
                    .withUsername("postgres")
                    .withPassword("postgres")
                    .build()
            )
        );

        // For demonstration, print the aggregates.
        hourlyAggregates.print();

        // Execute the job.
        env.execute("Taxi Trip Streaming Job");
    }
}

// POJO for TaxiTrip.
class TaxiTrip {
    private int vendorID;
    private LocalDateTime pickupDatetime;
    private LocalDateTime dropoffDatetime;
    private int passengerCount;
    private double tripDistance;
    private int ratecodeID;
    private String storeAndFwdFlag;
    private int puLocationID;
    private int doLocationID;
    private int paymentType;
    private double fareAmount;
    private double extra;
    private double mtaTax;
    private double tipAmount;
    private double tollsAmount;
    private double improvementSurcharge;
    private double totalAmount;
    private double congestionSurcharge;
    private double airportFee;

    // Calculated field
    private long tripDuration; // in seconds

    // Example CSV format: "VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,..."
    public static TaxiTrip fromCsv(String csvLine) {
        try {
            String[] parts = csvLine.split(",");
            TaxiTrip trip = new TaxiTrip();
            trip.vendorID = Integer.parseInt(parts[0]);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            trip.pickupDatetime = LocalDateTime.parse(parts[1], formatter);
            trip.dropoffDatetime = LocalDateTime.parse(parts[2], formatter);
            trip.passengerCount = Integer.parseInt(parts[3]);
            trip.tripDistance = Double.parseDouble(parts[4]);
            trip.ratecodeID = Integer.parseInt(parts[5]);
            trip.storeAndFwdFlag = parts[6];
            trip.puLocationID = Integer.parseInt(parts[7]);
            trip.doLocationID = Integer.parseInt(parts[8]);
            trip.paymentType = Integer.parseInt(parts[9]);
            trip.fareAmount = Double.parseDouble(parts[10]);
            trip.extra = Double.parseDouble(parts[11]);
            trip.mtaTax = Double.parseDouble(parts[12]);
            trip.tipAmount = Double.parseDouble(parts[13]);
            trip.tollsAmount = Double.parseDouble(parts[14]);
            trip.improvementSurcharge = Double.parseDouble(parts[15]);
            trip.totalAmount = Double.parseDouble(parts[16]);
            trip.congestionSurcharge = Double.parseDouble(parts[17]);
            trip.airportFee = Double.parseDouble(parts[18]);
            return trip;
        } catch (Exception e) {
            // Log parsing error in production code.
            return null;
        }
    }

    // Calculate trip duration in seconds.
    public void calculateDuration() {
        this.tripDuration = java.time.Duration.between(pickupDatetime, dropoffDatetime).getSeconds();
    }

    // Return pickup time as milliseconds for event time processing.
    public long getPickupTimeMillis() {
        return pickupDatetime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    // Get hour of the pickup for keying.
    public int getPickupHour() {
        return pickupDatetime.getHour();
    }

    // Getters for all fields (needed for JdbcSink)
    public int getVendorID() {
        return vendorID;
    }

    public LocalDateTime getPickupDatetime() {
        return pickupDatetime;
    }

    public LocalDateTime getDropoffDatetime() {
        return dropoffDatetime;
    }

    public int getPassengerCount() {
        return passengerCount;
    }

    public double getTripDistance() {
        return tripDistance;
    }

    public int getRatecodeID() {
        return ratecodeID;
    }

    public String getStoreAndFwdFlag() {
        return storeAndFwdFlag;
    }

    public int getPuLocationID() {
        return puLocationID;
    }

    public int getDoLocationID() {
        return doLocationID;
    }

    public int getPaymentType() {
        return paymentType;
    }

    public double getFareAmount() {
        return fareAmount;
    }

    public double getExtra() {
        return extra;
    }

    public double getMtaTax() {
        return mtaTax;
    }

    public double getTipAmount() {
        return tipAmount;
    }

    public double getTollsAmount() {
        return tollsAmount;
    }

    public double getImprovementSurcharge() {
        return improvementSurcharge;
    }

    public double getTotalAmount() {
        return totalAmount;
    }

    public double getCongestionSurcharge() {
        return congestionSurcharge;
    }

    public double getAirportFee() {
        return airportFee;
    }

    public long getTripDuration() {
        return tripDuration;
    }
}

// POJO for hourly aggregate results.
class HourlyAggregate {
    private int hour;
    private long windowStart;
    private long windowEnd;
    private double averageFare;
    private int tripCount;

    public HourlyAggregate(int hour, long windowStart, long windowEnd, double averageFare, int tripCount) {
        this.hour = hour;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.averageFare = averageFare;
        this.tripCount = tripCount;
    }

    // Getters for JdbcSink
    public int getHour() {
        return hour;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public double getAverageFare() {
        return averageFare;
    }

    public int getTripCount() {
        return tripCount;
    }

    @Override
    public String toString() {
        return "Hour: " + hour + ", Window: [" + windowStart + ", " + windowEnd + "], Average Fare: " 
               + averageFare + ", Trips: " + tripCount;
    }
}

// WindowFunction to compute hourly aggregates.
class TaxiTripHourlyAggregateWindowFunction implements WindowFunction<TaxiTrip, HourlyAggregate, Integer, TimeWindow> {
    @Override
    public void apply(Integer hour, TimeWindow window, Iterable<TaxiTrip> input, Collector<HourlyAggregate> out) {
        int count = 0;
        double fareSum = 0.0;
        for (TaxiTrip trip : input) {
            fareSum += trip.getFareAmount();
            count++;
        }
        double averageFare = count > 0 ? fareSum / count : 0;
        out.collect(new HourlyAggregate(hour, window.getStart(), window.getEnd(), averageFare, count));
    }
}