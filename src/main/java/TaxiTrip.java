import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

public class TaxiTrip {
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
    private long tripDuration; // in seconds

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    public static TaxiTrip fromCsv(String csvLine) {
        try {
            System.out.println("Parsing CSV line: " + csvLine);
            
            // Skip header if present
            if (csvLine.startsWith("VendorID") || csvLine.trim().isEmpty()) {
                System.out.println("Skipping header or empty line");
                return null;
            }
            
            // Split by comma, but handle cases where commas are inside quotes
            String[] parts = csvLine.split(",");
            
            // Ensure we have the expected number of fields
            if (parts.length < 19) {
                System.err.println("Invalid CSV format. Expected at least 19 fields, got " + parts.length);
                return null;
            }

            TaxiTrip trip = new TaxiTrip();
            
            // Parse each field
            trip.vendorID = Integer.parseInt(parts[0].trim());
            
            // Parse datetime fields, handle potential formatting issues
            String pickupStr = parts[1].trim();
            String dropoffStr = parts[2].trim();
            
            try {
                trip.pickupDatetime = LocalDateTime.parse(pickupStr, formatter);
                trip.dropoffDatetime = LocalDateTime.parse(dropoffStr, formatter);
            } catch (Exception e) {
                System.err.println("Error parsing datetime with primary format: " + e.getMessage());
                // Try alternative format without microseconds
                DateTimeFormatter altFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                try {
                    trip.pickupDatetime = LocalDateTime.parse(pickupStr, altFormatter);
                    trip.dropoffDatetime = LocalDateTime.parse(dropoffStr, altFormatter);
                } catch (Exception e2) {
                    System.err.println("Failed with alternative format too: " + e2.getMessage());
                    return null;
                }
            }
            
            trip.passengerCount = Integer.parseInt(parts[3].trim());
            trip.tripDistance = Double.parseDouble(parts[4].trim());
            trip.ratecodeID = Integer.parseInt(parts[5].trim());
            trip.storeAndFwdFlag = parts[6].trim();
            trip.puLocationID = Integer.parseInt(parts[7].trim());
            trip.doLocationID = Integer.parseInt(parts[8].trim());
            trip.paymentType = Integer.parseInt(parts[9].trim());
            trip.fareAmount = Double.parseDouble(parts[10].trim());
            trip.extra = Double.parseDouble(parts[11].trim());
            trip.mtaTax = Double.parseDouble(parts[12].trim());
            trip.tipAmount = Double.parseDouble(parts[13].trim());
            trip.tollsAmount = Double.parseDouble(parts[14].trim());
            trip.improvementSurcharge = Double.parseDouble(parts[15].trim());
            trip.totalAmount = Double.parseDouble(parts[16].trim());
            
            // Handle optional fields that might not be present in all records
            if (parts.length > 17) {
                trip.congestionSurcharge = Double.parseDouble(parts[17].trim());
            }
            
            if (parts.length > 18) {
                trip.airportFee = Double.parseDouble(parts[18].trim());
            }
            
            // Calculate duration
            trip.calculateDuration();
            
            System.out.println("Successfully parsed trip: " + trip);
            return trip;
        } catch (Exception e) {
            System.err.println("Failed to parse CSV line: " + csvLine);
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    public void calculateDuration() {
        if (pickupDatetime != null && dropoffDatetime != null) {
            this.tripDuration = ChronoUnit.SECONDS.between(pickupDatetime, dropoffDatetime);
        } else {
            this.tripDuration = 0;
        }
    }

    // Getters and setters
    public int getVendorID() { return vendorID; }
    public LocalDateTime getPickupDatetime() { return pickupDatetime; }
    public LocalDateTime getDropoffDatetime() { return dropoffDatetime; }
    public int getPassengerCount() { return passengerCount; }
    public double getTripDistance() { return tripDistance; }
    public int getRatecodeID() { return ratecodeID; }
    public String getStoreAndFwdFlag() { return storeAndFwdFlag; }
    public int getPuLocationID() { return puLocationID; }
    public int getDoLocationID() { return doLocationID; }
    public int getPaymentType() { return paymentType; }
    public double getFareAmount() { return fareAmount; }
    public double getExtra() { return extra; }
    public double getMtaTax() { return mtaTax; }
    public double getTipAmount() { return tipAmount; }
    public double getTollsAmount() { return tollsAmount; }
    public double getImprovementSurcharge() { return improvementSurcharge; }
    public double getTotalAmount() { return totalAmount; }
    public double getCongestionSurcharge() { return congestionSurcharge; }
    public double getAirportFee() { return airportFee; }
    public long getTripDuration() { return tripDuration; }

    public long getPickupTimeMillis() {
        // Correct way to convert LocalDateTime to epoch milliseconds
        return pickupDatetime != null 
            ? pickupDatetime.toInstant(ZoneOffset.UTC).toEpochMilli()
            : 0;
    }

    public int getPickupHour() {
        return pickupDatetime != null ? pickupDatetime.getHour() : 0;
    }

    @Override
    public String toString() {
        return "TaxiTrip{" +
                "vendorID=" + vendorID +
                ", pickup=" + pickupDatetime +
                ", dropoff=" + dropoffDatetime +
                ", passengerCount=" + passengerCount +
                ", tripDistance=" + tripDistance +
                ", fareAmount=" + fareAmount +
                ", totalAmount=" + totalAmount +
                ", tripDuration=" + tripDuration +
                '}';
    }
}