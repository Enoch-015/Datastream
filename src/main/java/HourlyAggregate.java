import java.io.Serializable;

public class HourlyAggregate implements Serializable {
    private int hour;
    private long windowStart;
    private long windowEnd;
    private double averageFare;
    private int tripCount;

    public HourlyAggregate() {}

    public HourlyAggregate(int hour, long windowStart, long windowEnd, double averageFare, int tripCount) {
        this.hour = hour;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.averageFare = averageFare;
        this.tripCount = tripCount;
    }

    // Getters
    public int getHour() { return hour; }
    public long getWindowStart() { return windowStart; }
    public long getWindowEnd() { return windowEnd; }
    public double getAverageFare() { return averageFare; }
    public int getTripCount() { return tripCount; }

    @Override
    public String toString() {
        return "HourlyAggregate{" +
                "hour=" + hour +
                ", windowStart=" + new java.sql.Timestamp(windowStart) +
                ", windowEnd=" + new java.sql.Timestamp(windowEnd) +
                ", averageFare=" + averageFare +
                ", tripCount=" + tripCount +
                '}';
    }
}