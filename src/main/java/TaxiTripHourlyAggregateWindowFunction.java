import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class TaxiTripHourlyAggregateWindowFunction 
        implements WindowFunction<TaxiTrip, HourlyAggregate, Integer, TimeWindow> {

    @Override
    public void apply(Integer hour, 
                      TimeWindow window, 
                      Iterable<TaxiTrip> input, 
                      Collector<HourlyAggregate> out) {
        
        List<Double> fares = new ArrayList<>();
        int count = 0;
        
        for (TaxiTrip trip : input) {
            fares.add(trip.getFareAmount());
            count++;
        }
        
        // Calculate average fare
        double avgFare = 0.0;
        if (count > 0) {
            double total = 0.0;
            for (Double fare : fares) {
                total += fare;
            }
            avgFare = total / count;
        }
        
        System.out.println("Created hourly aggregate for hour " + hour + 
                ", window: " + new java.sql.Timestamp(window.getStart()) + 
                " to " + new java.sql.Timestamp(window.getEnd()) + 
                ", count: " + count + 
                ", avg fare: " + avgFare);
        
        out.collect(new HourlyAggregate(
            hour,
            window.getStart(),
            window.getEnd(),
            avgFare,
            count
        ));
    }
}