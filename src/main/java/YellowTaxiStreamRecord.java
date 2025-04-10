import java.time.format.DateTimeFormatter;

public class YellowTaxiStreamRecord {
    public String vendorName;
    public String pickupDatetime;
    public String dropoffDatetime;
    public int passengerCount;
    public double tripDistance;
    public String rateCodeName;
    public String storeAndFwdFlag;
    public int puLocationID;
    public int doLocationID;
    public String paymentTypeName;
    public double fareAmount;
    public double extra;
    public double mtaTax;
    public double tipAmount;
    public double tollsAmount;
    public double improvementSurcharge;
    public double totalAmount;
    public double congestionSurcharge;
    public double airportFee;

    public static YellowTaxiStreamRecord fromTrip(TaxiTrip trip) {
        YellowTaxiStreamRecord r = new YellowTaxiStreamRecord();
        // Map vendor ID to vendor name using traditional switch-case
        switch (trip.getVendorID()) {
            case 1:
                r.vendorName = "Creative Mobile Technologies, LLC";
                break;
            case 2:
                r.vendorName = "Curb Mobility, LLC";
                break;
            case 6:
                r.vendorName = "Myle Technologies Inc";
                break;
            case 7:
                r.vendorName = "Helix";
                break;
            default:
                r.vendorName = "Unknown Vendor";
                break;
        }
        
        // Use a formatter to produce the required timestamp format (yyyy-MM-dd HH:mm:ss)
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        r.pickupDatetime = trip.getPickupDatetime().format(formatter);
        r.dropoffDatetime = trip.getDropoffDatetime().format(formatter);
        
        r.passengerCount = trip.getPassengerCount();
        r.tripDistance = trip.getTripDistance();
        
        // Map ratecode ID to rate code name
        switch (trip.getRatecodeID()) {
            case 1:
                r.rateCodeName = "Standard rate";
                break;
            case 2:
                r.rateCodeName = "JFK";
                break;
            case 3:
                r.rateCodeName = "Newark";
                break;
            case 4:
                r.rateCodeName = "Nassau or Westchester";
                break;
            case 5:
                r.rateCodeName = "Negotiated fare";
                break;
            case 6:
                r.rateCodeName = "Group ride";
                break;
            case 99:
                r.rateCodeName = "Null/unknown";
                break;
            default:
                r.rateCodeName = "Other";
                break;
        }
        
        r.storeAndFwdFlag = trip.getStoreAndFwdFlag();
        r.puLocationID = trip.getPuLocationID();
        r.doLocationID = trip.getDoLocationID();
        
        // Map payment type to payment type name
        switch (trip.getPaymentType()) {
            case 0:
                r.paymentTypeName = "Flex Fare trip";
                break;
            case 1:
                r.paymentTypeName = "Credit card";
                break;
            case 2:
                r.paymentTypeName = "Cash";
                break;
            case 3:
                r.paymentTypeName = "No charge";
                break;
            case 4:
                r.paymentTypeName = "Dispute";
                break;
            case 5:
                r.paymentTypeName = "Unknown";
                break;
            case 6:
                r.paymentTypeName = "Voided trip";
                break;
            default:
                r.paymentTypeName = "Other";
                break;
        }
        
        r.fareAmount = trip.getFareAmount();
        r.extra = trip.getExtra();
        r.mtaTax = trip.getMtaTax();
        r.tipAmount = trip.getTipAmount();
        r.tollsAmount = trip.getTollsAmount();
        r.improvementSurcharge = trip.getImprovementSurcharge();
        r.totalAmount = trip.getTotalAmount();
        r.congestionSurcharge = trip.getCongestionSurcharge();
        r.airportFee = trip.getAirportFee();
        
        return r;
    }
}
