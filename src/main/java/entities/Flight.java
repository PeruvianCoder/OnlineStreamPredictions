package entities;

import models.FlightData;

public class Flight {
    private final String month;
    private final String day;
    private final String flightNumber;
    private final String time;
    private final String airLine;
    private final String actualDelay;
    private final String airport;


    public Flight(String data) {
        String[] parts = data.split(",");
        int arrivalDelayIndex = parts.length == FlightData.Fields.values().length ? FlightData.Fields.ARR_DELAY_NEW.ordinal() : FlightData.Fields.ARR_DELAY_NEW.ordinal() - 2;
        this.month = parts[FlightData.Fields.MONTH.ordinal()];
        this.day = parts[FlightData.Fields.DAY_OF_WEEK.ordinal()];
        this.flightNumber = parts[FlightData.Fields.FL_NUM.ordinal()];
        this.time = parts[FlightData.Fields.CRS_DEP_TIME.ordinal()];
        this.airLine = parts[FlightData.Fields.UNIQUE_CARRIER.ordinal()];
        this.airport = parts[FlightData.Fields.ORIGIN.ordinal()];
        this.actualDelay = parts[arrivalDelayIndex];
    }

    @Override
    public String toString() {
        return "Flight{" +
                "month='" + month + '\'' +
                ", day='" + day + '\'' +
                ", flightNumber='" + flightNumber + '\'' +
                ", time='" + time + '\'' +
                ", airLine='" + airLine + '\'' +
                ", actualDelay='" + actualDelay + '\'' +
                ", airport='" + airport + '\'' +
                '}';
    }

}