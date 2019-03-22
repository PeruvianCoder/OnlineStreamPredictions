package models;

import entities.DataRegression;
import entities.Flight;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

public final class Predictor {

    private static final Logger log = LoggerFactory.getLogger(Predictor.class);

    private Predictor(){}

    public static String predict(DataRegression dataRegression) {
        try (OnlineLogisticRegression logisticRegression = new OnlineLogisticRegression()) {
            FlightData flightData = new FlightData(dataRegression.data);
            logisticRegression.readFields(new DataInputStream(new ByteArrayInputStream(dataRegression.coefficients)));
            double prediction = logisticRegression.classifyScalar(flightData.vector);
            String arrivalPrediction = prediction > 0.5 ? "on-time" : "late";
            return String.format("%s predicted to be %s", new Flight(dataRegression.data), arrivalPrediction);
        } catch (Exception ex) {
            log.error("Problems with predicting " + dataRegression.data, ex);
            return null;
        }
    }
}
