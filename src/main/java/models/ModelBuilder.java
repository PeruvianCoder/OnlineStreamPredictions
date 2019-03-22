package models;

import org.apache.mahout.classifier.evaluation.Auc;
import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.*;

public class ModelBuilder {

    private static final int NUM_EPOCS = 20;
    private static final int MAX_RECORDS = 500;
    private static final double TRAINING_PERCENTAGE = .20;
    private static final Logger log = LoggerFactory.getLogger(ModelBuilder.class);

    private ModelBuilder() {}

    public static void main (String[] args) throws IOException {
        log.info("Training models now...");

    }

    public static Map<String, OnlineLogisticRegression> train (String path) throws IOException {
//        final Map<String,OnlineLogisticRegression> data = DataLoader.getFlightDataByAirport(path);
        final Map<String,OnlineLogisticRegression> data = new HashMap<String, OnlineLogisticRegression>();
        final Map<String, OnlineLogisticRegression> regressionMap = new HashMap<>();


        return regressionMap;
    }

    private static Map<String, List<String>> getRandomSampling (Map<String, List<String>> data) {
        Map<String, List<String>> sample = new HashMap<>();
        SecureRandom random = new SecureRandom();

        for (Map.Entry<String, List<String>> entry : data.entrySet()) {
            int total = entry.getValue().size() > MAX_RECORDS ? MAX_RECORDS : entry.getValue().size();
            String key = entry.getKey();
            List<String> currList = entry.getValue();
            Collections.shuffle(currList);
            Set<String> set = new HashSet<>();
            while (set.size() < total) {
                set.add(currList.get(random.nextInt(currList.size())));
            }
            sample.put(key, new ArrayList<>(set));
        }

        return sample;
    }

    public static byte[] train (List<String> input) throws IOException {
        List<FlightData> data = new ArrayList<>();
        for (String curr : input) {
            data.add(new FlightData(curr));
        }

        return getBytesRegression(onlineRegression(data));
    }

    private static OnlineLogisticRegression onlineRegression(List<FlightData> data) {
        OnlineLogisticRegression logisticRegression = new OnlineLogisticRegression(2, FlightData.FEATURES, new L1());

        for (int i = 0; i < NUM_EPOCS; i++) {
            for (FlightData currData : data) {
                logisticRegression.train(currData.realResult, currData.vector);
            }
        }

        return logisticRegression;
    }

    private static byte[] getBytesRegression(OnlineLogisticRegression logisticRegression) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(baos);
        logisticRegression.write(dataOutputStream);
        return baos.toByteArray();
    }

    private static void testTrainedRegression(OnlineLogisticRegression onlineLogisticRegression, String key, List<String> testFights) {
        Auc eval = new Auc(0.5);

        for (String testFight : testFights) {
            FlightData flightData = new FlightData(testFight);
            eval.add(flightData.realResult, onlineLogisticRegression.classifyScalar(flightData.vector));
        }

        log.info("Training accuracy for {} {}", key, eval.auc());
    }

    public static Map<String, byte[]> buildModel (String path) throws IOException {
        Map<String, OnlineLogisticRegression> model = train(path);
        Map<String, byte[]> coefficientMap = new HashMap<>();

        for (Map.Entry<String, OnlineLogisticRegression> entry : model.entrySet()) {
            coefficientMap.put(entry.getKey(), getBytesRegression(entry.getValue()));
        }

        return coefficientMap;
    }
}
