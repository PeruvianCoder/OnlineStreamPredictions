package models;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.vectorizer.encoders.ConstantValueEncoder;
import org.apache.mahout.vectorizer.encoders.ContinuousValueEncoder;
import org.apache.mahout.vectorizer.encoders.FeatureVectorEncoder;
import org.apache.mahout.vectorizer.encoders.StaticWordValueEncoder;

public class FlightData {

    public enum Fields {
        MONTH, DAY_OF_WEEK, UNIQUE_CARRIER, FL_NUM, ORIGIN, DEST, CRS_DEP_TIME, DEP_DELAY, TAXI_OUT, ARR_DELAY_NEW, DISTANCE;
    }

    public static final int FEATURES = 6;

    public final RandomAccessSparseVector vector = new RandomAccessSparseVector(FEATURES);
    public final int realResult;

    private final ConstantValueEncoder bias = new ConstantValueEncoder("bias");
    private final FeatureVectorEncoder categoryValueEncoder = new StaticWordValueEncoder("categories");
    private final ContinuousValueEncoder numericEncoder = new ContinuousValueEncoder("numbers");

    public FlightData (String data) {
        String[] parts = data.split(",");

        int arrivalDelayIndex = parts.length == Fields.values().length ? Fields.ARR_DELAY_NEW.ordinal() : Fields.ARR_DELAY_NEW.ordinal() - 2;
        int distanceIndex = parts.length == Fields.values().length ? Fields.DISTANCE.ordinal() : Fields.DISTANCE.ordinal() - 2;

        String late = parts[arrivalDelayIndex];
        late = late.isEmpty() ? "0.0" : late;
        realResult = Double.parseDouble(late) == 0.0 ? 1 : 0;
        bias.addToVector("1", vector);


        for (Fields field : Fields.values()) {
            switch (field) {
                case DAY_OF_WEEK:
                case UNIQUE_CARRIER:
                case ORIGIN:
                case DEST:
                    categoryValueEncoder.addToVector(parts[field.ordinal()], vector);
                    break;
                case DISTANCE:
                    Double distance = Double.parseDouble(parts[distanceIndex]) / 100000;
                    numericEncoder.addToVector(distance.toString(), vector);
                    break;
                case TAXI_OUT:
                case DEP_DELAY:
                    if (parts.length == Fields.values().length) {
                        String strField = parts[field.ordinal()];
                        if (strField.isEmpty()) {
                            strField = "0.0";
                        }
                        numericEncoder.addToVector(strField, vector);
                    }
                    break;
                default:
            }
        }
    }
}
