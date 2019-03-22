package streams;

import data.FlightDataLoader;
import models.ModelBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class DataProducer {

    private static final Logger log = LoggerFactory.getLogger(DataProducer.class);

    public static void main(String[] args) throws IOException, InterruptedException{
        if (args.length > 0) {
            log.info("Running in populate GlobalKTable Mode");
            populateGlobalKTable();
        } else {
            log.info("Sending simulated for updating model");
            List<String> sampleFiles = Arrays.asList("incoming_data1.csv", "incoming_data1.csv", "incoming_data1.csv");
            for (String sampleFile : sampleFiles) {
                sendRawData(sampleFile);
                Thread.sleep(30000);
            }
        }
    }

    public static void sendRawData(String fileName) throws InterruptedException, IOException {
        log.info("Sending some raw data and new update data to run demo");
        Map<String, List<String>> dataToLoad = FlightDataLoader.getFlightDataByAirport("src/main/resources/" + fileName);
        Producer<String, String> producer = getDataProducer();
        int counter = 0;
        int maxPerAirport = 200;
        for (Map.Entry<String, List<String>> entry : dataToLoad.entrySet()) {
            String key = entry.getKey();
            for (String flight : entry.getValue()) {

                ProducerRecord<String, String> rawDataRecord = new ProducerRecord<>("raw-airline-data", key, flight);
                ProducerRecord<String, String> incomingMlData = new ProducerRecord<>("ml-data-input", key, flight);
                producer.send(rawDataRecord);
                producer.send(incomingMlData);
                counter++;

                if (counter > 0 && counter % 10 == 0) {
                    Thread.sleep(10000);
                }

                if(counter > 0 && counter % maxPerAirport == 0) {
                    break;
                }
            }
        }

        log.info("Sent {} number records to raw data feed, closing down", counter);
        producer.close();
    }

    public static void populateGlobalKTable() throws IOException {
        log.info("Building the model");

        Map<String, byte[]> model = ModelBuilder.buildModel("src/main/resources/allFlights.txt");
        Producer<String, byte[]> producer = getGlobalKTableProducer();

        for (Map.Entry<String, byte[]> entry : model.entrySet()) {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>("onlineRegression-by-airport", entry.getKey(), entry.getValue());
            producer.send(record);
        }

        log.info("Done publishing to topic, shutting down.");
        producer.close();
    }

    private static Producer<String, byte[]> getGlobalKTableProducer() {
        Properties props = getProps("org.apache.kafka.common.serialization.ByteArraySerializer");
        return new KafkaProducer<>(props);
    }

    private static Producer<String, String> getDataProducer() {
        Properties props = getProps("org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }

    private static Properties getProps(String valueSerializer) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", valueSerializer);
        return props;
    }
}