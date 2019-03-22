package streams;

import entities.DataRegression;
import models.Predictor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.JsonDeserializer;
import util.JsonSerializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class OnlinePredictions {

    private static final Logger LOG = LoggerFactory.getLogger(OnlinePredictions.class);

    public static void main(String[] args) throws Exception {
        final Serde<byte[]> byteArraySerde = Serdes.ByteArray();

        StreamsBuilder builder = new StreamsBuilder();

        // this stream reads in the raw airline data and does the updating of onlineRegression
        KStream<String, String> dataByAirportStream = builder.stream(
                "raw-airline-data",
                Consumed.with(
                Serdes.String(),
                Serdes.String()
            ));

//        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore("flights");
        GlobalKTable<String, byte[]> regressionsByAirPortTable = builder.globalTable(
                "ml-data-input",
                Materialized.<String, byte[], KeyValueStore<Bytes, byte[]>>as(
                        "flights")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(byteArraySerde)
                );

        // stream reads raw data joins with co-efficients then makes prediction
        dataByAirportStream.join(regressionsByAirPortTable,
                (k, v) -> k,
                DataRegression::new)
                .mapValues(Predictor::predict)
                .filter((k, v) -> v != null)
                .peek((k, v) -> System.out.println("Prediction " + v))
                .to("predictions");

        // this is our update stream that will continuously update our model
        // in reality this would be run in a separate process

        Serde<List<String>> listSerde = getListStringSerde();

        builder.build()
                .addSource("new-data-source","ml-data-input")
                .addProcessor("modelUpdater",
                        AirlinePredictorProcessor::new,
                        "new-data-source")
                .addSink("updates-sink",
                        "onlineRegression-by-airport",
                        Serdes.String().serializer(),
                        byteArraySerde.serializer(),
                        "modelUpdater");

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), getProperties());
        CountDownLatch doneSignal = new CountDownLatch(1);

        kafkaStreams.setUncaughtExceptionHandler((t, e) -> LOG.info("Error in thread " + t + " for " + e));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Kill signal received shutting down");
            doneSignal.countDown();
            kafkaStreams.close(Duration.ofSeconds(5));
            LOG.info("Closed, bye");
        }));

        kafkaStreams.cleanUp();
        try {
            kafkaStreams.start();
            LOG.info("Waiting for shutdown");
            doneSignal.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static Serde<List<String>> getListStringSerde() {
        Map<String, Object> serdeProps = new HashMap<>();
        final Serializer<List<String>> listJsonSerializer = new JsonSerializer<>();
        final Deserializer<List<String>> listDeserializer = new JsonDeserializer<>();
        serdeProps.put("class", List.class);
        listDeserializer.configure(serdeProps, false);
        return Serdes.serdeFrom(listJsonSerializer, listDeserializer);
    }

    private static Properties getProperties() {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-online-inferencing");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "streams-online-inferencing-clientID");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);

        return props;
    }
}
