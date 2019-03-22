package streams;

import models.ModelBuilder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.internals.MeteredKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;


public class AirlinePredictorProcessor extends AbstractProcessor<String, String> {

    private MeteredKeyValueStore<String, List<String>> flights;
    private static final Logger LOG = LoggerFactory.getLogger(AirlinePredictorProcessor.class);


    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        super.init(context);
        flights = (MeteredKeyValueStore) context().getStateStore("flights");
        Duration duration = Duration.ofMillis(10000L);
        context().schedule(duration, PunctuationType.STREAM_TIME, this::punctuate);
    }

    @Override
    public void process(String airportId, String flightData) {
        List<String> flightList = this.flights.get(airportId);
        if (flightList == null) {
            flightList = new ArrayList<>();
        }
        LOG.debug("Adding key {}", airportId);
        flightList.add(flightData);
        this.flights.put(airportId, flightList);
    }

    public void punctuate(long timestamp) {
        KeyValueIterator<String, List<String>> allFlights = flights.all();
        while (allFlights.hasNext()) {
            KeyValue<String, List<String>> kv = allFlights.next();

            List<String> flightList = kv.value;
            String airportCode = kv.key;
            LOG.debug("Found Key {}", airportCode);
            if(flightList.size() >= 100){
                try {
                    LOG.debug("sending flight list {}", flightList);
                    byte[] serializedRegression = ModelBuilder.train(flightList);
                    context().forward(airportCode, serializedRegression);
                    LOG.info("updating model for {}", airportCode);
                    flightList.clear();
                    flights.put(airportCode, flightList);
                }catch (Exception e) {
                    LOG.error("couldn't update online regression for {}",airportCode, e);
                }
            }
        }
        allFlights.close();
    }

}
