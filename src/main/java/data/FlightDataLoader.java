package data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.groupingBy;

public class FlightDataLoader {
    private static final Pattern AIRPORTS = Pattern.compile("\"(SLC|SEA|EWR|MCO|BOS|CLT|LAS|PHX|SFO|IAH|LAX|DEN|DFW|ORD|ATL)\"");
    private static final Logger log = LoggerFactory.getLogger(FlightDataLoader.class);

    private static final int INDEX = 4;

    private static Function<Integer, Predicate<String>> matcher = index -> line -> AIRPORTS.matcher(line.split(",")[index]).matches();
    private static BiFunction<Integer, String, String> getFieldAt = (index, line) -> line.split(",")[index];
    private static Function<String, String> cleanQuotes = line -> line.replaceAll("\"", "");

    private FlightDataLoader() {}

    public static void main (String[] args) throws Exception {
        log.info("Getting flights...");

        Map<String, List<String>> trainingData = getFlightDataByAirport("");

        for (String key : trainingData.keySet()) {
            log.info("{} number flights {}", key, trainingData.get(key).size());
        }
    }

    public static Map<String, List<String>> getFlightDataByAirport(String path) throws IOException {
        return loadFilteredByAirportRegex(INDEX, new File(path));
    }

    private static Map<String, List<String>> loadFilteredByAirportRegex (int index, File file) throws IOException {
        return Files.readAllLines(file.toPath()).stream()
                .filter(matcher.apply(index))
                .map(cleanQuotes)
                .collect(groupingBy(line -> getFieldAt.apply(index, line)));
    }
}
