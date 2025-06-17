package org.weather.messaging.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weather.model.Location;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The {@code WeatherProducer} class is a Kafka producer that fetches hourly weather data
 * from the Open-Meteo API for a predefined list of locations and sends the data to a Kafka topic.
 * <p>
 * The producer is scheduled to run every minute, pulling fresh data and publishing it to the
 * topic.
 */
public class WeatherProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WeatherProducer.class.getSimpleName());
    private static final String TOPIC = "weather-forecast";
    private static final String KAFKA_SERVER = "localhost:9092";

    public static void main (String[] args) {
        Properties properties = getProperties();

        try (ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
             KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {

            Runnable task = ()->{
                LOGGER.info("Weather producer running. Fetching every 1 minute...");
                List<Location> locations = getLocations();

                WeatherDataPublisher dataPublisher = new WeatherDataPublisher(kafkaProducer, locations, TOPIC );
                dataPublisher.publishWeatherData();
            };

            executorService.scheduleAtFixedRate(task, 0, 1, TimeUnit.MINUTES);

            new CountDownLatch(1).await(); // Keep the main thread alive
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("Weather producer was interrupted", e);
        }
    }

    private static List<Location> getLocations() {
        ObjectMapper mapper = new ObjectMapper();
        try (InputStream inputStream = WeatherProducer.class.getClassLoader().getResourceAsStream("locations.json")) {
            if (inputStream == null) {
                throw new IllegalStateException("Could not find locations.json");
            }
            return List.of(mapper.readValue(inputStream, Location[].class));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load locations from file", e);
        }
    }

    private static Properties getProperties () {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}