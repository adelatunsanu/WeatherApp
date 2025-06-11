package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
                for (Location location:locations) {
                    String locationName = location.name();
                    List<String> forecastList;
                    try {
                        forecastList = OpenMeteoClient.getHourlyWeatherData(location);
                    } catch (IOException e) {
                        LOGGER.error("Failed to fetch weather data for location: {}", locationName, e);
                        continue;
                    }

                    for (String jsonData : forecastList) {
                        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, locationName, jsonData);

                        kafkaProducer.send(record, (recordMetadata, exception) -> {
                            if (exception == null){
                                LOGGER.info("Sent weather data: {} ----> Partition: {} Offset: {} Timestamp: {}", jsonData, recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                            } else{
                                LOGGER.error("Failed to send weather data for location: {}", locationName, exception);
                            }
                        });
                    }
                }
            };

            executorService.scheduleAtFixedRate(task, 0, 1, TimeUnit.MINUTES);

            new CountDownLatch(1).await(); // Keep the main thread alive
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("Weather producer was interrupted", e);
        }
    }

    private static List<Location> getLocations(){
        return List.of(
                new Location("Thassos - Skala Rachoniou", 40.7794, 24.6124),
                new Location("Athens", 37.9838, 23.7275),
                new Location("Santorini", 36.393154, 25.461510),
                new Location("Thessaloniki", 40.6401, 22.9444)
        );
    }

    private static Properties getProperties () {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}