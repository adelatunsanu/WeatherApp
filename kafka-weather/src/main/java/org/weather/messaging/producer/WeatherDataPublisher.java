package org.weather.messaging.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weather.client.OpenMeteoClient;
import org.weather.model.Location;

import java.io.IOException;
import java.util.List;

/**
 * The {@code WeatherDataPublisher} class encapsulates the logic for fetching hourly weather data
 * from the Open-Meteo API and sending it to a Kafka topic.
 */
public class WeatherDataPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(WeatherDataPublisher.class);

    private final KafkaProducer<String, String> kafkaProducer;
    private final List<Location> locations;
    private final String topic;

    /**
     * Constructs a new {@code WeatherDataPublisher} instance.
     *
     * @param kafkaProducer   the Kafka producer to send weather data messages
     * @param locations       the list of locations for which weather data is fetched
     * @param topic           the Kafka topic to which data is published
     */
    public WeatherDataPublisher(KafkaProducer<String, String> kafkaProducer, List<Location> locations, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.locations = locations;
        this.topic = topic;
    }

    /**
     * Fetches hourly weather data for each configured location and sends the data to the configured Kafka topic.
     */
    public void publishWeatherData() {
        for (Location location : locations) {
            String locationName = location.name();
            List<String> forecastList;
            try {
                forecastList = OpenMeteoClient.getHourlyWeatherData(location);
            } catch (IOException e) {
                LOGGER.error("Failed to fetch weather data for location: {}", locationName, e);
                continue;
            }

            for (String jsonData : forecastList) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, locationName, jsonData);

                kafkaProducer.send(record, (recordMetadata, exception) -> {
                    if (exception == null) {
                        LOGGER.info("Sent weather data: {} ---> Partition: {} Offset: {} Timestamp: {}",
                                jsonData, recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                    } else {
                        LOGGER.error("Failed to send weather data for location: {}", locationName, exception);
                    }
                });
            }
        }
    }
}
