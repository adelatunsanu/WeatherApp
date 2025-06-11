package org.example;

import org.apache.kafka.clients.producer.*;
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
 * The Kafka Producer.
 */
public class WeatherProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WeatherProducer.class.getSimpleName());
    private static final String TOPIC = "weather-forecast";
    private static final String KAFKA_SERVER = "localhost:9092";

    public static void main (String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
             KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {
            Runnable task = ()->{
                LOGGER.info("Weather producer running. Fetching every 1 minute...");
                try {
                    List<Location> locations = getLocations();
                    for (Location location:locations) {
                        List<String> forecastList = OpenMeteoClient.getHourlyWeatherData(location);
                        ProducerRecord<String, String> record;

                        for (String jsonData : forecastList) {
                            record = new ProducerRecord<>(TOPIC, location.name(), jsonData);
                            kafkaProducer.send(record, new Callback() {
                                @Override
                                public void onCompletion (RecordMetadata recordMetadata, Exception e) {
                                    // executes every time a record is successfully sent or when an exception is thrown
                                    if (e == null){
                                        LOGGER.info("Data: {} ----> Partition: {} Offset: {} Timestamp: {}", jsonData, recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                                    } else{
                                        LOGGER.error("Error while producing", e);
                                    }
                                }
                            });
                        }
                    }
                } catch (IOException e) {
                    LOGGER.error("Error while producing", e);
                }
            };

            executorService.scheduleAtFixedRate(task, 0, 1, TimeUnit.MINUTES);

            new CountDownLatch(1).await(); // Keep the main thread alive
        } catch (InterruptedException e) {
            e.printStackTrace();
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
}