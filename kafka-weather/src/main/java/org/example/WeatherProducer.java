package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        try (ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor()) {
            Runnable task = new Runnable() {
                @Override
                public void run () {
                    LOGGER.info("Weather producer running. Fetching every 1 minute...");
                    try {
                        ProducerRecord<String, String> record = null;
                        List<String> forecastList = OpenMeteoClient.getHourlyWeatherData(40.46, 24.36);
                        for (String jsonData : forecastList) {
                            record = new ProducerRecord<>(TOPIC, jsonData);
                            kafkaProducer.send(record);
                            LOGGER.info("Sent to Kafka: {}", jsonData);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            };


            executorService.scheduleAtFixedRate(task, 0, 1, TimeUnit.MINUTES);
        }
    }
}