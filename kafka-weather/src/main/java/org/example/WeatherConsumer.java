package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;


public class WeatherConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WeatherConsumer.class.getSimpleName());
    private static final String TOPIC = "weather-forecast";
    private static final String KAFKA_SERVER = "localhost:9092";
    private static final String GROUP_ID = "weather-consumer-group";

    public static void main (String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {
            final Thread mainThread = Thread.currentThread();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Shutdown detected. Triggering Kafka consumer wakeup...");

                kafkaConsumer.wakeup(); // triggers WakeupException in the consumer.poll()

                try {
                    mainThread.join(); // Wait for main thread to finish
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    LOGGER.warn("Shutdown hook interrupted", exception);
                }
            }));

            kafkaConsumer.subscribe(List.of(TOPIC));

            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
                    for (ConsumerRecord<String, String> record : records) {
                        LOGGER.info("Received weather data: {} ----> Partition: {} Offset: {} Timestamp: {}", record.value(), record.partition(), record.offset(), record.timestamp());
                        // TODO - process data
                    }
                }
            } catch (WakeupException exception) {
                LOGGER.info("Kafka consumer wakeup triggered. Shutting down gracefully...");
            } catch (Exception exception) {
                LOGGER.error("Unexpected exception Kafka consumer.", exception);
            } finally {
                LOGGER.info("Kafka consumer has been closed.");
            }
        }
    }
}