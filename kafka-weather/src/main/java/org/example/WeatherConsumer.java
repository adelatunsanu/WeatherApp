package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class WeatherConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WeatherProducer.class.getSimpleName());
    private static final String TOPIC = "weather-forecast";
    private static final String KAFKA_SERVER = "localhost:9092";
    private static final String GROUP_ID = "weather-consumer-group";

    public static void main (String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        pro\

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(List.of(TOPIC));

        while(true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record: records){
                LOGGER.info("Received weather data for location {}: {} ----> Partition: {} Offset: {}", record.key(), record.value(), record.partition(), record.offset());
                // TODO - process data
            }
        }
    }
}
