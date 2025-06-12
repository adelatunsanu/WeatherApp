package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class WeatherAlertConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WeatherAlertConsumer.class.getSimpleName());
    private static final String TOPIC = "weather-alerts";
    private static final String KAFKA_SERVER = "localhost:9092";
    private static final String GROUP_ID = "weather-alerts-consumer-group";

    public static void main (String[] args) {
        Properties properties = getProperties();

        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)){
            consumer.subscribe(List.of(TOPIC));

            while (true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMinutes(1));
                if (records.isEmpty()) {
                    continue;
                }

                for (ConsumerRecord<String, String> record:records){
                    LOGGER.info("Received alert data: {} ----> Partition: {} Offset: {} Timestamp: {}", record.value(), record.partition(), record.offset(), record.timestamp());
                }
            }
        }
    }

    private static Properties getProperties () {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }
}
