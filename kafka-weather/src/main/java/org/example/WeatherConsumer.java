package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

/**
 * The {@code WeatherConsumer} class is a Kafka consumer that subscribes to a topic and logs received weather data.
 */
public class WeatherConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WeatherConsumer.class.getSimpleName());
    private static final String TOPIC_FORECAST = "weather-forecast";
    private static final String TOPIC_ALERTS = "weather-alerts";
    private static final String KAFKA_SERVER = "localhost:9092";
    private static final String GROUP_ID = "weather-consumer-group";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final double PRECIPITATION_THRESHOLD = 5.0;

    public static void main (String[] args) {
        Properties consumerProperties = getConsumerProperties();
        Properties producerProperties = getProducerProperties();

        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
             KafkaProducer<String, String> alertProducer = new KafkaProducer<>(producerProperties)) {

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

            kafkaConsumer.subscribe(List.of(TOPIC_FORECAST), new ConsumerRebalanceListener() {

                @Override
                public void onPartitionsRevoked (Collection<TopicPartition> partitions) {
                    LOGGER.info("Partitions revoked: {}", partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    LOGGER.info("Partitions assigned: {}", partitions);
                }
            });

            try {
                // Group temperatures by city and by date
                Map<String, Map<LocalDate, List<Double>>> tempPerCityPerDay = new HashMap<>();

                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));

                    if (records.isEmpty()) {
                        continue; // skip to next poll, no messages received
                    }

                    for (ConsumerRecord<String, String> record : records) {
                        LOGGER.info("Received weather data: {} ----> Partition: {} Offset: {} Timestamp: {}", record.value(), record.partition(), record.offset(), record.timestamp());

                        String city = record.key();
                        String json = record.value();

                        try{
                            JsonNode node = MAPPER.readTree(json);
                            String dateTimeString = node.get("time").asText();
                            double temperature = node.get("temperature").asDouble();
                            double precipitation =  node.get("precipitation").asDouble() ;

                            LocalDate date = LocalDateTime.parse(dateTimeString).toLocalDate();
                            tempPerCityPerDay
                                    .computeIfAbsent(city, c -> new TreeMap<>())
                                    .computeIfAbsent(date, d -> new ArrayList<>())
                                    .add(temperature);

                            // Check for alert condition
                            if (precipitation > PRECIPITATION_THRESHOLD) {
                                ObjectNode alert = MAPPER.createObjectNode();
                                alert.put("city", city);
                                alert.put("time", dateTimeString);
                                alert.put("precipitation", precipitation);
                                alert.put("alert", "Heavy precipitation forecasted");

                                String alertJson = MAPPER.writeValueAsString(alert);
                                ProducerRecord<String, String> alertRecord = new ProducerRecord<>(TOPIC_ALERTS, city, alertJson);
                                alertProducer.send(alertRecord, (metadata, exception) -> {
                                    if (exception == null) {
                                        LOGGER.info("Alert sent for {} at {}: {} mm ----> Partition: {} Offset: {} Timestamp: {}", city, dateTimeString, precipitation, metadata.partition(), metadata.offset(), metadata.timestamp());
                                    } else {
                                        LOGGER.error("Failed to send alert", exception);
                                    }
                                });
                            }
                        } catch (Exception e) {
                            LOGGER.error("Failed to parse JSON for record: {}", json, e);
                        }
                    }

                    calculateAverageTempPerCityPerDay(tempPerCityPerDay);

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

    private static Properties getConsumerProperties () {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    private static Properties getProducerProperties () {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    private static void calculateAverageTempPerCityPerDay(Map<String, Map<LocalDate, List<Double>>> tempPerCityPerDay){
        for (var entry : tempPerCityPerDay.entrySet()) {
            String city = entry.getKey();
            Map<LocalDate, List<Double>> dailyTemps = entry.getValue();

            for (var dateEntry : dailyTemps.entrySet()) {
                LocalDate date = dateEntry.getKey();
                List<Double> temps = dateEntry.getValue();

                double avg = temps.stream()
                        .mapToDouble(Double::doubleValue)
                        .average()
                        .orElse(Double.NaN);

                LOGGER.info("City: {} | Date: {} | Records processed: {} | Average Temperature: {}°C",
                        city, date, temps.size(), String.format("%.2f", avg));
            }
        }
    }
}