package org.weather.messaging.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

/**
 * {@code WeatherDataConsumer} consumes weather forecast data from a Kafka topic,
 * calculates average temperatures per city per day and sends rain alerts if precipitation exceeds a threshold.
 */
public class WeatherDataConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WeatherDataConsumer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final double PRECIPITATION_THRESHOLD = 5.0;

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final KafkaProducer<String, String> alertProducer;
    private final String forecastTopic;
    private final String alertTopic;

    /**
     * Constructs a new {@code WeatherDataConsumer} instance.
     *
     * @param kafkaConsumer   the Kafka consumer to consume weather data messages
     * @param alertProducer   the Kafka producer to send weather alerts
     * @param forecastTopic   the Kafka topic from which weather data is consumed
     * @param alertTopic      the Kafka topic to which alerts are published
     */
    public WeatherDataConsumer(KafkaConsumer<String, String> kafkaConsumer, KafkaProducer<String, String> alertProducer, String forecastTopic, String alertTopic) {
        this.kafkaConsumer = kafkaConsumer;
        this.alertProducer = alertProducer;
        this.forecastTopic = forecastTopic;
        this.alertTopic = alertTopic;
    }

    public void consumeAndProcess() {
        kafkaConsumer.subscribe(List.of(forecastTopic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                LOGGER.info("Partitions revoked: {}", partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                LOGGER.info("Partitions assigned: {}", partitions);
            }
        });

        Map<String, Map<LocalDate, List<Double>>> tempPerCityPerDay = new HashMap<>();

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));

                if (records.isEmpty()) {
                    continue; // skip to next poll, no messages received
                }

                Map<String, Map<LocalDate, Double>> rainAlerts = new HashMap<>();

                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info("Received weather data: {} ----> Partition: {} Offset: {} Timestamp: {}",
                            record.value(), record.partition(), record.offset(), record.timestamp());

                    String city = record.key();
                    String json = record.value();

                    try {
                        JsonNode node = MAPPER.readTree(json);
                        String dateTimeString = node.get("time").asText();
                        double temperature = node.get("temperature").asDouble();
                        double precipitation = node.get("precipitation").asDouble();

                        LocalDate date = LocalDateTime.parse(dateTimeString).toLocalDate();

                        tempPerCityPerDay
                                .computeIfAbsent(city, c -> new TreeMap<>())
                                .computeIfAbsent(date, d -> new ArrayList<>())
                                .add(temperature);

                        // Flag this city for alert if needed
                        if (precipitation > PRECIPITATION_THRESHOLD) {
                            rainAlerts
                                    .computeIfAbsent(city, c -> new TreeMap<>())
                                    .put(date, precipitation);
                        }
                    } catch (Exception e) {
                        LOGGER.error("Failed to parse JSON for record: {}", json, e);
                    }
                }

                calculateAverageTempPerCityPerDay(tempPerCityPerDay);
                sendRainAlerts(rainAlerts);
            }
        } catch (WakeupException exception) {
            LOGGER.info("Kafka consumer wakeup triggered. Shutting down gracefully...");
        } catch (Exception exception) {
            LOGGER.error("Unexpected exception Kafka consumer.", exception);
        } finally {
            LOGGER.info("Kafka consumer has been closed.");
        }
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

    private void sendRainAlerts(Map<String, Map<LocalDate, Double>> rainAlerts) throws JsonProcessingException {
        for (Map.Entry<String, Map<LocalDate, Double>> cityEntry : rainAlerts.entrySet()) {
            String city = cityEntry.getKey();

            for (Map.Entry<LocalDate, Double> dateEntry : cityEntry.getValue().entrySet()) {
                LocalDate date = dateEntry.getKey();
                double precipitation = dateEntry.getValue();
                String alertJson = getRainAlert(city, date, precipitation);

                alertProducer.send(new ProducerRecord<>(this.alertTopic, city, alertJson), (metadata, exception) -> {
                    if (exception == null) {
                        LOGGER.info("Alert data sent: {} ----> Partition: {} Offset: {} Timestamp: {}", alertJson, metadata.partition(), metadata.offset(), metadata.timestamp());
                    } else {
                        LOGGER.error("Failed to send alert for {}", city, exception);
                    }
                });
            }
        }
    }

    private static String getRainAlert(String city, LocalDate date, double precipitation) throws JsonProcessingException {
        ObjectNode alert = MAPPER.createObjectNode();
        alert.put("city", city);
        alert.put("date", date.toString());
        alert.put("alert", "Heavy precipitation forecasted");
        alert.put("precipitation", precipitation);

        return MAPPER.writeValueAsString(alert);
    }
}
