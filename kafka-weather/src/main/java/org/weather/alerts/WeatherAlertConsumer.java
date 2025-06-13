package org.weather.alerts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class WeatherAlertConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WeatherAlertConsumer.class.getSimpleName());
    private static final String TOPIC = "weather-alerts";
    private static final String KAFKA_SERVER = "localhost:9092";
    private static final String GROUP_ID = "weather-alerts-consumer-group";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Dotenv DOT_ENV = Dotenv.load(); // Auto-loads from project root
    private static final String PUSHOVER_USER_KEY = DOT_ENV.get("PUSHOVER_USER");
    private static final String PUSHOVER_APP_TOKEN = DOT_ENV.get("PUSHOVER_TOKEN");

    public static void main (String[] args) {
        Properties properties = getProperties();

        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)){
            final Thread mainThread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Shutdown detected. Triggering Kafka consumer wakeup...");

                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    LOGGER.warn("Shutdown hook interrupted", exception);
                }
            }));

            consumer.subscribe(List.of(TOPIC));
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMinutes(1));
                    if (records.isEmpty()) {
                        continue;
                    }

                    for (ConsumerRecord<String, String> record : records) {
                        LOGGER.info("Received alert data: {} ----> Partition: {} Offset: {} Timestamp: {}", record.value(), record.partition(), record.offset(), record.timestamp());

                        try {
                            JsonNode alert = MAPPER.readTree(record.value());

                            String city = alert.get("city").asText();
                            String alertMsg = alert.get("alert").asText();
                            double precipitation = alert.get("precipitation").asDouble();

                            sendNotification(city, alertMsg, precipitation);

                        } catch (Exception e) {
                            LOGGER.error("Failed to process alert: {}", record.value(), e);
                        }
                    }
                }
            } catch (WakeupException exception) {
                LOGGER.info("Kafka consumer wakeup triggered. Shutting down gracefully...");
            } finally {
                LOGGER.info("Kafka consumer has been closed.");
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

    private static void sendNotification(String city, String alertMsg, double precipitation) {
        try{
            String message = String.format("🌧️ Alert for %s: %s (%.2f mm)", city, alertMsg, precipitation);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://api.pushover.net/1/messages.json"))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(
                            "token=" + PUSHOVER_APP_TOKEN +
                                    "&user=" + PUSHOVER_USER_KEY +
                                    "&message=" + URLEncoder.encode(message, StandardCharsets.UTF_8)))
                    .build();

            HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.discarding());

            LOGGER.info("📲 Pushover notification sent: {}", message);
        } catch (Exception e) {
            LOGGER.error("Failed to send Pushover notification", e);
        }
    }
}
