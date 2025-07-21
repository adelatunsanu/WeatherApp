package org.weather.messaging.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * The {@code WeatherConsumer} class is a Kafka consumer that subscribes to a topic and logs received weather data.
 */
public class WeatherConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WeatherConsumer.class.getSimpleName());
    private static final String FORECAST_TOPIC = "weather-forecast";
    private static final String ALERTS_TOPIC = "weather-alerts";
    private static final String KAFKA_SERVER = "localhost:9092";
    private static final String GROUP_ID = "weather-consumer-group";

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

            WeatherDataConsumer dataConsumer = new WeatherDataConsumer(kafkaConsumer, alertProducer, FORECAST_TOPIC, ALERTS_TOPIC);
            dataConsumer.consumeAndProcess();
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
}