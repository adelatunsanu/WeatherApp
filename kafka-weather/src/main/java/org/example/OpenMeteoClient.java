package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.net.URI;

/**
 * A Kafka-compatible client for fetching weather forecast data from the Open-Meteo API.
 * <p>
 * This client uses the Open-Meteo public REST API to retrieve hourly weather data
 * (e.g., temperature, precipitation) based on geographic coordinates.
 */
public class OpenMeteoClient {

    /**
     * Fetches weather forecast data from the Open-Meteo API for a given geographic location.
     * <p>
     * This method sends an HTTP GET request to the Open-Meteo forecast endpoint with the specified
     * latitude and longitude. It retrieves hourly weather data such as temperature and precipitation.
     * The response is returned as a {@link JsonNode}, with timestamps
     * formatted in the {@code Europe/Athens} time zone.
     *
     * @param latitude  the geographic latitude.
     * @param longitude the geographic longitude.
     * @return a {@link JsonNode} representing the parsed JSON response from the Open-Meteo API
     * @throws IOException if an I/O error occurs during the HTTP request or while reading the response
     *
     * @see <a href="https://open-meteo.com/en/docs">Open-Meteo API Documentation</a>
     */
    private static JsonNode fetchJsonNode(double latitude, double longitude) throws IOException {
        String urlString = String.format(
                "https://api.open-meteo.com/v1/forecast?latitude=%.2f&longitude=%.2f&hourly=temperature_2m,precipitation&timezone=Europe%%2FAthens",
                latitude, longitude);

        URL url = URI.create(urlString).toURL();
        HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
        httpConnection.setRequestMethod("GET");

        ObjectMapper objectMapper = new ObjectMapper();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(httpConnection.getInputStream()))) {
            return objectMapper.readTree(reader);
        }
    }
}
