package org.weather.model;

/**
 * A record representing a geographical location by name, latitude and longitude.
 * <p>
 * This record is used to identify cities or regions for which weather data should be fetched
 * from the Open-Meteo API.
 *
 * @param name      a human-readable name of the location (e.g. "Athens").
 * @param latitude  the geographic latitude.
 * @param longitude the geographic longitude.
 */
public record Location(String name, double latitude, double longitude){}
