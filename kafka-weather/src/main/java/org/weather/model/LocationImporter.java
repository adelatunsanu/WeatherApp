package org.weather.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weather.db.DBConnector;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to import location data from a JSON resource file and insert it into the database.
 * <p>
 * Reads a JSON file containing an array of location objects, deserializes them into {@link Location} records,
 * and inserts each location into the database table.
 * <p>Requires a working database connection via {@link DBConnector}.</p>
 */
public class LocationImporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocationImporter.class.getSimpleName());

    public static void main (String[] args) {
        insertDataInDB();
    }

    private static void insertDataInDB(){
        try (Connection connection = DBConnector.getConnection();
             InputStream input = LocationImporter.class.getClassLoader().getResourceAsStream("locations.json")) {

            if (input == null) {
                throw new FileNotFoundException("Could not find 'locations.json' in classpath.");
            }

            String createTableSql = """
                CREATE TABLE IF NOT EXISTS locations (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL UNIQUE,
                    latitude DOUBLE NOT NULL,
                    longitude DOUBLE NOT NULL
                )
                """;

            try (PreparedStatement createTableStmt = connection.prepareStatement(createTableSql)) {
                createTableStmt.execute();
                LOGGER.info("Checked or created 'locations' table.");
            }

            ObjectMapper mapper = new ObjectMapper();
            List<Location> locations = mapper.readValue(input, new TypeReference<>() {});

            String checkSql = "SELECT COUNT(*) FROM locations WHERE name = ?";
            String insertSql  = "INSERT INTO locations (name, latitude, longitude) VALUES (?, ?, ?)";

            try (PreparedStatement checkStmt = connection.prepareStatement(checkSql);
                 PreparedStatement insertStmt = connection.prepareStatement(insertSql)) {

                for (Location location : locations) {
                    if (location.name() == null) {
                        LOGGER.warn("Skipped location with null name.");
                        continue;
                    }

                    checkStmt.setString(1, location.name());
                    try (ResultSet rs = checkStmt.executeQuery()) {
                        rs.next();
                        if (rs.getInt(1) == 0) {
                            insertStmt.setString(1, location.name());
                            insertStmt.setDouble(2, location.latitude());
                            insertStmt.setDouble(3, location.longitude());
                            insertStmt.executeUpdate();
                            LOGGER.info("Inserted: {}", location.name());
                        } else {
                            LOGGER.info("Skipped (already exists): {}", location.name());
                        }
                    }
                }
            }

            LOGGER.info("Import complete.");
        } catch (SQLException e) {
            LOGGER.error("Failed to connect to the database or while inserting data.", e);
        } catch (IOException e) {
            LOGGER.error("Failed to insert data", e);
        }
    }

    /**
     * Retrieves all locations stored in the database.
     * @return a list of {@link Location} instances representing all locations in the database;
     *         never {@code null}, but possibly empty if no records exist or on error.
     */
    public static List<Location> getLocationsFromDB() {
        List<Location> locations = new ArrayList<>();

        String query = "SELECT name, latitude, longitude FROM locations";

        try (Connection connection = DBConnector.getConnection();
             PreparedStatement stmt = connection.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                String name = rs.getString("name");
                double latitude = rs.getDouble("latitude");
                double longitude = rs.getDouble("longitude");

                Location location = new Location(name, latitude, longitude);
                locations.add(location);
            }

            LOGGER.info("Retrieved {} locations from the database.", locations.size());
        } catch (SQLException e) {
            LOGGER.error("Error retrieving locations from the database", e);
        } catch (IOException e) {
            LOGGER.error("Failed to retrieve data", e);
        }
        return locations;
    }
}
