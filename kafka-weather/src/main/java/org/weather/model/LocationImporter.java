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
}
