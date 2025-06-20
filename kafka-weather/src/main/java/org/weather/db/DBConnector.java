package org.weather.db;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Helper class to manage database connections using configuration from a properties file.
 * <p>
 * Reads the database connection details (URL, username, password) from a {@code db.properties}
 * file located in the classpath, and returns a {@link java.sql.Connection} using MySQL's
 * {@link com.mysql.cj.jdbc.MysqlDataSource}.
 */
public class DBConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBConnector.class.getSimpleName());

    /**
     * Loads database connection details from {@code db.properties} and returns a live connection.
     *
     * @return a {@link Connection} to the configured MySQL database, or {@code null} if the connection fails
     */
    public static Connection getConnection() {
        Connection connection = null;

        try (InputStream input = DBConnector.class.getClassLoader().getResourceAsStream("db.properties")) {
            if (input == null) {
                LOGGER.error("Unable to find db.properties");
                return null;
            }

            Properties property = new Properties();
            property.load(input);

            String url = property.getProperty("db.url");
            String username = property.getProperty("db.username");
            String password = property.getProperty("db.password");

            var dataSource = new MysqlDataSource();
            dataSource.setURL(url);
            connection = dataSource.getConnection(username, password);
            LOGGER.info("Connection to database established");
        } catch(SQLException | IOException e){
            LOGGER.error("Connection to database failed", e);
        }
        return connection;
    }
}
