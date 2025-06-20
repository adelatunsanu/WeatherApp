# 🌦️ WeatherApp

A Java-based Kafka-powered weather monitoring application that:
- Fetches hourly weather forecasts using the OpenMeteo API
- Sends real-time data to Apache Kafka
- Consumes weather data and calculates daily average temperatures per city
- Detects extreme precipitation and sends alerts using Pushover notifications

## 🚀 Features

- ⏱️ Scheduled weather data fetch every minute
- ☁️ Kafka producer and consumer using the `weather-forecast` and `weather-alerts` topics
- 🌡️ Calculates daily average temperature per city
- 🌧️ Detects high precipitation and triggers alerts
- 📲 Sends notifications to your device using the Pushover API
- ✅ Configuration via `.env` file

## 🛠️ Tech Stack

- Java 21
- Apache Kafka 3.9.0 (running in WSL)
- MySQL 8.0 (database for storing data)
- [MySQL Connector/J](https://dev.mysql.com/downloads/connector/j/) (JDBC driver)
- Pushover API
- [Open-Meteo](https://open-meteo.com/)

## 🛰️ Start Apache Kafka in WSL
This project assumes Kafka is installed and running inside WSL.

1. Start Zookeeper:

   `zookeeper-server-start.sh ~/kafka_2.13-3.9.0/config/zookeeper.properties`
    
2. Start Kafka Broker:

   `kafka-server-start.sh ~/kafka_2.13-3.9.0/config/server.properties`

3. Create Kafka Topic (if not already created):

    `kafka-topics.sh --bootstrap-server localhost:9092 --topic weather-forecast --partitions 3 --replication-factor 1 --create`
  
    `kafka-topics.sh --bootstrap-server localhost:9092 --topic weather-alerts --partitions 2 --replication-factor 1 --create`

## 📲 Pushover Notifications Setup

#### ✅ Prerequisites
1. Pushover account, sign up at https://pushover.net
2. Create a Pushover Application

#### 🔐 Store Secrets in a .env File
Create a file named .env in the root of your project:

    your-project/
    ├── .env
    ├── src/
    ├── build.gradle

##### Contents of .env File:

```properties
PUSHOVER_TOKEN=your-app-token-here 
PUSHOVER_USER=your-user-key-here
```

## 🗄️ Database Configuration

This project uses a helper class called `DBConnector` to connect to a MySQL database.  
Connection settings are loaded from a `db.properties` file located in `src/main/resources`.

### 📁 Example `db.properties`

```properties
db.url=jdbc:mysql://localhost:3306/weather_app
db.username=root
db.password=yourpassword
```




