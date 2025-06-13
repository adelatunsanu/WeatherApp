# WeatherApp

## 📲 Pushover Notifications Setup
This project includes support for Pushover to send real-time weather alerts (e.g., heavy precipitation warnings) directly to your device.

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
PUSHOVER_TOKEN=your-app-token-here 
PUSHOVER_USER=your-user-key-here