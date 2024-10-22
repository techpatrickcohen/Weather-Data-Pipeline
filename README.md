
# Weather Data Pipeline

![weather data pipeline flowchart](https://patrick-cohen.com/wp-content/uploads/2024/10/weather-data-pipeline-flowchart.jpg)

Welcome to the **Weather Data Pipeline** project! This repository contains an automated, scalable weather data pipeline built with Apache Airflow, PostgreSQL, and cloud integration. The pipeline fetches and processes weather data from multiple cities, stores the data in a local database, and provides meaningful insights through a Streamlit dashboard.

## Features

- ⚙️**Automated Data Ingestion**: Weather data is fetched every 15 minutes from the Open-Meteo API and stored in a PostgreSQL database.
- ⌛**Historical Data Collection**: On the first run, the pipeline retrieves weather data for the past 5 days.
- ☁︎**Cloud Integration**: Synchronization of local PostgreSQL data to a remote AWS RDS instance for scalability.
- 📊**Real-Time Insights**: Visualize weather data trends, including temperature, humidity, and useful metrics on a user-friendly dashboard.
- 📱**Mobile Compatibility**: [App Repo](https://github.com/techpatrickcohen/Weather-Dashboard-App) React Native Android and iOS application companion app developed to provide useful insights on the go.

## Architecture

The pipeline consists of the following components:

1. **Apache Airflow DAGs**: For automating weather data ingestion and processing tasks.
2. **PostgreSQL Database**: Stores the weather data for each city with fields such as temperature, humidity, and wind speed.
3. **Streamlit Dashboard**: A web-based interface that visualizes key metrics and trends in the weather data.
4. **AWS RDS PostgreSQL Sync**: Scheduled tasks in Airflow sync local data to a cloud-based PostgreSQL instance.

## Technologies Used

- **Apache Airflow**: Orchestration tool used to manage ETL tasks.
- **PostgreSQL**: Used as the local and cloud database for storing weather data.
- **Open-Meteo API**: Provides the weather data for multiple cities.
- **Streamlit**: Used to build the web-based dashboard for data visualization.
- **AWS RDS**: Cloud database to scale data storage.
- **Docker**: Manages the entire environment in containers for portability and easy deployment.

## Installation

### Prerequisites

- **Docker**: Ensure Docker is installed and running on your machine.
- **Python 3.8+**
- **AWS CLI** (optional for cloud integration)

### Clone the Repository

```bash
git clone https://github.com/techpatrickcohen/Weather-Data-Pipeline.git
cd Weather-Data-Pipeline
```

### Step 1: Configure Environment Variables

Set up your environment variables for the project by creating a `.env` file:

```bash
_AIRFLOW_WWW_USER_USERNAME=""
_AIRFLOW_WWW_USER_PASSWORD=""
LOCAL_AIRFLOW_POSTGRESQL_USER=""
LOCAL_AIRFLOW_POSTGRESQL_PASS=""
AWS_RDS_CONN_DBNAME=""
AWS_RDS_CONN_USER=""
AWS_RDS_CONN_PASS=""
AWS_RDS_CONN_HOST=""
```

### Step 2: Start Docker Containers

```bash
docker-compose up
```

This command starts up the entire stack, including Airflow, PostgreSQL, and Streamlit for visualization.

### Step 3: Initialize Airflow DAGs

The `weather_data_pipeline` DAG is set up to run every 15 minutes and fetch weather data.  
The `sync_local_to_rds` DAG is set up to also run every 15 minutes to check if the remote AWS RDS database is missing data. If so, data from the local Postgres database is pushed to the cloud.

You can trigger them manually via the Airflow UI:

```bash
http://localhost:8080
```

### Step 4: Access the Dashboard

Visit `http://localhost:8501` to access the Streamlit weather data visualization dashboard.

## Usage

### Viewing Data Insights

The Streamlit dashboard provides visualizations of key metrics such as:

- **Current Weather**: Displays the latest temperature, humidity, and weather conditions for a specified city.
- **Trends**: Graphs showing weather trends over the past 24 hours or 10 days.

## Future Improvements

- **More Data Sources**: Implement data ingestion from multiple cities to handle more data.

## Contributing

We welcome contributions! If you'd like to contribute, please:

1. Fork the repository.
2. Create a feature branch.
3. Make your changes and ensure tests pass.
4. Submit a pull request for review.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.
