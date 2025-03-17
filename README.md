## ie-mda-II-mar25-group10

# Cycling in numbers - A Case Study of Cycle Paths in Rhine-Kreis Neuss


Five Counting stations have been permanently documenting cycling traffic on central roads since 2016 in Cycle paths in the Rhine-Kreis Neuss. The daily measurement of cycling traffic is done with the help of induction loops laid in the ground. With the permanent collection of data can gain insights on the daily, weekly and annual cycles and on it building long-term cycling developments over several years.

More details on the data source here: https://data.europa.eu/data/datasets/eco-counter-data-rhein-kreis-neuss?locale=en


## Project Overview
This project analyzes bicycle traffic data in the Rhine-Kreis Neuss region, Germany, leveraging real-time streaming, batch processing, and predictive analytics. By using big data technologies such as Apache Kafka, Apache Spark, and Hadoop, the system ingests, processes, and visualizes cycling trends to support urban planning and bike-sharing initiatives.

## Tech Stack
- **Apache Kafka**: Streams real-time sensor data for processing.
- **Apache Spark (PySpark)**: Performs both real-time streaming analytics and batch processing.
- **Hadoop HDFS**: Stores processed data and machine learning models.

## Workflow
1. **Data Ingestion**: Sensor data is collected 
2. **Real-Time Streaming**: Apache Kafka stores and streams the cycling data for real-time processing.
3. **Batch Processing & ML Models**: Apache Spark processes the data to generate insights and forecasts.
4. **Storage**: Data is stored in Hadoop HDFS for historical analysis.

## Notebooks Overview
1. **Data Transformation** (`1_data_transformation_cycling_germany.ipynb`):
   - Cleans and normalizes raw cycling data.
   - Extracts key time and location-based attributes.
   - Stores structured data in HDFS.
2. **Exploratory Data Analysis (EDA)** (`2_analysis_cycling_germany.ipynb`):
   - Identifies trends in cycling patterns.
   - Compares traffic across different stations and times.
   - Prepares data for forecasting.
3. **Streaming Simulation** (`3_cycling_streaming_simulation.ipynb`):
   - Simulates real-time data ingestion using Apache Kafka.
   - Validates the integrity of streaming messages.
   - Logs and monitors real-time ingestion.
4. **Batch Processing & Analytics** (`4_cycling_stream_batching.ipynb`):
   - Processes streaming data using Spark Structured Streaming.
   - Aggregates and analyzes traffic trends over time.
   - Stores results for further visualization and ML modeling.
