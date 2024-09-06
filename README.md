# “Snowflake Cortex Forecasting”


<!-- WARNING: THIS FILE WAS AUTOGENERATED! DO NOT EDIT! -->

# Documentation

> Note: This is a POC and is not production ready. This is a repository
> for the Cortex ML Function Forecasting. There are many updates that
> can be made to make this better and more robust for a production ready
> use case, but this will get you 80-90% of the way there for simple
> forecating use cases. This was developed in hopes that it would get
> you started on your forecasting journey. Then allow you to make
> adjustments to fit your specific use case.

## Overview

The
[`SnowflakeMLForecast`](https://sfc-gh-jdemlow.github.io/cortex_forecast/cortex_forecast.html#snowflakemlforecast)
class is a flexible tool designed for creating, managing, and analyzing
forecast models within Snowflake using the
`CREATE SNOWFLAKE.ML.FORECAST` functionality. It allows users to define
models, configure inputs, generate forecasts, and visualize results
seamlessly.

## Features

- Dynamic Forecast Model Creation: Automatically generates SQL queries
  to create forecast models based on configuration files.
- Visualization: Integrates with both Streamlit and standard Python
  environments to display forecast results and key data aspects.
- Tag Management: Handles the creation of tags in Snowflake and ensures
  smooth operation even if tags already exist.
- Configurable: Supports YAML configuration files for easy setup and
  flexibility.
- Error Handling: Robust error handling and user feedback for a seamless
  experience.

## Installation

### Requirements

- Python 3.7+
- Snowflake Connector
- Pandas
- Altair (for visualizations)
- Streamlit (optional, for UI)

### Setup

1.  Clone the Repository:

2.  Install Dependencies:

3.  Set Up Configuration: Create a YAML file with your configuration
    settings (explained below).

## Configuration

The
[`SnowflakeMLForecast`](https://sfc-gh-jdemlow.github.io/cortex_forecast/cortex_forecast.html#snowflakemlforecast)
class relies on a YAML configuration file to define the input data,
forecast settings, and other options. Below is an example configuration:

> This configuration is what is used in the storage example to be able
> to forecast your snowflake storage usage.

``` yaml
model:
  name: my_forecast_model
  tags:
    environment: production
    team: data_science
  comment: "Forecast model for predicting trends."

input_data:
  table: storage_usage_train
  table_type: table  # Options: 'table', 'view'
  timestamp_column: usage_date
  target_column: storage_gb
  series_column: null  # Set to column name for multiple time series
  exogenous_columns: # Or [column1, column2] if thre are no columns it will use all columns in the view or table
    - column1
    - column2

forecast_config:
  training_days: 180
  forecast_days: 30
  config_object:
    on_error: skip
    evaluate: true
    evaluation_config:
      n_splits: 2 # Default is 2
      gap: 0 # Default is 0
      prediction_interval: 0.95

output:
  table: storage_forecast_results
```

## Usage

### Creating a SnowflakeMLForecast Instance

``` python
# Step 1: Create a Forecast Model Instance

# Define your connection configuration
connection_config = {
    'user': 'your_user',
    'password': 'your_password',
    'account': 'your_account',
    'database': 'your_database',
    'warehouse': 'your_warehouse',
    'schema': 'your_schema',
    'role': 'your_role'
}

# Create an instance of SnowflakeMLForecast
forecast_model = SnowflakeMLForecast(
    config_file='path/to/your/config.yaml',
    connection_config=connection_config
)

# Step 2: Run Forecast and Visualize Results
forecast_model.generate_forecast_and_visualization(forecasting_period=30, confidence_interval=0.95)

# Step 3: Clean Up
forecast_model.cleanup()
```

# Full Example

> See in Docs/ folder for two example of this in action. One is for
> storage and the other is for Taxi Pick up in NYC.
