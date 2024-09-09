import streamlit as st
import yaml
import os
from snowflake.snowpark.version import VERSION

from cortex_forecast.forecast import SnowflakeMLForecast
from cortex_forecast.connection import SnowparkConnection

def create_connection_config():
    st.header("Snowflake Connection Configuration")
    
    connection_config = {
        'account': st.text_input("Snowflake Account"),
        'user': st.text_input("Username"),
        'password': st.text_input("Password", type="password"),
        'role': st.text_input("Role", value="CORTEX_USER_ROLE"),
        'warehouse': st.text_input("Warehouse", value="CORTEX_WH"),
        'database': st.text_input("Database", value="CORTEX"),
        'schema': st.text_input("Schema", value="DEV")
    }
    
    return connection_config

def create_forecast_config():
    st.header("Forecast Configuration")
    
    config = {
        'model': {
            'name': st.text_input("Model Name", value="my_forecast_model"),
            'tags': {
                'environment': st.selectbox("Environment", ["development", "production", "testing"], index=0),
                'team': st.text_input("Team Name", value="data_science")
            },
            'comment': st.text_area("Model Comment", value="Forecast model for predicting trends.")
        },
        'input_data': {
            'table': st.text_input("Input Table Name", value="storage_usage_train"),
            'table_type': st.selectbox("Table Type", ["table", "view"], index=0),
            'timestamp_column': st.text_input("Timestamp Column", value="usage_date"),
            'target_column': st.text_input("Target Column", value="storage_gb"),
            'series_column': st.text_input("Series Column (optional)"),
            'exogenous_columns': st.text_input("Exogenous Columns (comma-separated)")
        },
        'forecast_config': {
            'training_days': st.number_input("Training Days", value=180, min_value=1),
        }
    }

    # Add radio button to choose between 'forecast_days' and 'table'
    forecast_input_type = st.radio("Forecast Mode", ["Use Forecast Days", "Use Table for Prediction"])

    if forecast_input_type == "Use Forecast Days":
        config['forecast_config']['forecast_days'] = st.number_input("Forecast Days", value=30, min_value=1)
        config['forecast_config']['table'] = None
    else:
        config['forecast_config']['table'] = st.text_input("Prediction Table", value="ny_taxi_rides_h3_predict")
        config['forecast_config']['forecast_days'] = None  # No forecast days if table is used

    # Additional configuration options
    config['forecast_config']['config_object'] = {
        'on_error': st.selectbox("On Error", ["skip", "fail"], index=0),
        'evaluate': st.checkbox("Evaluate", value=True),
        'evaluation_config': {
            'n_splits': st.number_input("Number of Splits", value=2, min_value=1),
            'gap': st.number_input("Gap", value=0, min_value=0),
            'prediction_interval': st.slider("Prediction Interval", min_value=0.0, max_value=1.0, value=0.95, step=0.01)
        }
    }
    
    config['output'] = {
        'table': st.text_input("Output Table Name", value="storage_forecast_results")
    }
    
    return config


def main():
    st.title("Snowflake ML Forecast Configuration")
    
    # Try to create a SnowparkConnection
    try:
        connection = SnowparkConnection(
            connection_config={
                'user': os.getenv('SNOWFLAKE_USER'),
                'password': os.getenv('SNOWFLAKE_PASSWORD'),
                'account': os.getenv('SNOWFLAKE_ACCOUNT'),
                'database': 'CORTEX',
                'warehouse': 'CORTEX_WH',
                'schema': 'DEV',
                'role': 'CORTEX_USER_ROLE'
            }
        )
        session = connection.get_session()
        
        # Display connection information
        snowflake_environment = session.sql('SELECT current_user(), current_version()').collect()
        snowpark_version = VERSION
        
        st.write("Connection Established with the following parameters:")
        st.write(f"User: {snowflake_environment[0][0]}")
        st.write(f"Role: {session.get_current_role()}")
        st.write(f"Database: {session.get_current_database()}")
        st.write(f"Schema: {session.get_current_schema()}")
        st.write(f"Warehouse: {session.get_current_warehouse()}")
        st.write(f"Snowflake version: {snowflake_environment[0][1]}")
        st.write(f"Snowpark for Python version: {snowpark_version[0]}.{snowpark_version[1]}.{snowpark_version[2]}")
        
        connection_config = None
    except Exception as e:
        st.error(f"Failed to establish connection: {str(e)}")
        st.write("Please provide connection details:")
        connection_config = create_connection_config()
    
    forecast_config = create_forecast_config()
    
    if st.button("Generate Forecast"):
        # Convert exogenous_columns to a list before saving
        if forecast_config['input_data']['exogenous_columns']:
            forecast_config['input_data']['exogenous_columns'] = [col.strip() for col in forecast_config['input_data']['exogenous_columns'].split(',')]
        else:
            forecast_config['input_data']['exogenous_columns'] = []
        
        # Save the forecast config
        with open('forecast_config.yaml', 'w') as f:
            yaml.dump(forecast_config, f)
        
        try:
            # Create SnowflakeMLForecast instance
            forecast_model = SnowflakeMLForecast(
                config_file='forecast_config.yaml',
                connection_config=connection_config if connection_config else None,
                is_streamlit=True
            )
            
            # Create and run forecast
            forecast_data = forecast_model.create_and_run_forecast()
            
            st.success("Forecast generated successfully!")
            
            # Display the first few rows of the forecast data
            st.write("Forecast Data Preview:")
            st.dataframe(forecast_data.head())
            
            # Generate forecast and visualization
            forecast_model.generate_forecast_and_visualization()
            
            # Display the chart if it's available in the session state
            if 'chart' in st.session_state:
                st.altair_chart(st.session_state['chart'], use_container_width=True)
            
            # Display the full dataframe if it's available in the session state
            if 'df' in st.session_state:
                st.write("Full Forecast Data:")
                st.dataframe(st.session_state['df'])
        
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()