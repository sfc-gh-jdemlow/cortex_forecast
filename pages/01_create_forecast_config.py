import streamlit as st
import yaml
from cortex_forecast.forecast import SnowflakeMLForecast

def display_state_sidebar():
    st.sidebar.title("Current Selections")
    if 'selected_database' in st.session_state:
        st.sidebar.text_input("Database", value=st.session_state.selected_database, key="sidebar_database")
    if 'selected_schema' in st.session_state:
        st.sidebar.text_input("Schema", value=st.session_state.selected_schema, key="sidebar_schema")
    if 'selected_table_view' in st.session_state:
        st.sidebar.text_input("Table/View", value=st.session_state.selected_table_view, key="sidebar_table_view")
    if 'timestamp_column' in st.session_state:
        st.sidebar.text_input("Timestamp Column", value=st.session_state.timestamp_column, key="sidebar_timestamp")
    if 'target_column' in st.session_state:
        st.sidebar.text_input("Target Column", value=st.session_state.target_column, key="sidebar_target")
    if 'series_column' in st.session_state:
        st.sidebar.text_input("Series Column", value=st.session_state.series_column, key="sidebar_series")
    if 'exogenous_columns' in st.session_state:
        st.sidebar.text_area("Exogenous Columns", value=", ".join(st.session_state.exogenous_columns), key="sidebar_exogenous")
    
    if st.sidebar.button("Update Selections"):
        st.session_state.selected_database = st.session_state.sidebar_database
        st.session_state.selected_schema = st.session_state.sidebar_schema
        st.session_state.selected_table_view = st.session_state.sidebar_table_view
        st.session_state.timestamp_column = st.session_state.sidebar_timestamp
        st.session_state.target_column = st.session_state.sidebar_target
        st.session_state.series_column = st.session_state.sidebar_series
        st.session_state.exogenous_columns = [col.strip() for col in st.session_state.sidebar_exogenous.split(",")]
        st.rerun()

st.title("Forecast Configuration")

display_state_sidebar()

if 'selected_table_view' not in st.session_state:
    st.warning("Please select a table or view on the previous page before configuring the forecast.")
else:
    
    st.subheader("Forecast Configuration")

    def create_forecast_config():
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
                'table': st.session_state.selected_table_view,
                'table_type': "view" if "view" in st.session_state.selected_table_view.lower() else "table",
                'timestamp_column': st.session_state.timestamp_column,
                'target_column': st.session_state.target_column,
                'series_column': st.session_state.get('series_column', ''),
                'exogenous_columns': st.session_state.exogenous_columns
            },
            'forecast_config': {
                'training_days': st.number_input("Training Days", value=180, min_value=1),
            }
        }

        forecast_input_type = st.radio("Forecast Mode", ["Use Forecast Days", "Use Table for Prediction"])

        if forecast_input_type == "Use Forecast Days":
            config['forecast_config']['forecast_days'] = st.number_input("Forecast Days", value=30, min_value=1)
            config['forecast_config']['table'] = None
        else:
            config['forecast_config']['table'] = st.text_input("Prediction Table", value="ny_taxi_rides_h3_predict")
            config['forecast_config']['forecast_days'] = None

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
            'table': st.text_input("Output Table Name", value=f"{st.session_state.selected_table_view}_FORECAST_RESULTS")
        }
        
        return config

    forecast_config = create_forecast_config()

    if st.button("Save Configuration"):
        # Save the forecast config to session state
        st.session_state.forecast_config = forecast_config
        
        # Save the forecast config to a file (optional)
        with open('forecast_config.yaml', 'w') as f:
            yaml.dump(forecast_config, f)
        
        st.success("Forecast configuration saved. You can now proceed to the Model Execution page.")
        st.markdown("**[Click here to go to the Model Execution page](/Model_Execution)**")