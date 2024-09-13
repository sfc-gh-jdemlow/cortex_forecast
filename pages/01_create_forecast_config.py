import streamlit as st
import yaml
from cortex_forecast.forecast import SnowflakeMLForecast

def display_state_sidebar():
    st.sidebar.title("Current Selections")
    if 'selected_database' in st.session_state:
        st.sidebar.text_input("Database", value=st.session_state.selected_database, key="sidebar_database_display")
    if 'selected_schema' in st.session_state:
        st.sidebar.text_input("Schema", value=st.session_state.selected_schema, key="sidebar_schema_display")
    if 'selected_table_view' in st.session_state:
        st.sidebar.text_input("Table/View", value=st.session_state.selected_table_view, key="sidebar_table_view_display")
    if 'timestamp_column' in st.session_state:
        st.sidebar.text_input("Timestamp Column", value=st.session_state.timestamp_column, key="sidebar_timestamp_display")
    if 'target_column' in st.session_state:
        st.sidebar.text_input("Target Column", value=st.session_state.target_column, key="sidebar_target_display")
    if 'series_column' in st.session_state:
        st.sidebar.text_input("Series Column", value=st.session_state.series_column, key="sidebar_series_display")
    if 'exogenous_columns' in st.session_state:
        st.sidebar.text_area("Exogenous Columns", value=", ".join(st.session_state.exogenous_columns), key="sidebar_exogenous_display")
    
    if st.sidebar.button("Update Selections", key="sidebar_update_button"):
        st.session_state.selected_database = st.session_state.sidebar_database_display
        st.session_state.selected_schema = st.session_state.sidebar_schema_display
        st.session_state.selected_table_view = st.session_state.sidebar_table_view_display
        st.session_state.timestamp_column = st.session_state.sidebar_timestamp_display
        st.session_state.target_column = st.session_state.sidebar_target_display
        st.session_state.series_column = st.session_state.sidebar_series_display
        st.session_state.exogenous_columns = [col.strip() for col in st.session_state.sidebar_exogenous_display.split(",")]
        st.rerun()


def set_example_defaults():
    st.session_state.model_name = "tasty_byte_lobster_forecast"
    st.session_state.team_name = "data_science"
    st.session_state.model_comment = "Forecast model for Lobster Mac & Cheese sales"
    st.session_state.training_days = 365  # One year of training data
    st.session_state.forecast_days = 30  # Forecast for the next month
    st.session_state.evaluate = True
    st.session_state.n_splits = 2
    st.session_state.gap = 0  # One week gap
    st.session_state.prediction_interval = 0.95
    st.session_state.output_table = "LOBSTER_SALES_FORECAST"

st.title("Forecast Configuration")

display_state_sidebar()

if 'selected_table_view' not in st.session_state:
    st.warning("Please select a table or view on the previous page before configuring the forecast.")
else:
    st.subheader("Forecast Configuration")

    # Check if example data was loaded
    if 'example_data_loaded' in st.session_state and st.session_state.example_data_loaded:
        set_example_defaults()
        st.info("Example data detected. Configuration fields have been pre-filled with suggested values.")

    # Initialize the step if it doesn't exist
    if 'config_step' not in st.session_state:
        st.session_state.config_step = 0

    def create_forecast_config():
        config = {}

        if st.session_state.config_step >= 0:
            config['model'] = {
                'name': st.text_input("Model Name", value=st.session_state.get('model_name', 'my_forecast_model'), key="model_name_input")
            }
            st.session_state.model_name = config['model']['name']

            if st.session_state.config_step == 0 and st.button("Next", key="next_step_0"):
                st.session_state.config_step = 1
                st.rerun()

        if st.session_state.config_step >= 1:
            config['model']['tags'] = {
                'environment': st.selectbox("Environment", ["development", "production", "testing"], index=0, key="environment_select"),
                'team': st.text_input("Team Name", value=st.session_state.get('team_name', 'data_science'), key="team_name_input")
            }
            st.session_state.team_name = config['model']['tags']['team']

            if st.session_state.config_step == 1 and st.button("Next", key="next_step_1"):
                st.session_state.config_step = 2
                st.rerun()

        if st.session_state.config_step >= 2:
            config['model']['comment'] = st.text_area("Model Comment", value=st.session_state.get('model_comment', 'Forecast model for predicting trends.'), key="model_comment_input")
            st.session_state.model_comment = config['model']['comment']

            if st.session_state.config_step == 2 and st.button("Next", key="next_step_2"):
                st.session_state.config_step = 3
                st.rerun()

        if st.session_state.config_step >= 3:
            config['forecast_config'] = {
                'training_days': st.number_input("Training Days", value=st.session_state.get('training_days', 30), min_value=1, key="training_days_input")
            }
            st.session_state.training_days = config['forecast_config']['training_days']

            if st.session_state.config_step == 3 and st.button("Next", key="next_step_3"):
                st.session_state.config_step = 4
                st.rerun()

        if st.session_state.config_step >= 4:
            forecast_input_type = st.radio("Forecast Mode", ["Use Table for Prediction", "Use Forecast Days"], key="forecast_mode_radio")
            if forecast_input_type == "Use Forecast Days":
                config['forecast_config']['forecast_days'] = st.number_input("Forecast Days", value=st.session_state.get('forecast_days', 30), min_value=1, key="forecast_days_input")
                config['forecast_config']['table'] = None
                st.session_state.forecast_days = config['forecast_config']['forecast_days']
            else:
                st.write("Please enter the name of the table that contains the predictions.")
                config['forecast_config']['table'] = st.text_input("Prediction Table", value=st.session_state.get('prediction_table', ''), key="prediction_table_input")
                config['forecast_config']['forecast_days'] = None
                st.session_state.prediction_table = config['forecast_config']['table']

            if st.session_state.config_step == 4 and st.button("Next", key="next_step_5"):
                st.session_state.config_step = 5
                st.rerun()

        if st.session_state.config_step >= 5:
            config['forecast_config']['config_object'] = {
                'on_error': st.selectbox("On Error", ["skip", "fail"], index=0, key="on_error_select"),
                'evaluate': st.checkbox("Evaluate", value=st.session_state.get('evaluate', True), key="evaluate_checkbox"),
                'evaluation_config': {
                    'n_splits': st.number_input("Number of Splits", value=st.session_state.get('n_splits', 2), min_value=1, key="n_splits_input"),
                    'gap': st.number_input("Gap", value=st.session_state.get('gap', 0), min_value=0, key="gap_input"),
                    'prediction_interval': st.slider("Prediction Interval", min_value=0.0, max_value=1.0, value=st.session_state.get('prediction_interval', 0.95), step=0.01, key="prediction_interval_slider")
                }
            }
            st.session_state.evaluate = config['forecast_config']['config_object']['evaluate']
            st.session_state.n_splits = config['forecast_config']['config_object']['evaluation_config']['n_splits']
            st.session_state.gap = config['forecast_config']['config_object']['evaluation_config']['gap']
            st.session_state.prediction_interval = config['forecast_config']['config_object']['evaluation_config']['prediction_interval']

            if st.session_state.config_step == 5 and st.button("Next", key="next_step_6"):
                st.session_state.config_step = 6
                st.rerun()

        if st.session_state.config_step >= 6:
            st.write('Please enter the name of the table where the forecast will be saved.')
            config['output'] = {
                'table': st.text_input("Output Table Name", value=st.session_state.get('output_table', ''), key="output_table_input")
            }
            st.session_state.output_table = config['output']['table']

            if st.button("Save Configuration"):
                # Add input_data configuration
                config['input_data'] = {
                    'database': st.session_state.selected_database,
                    'schema': st.session_state.selected_schema,
                    'table': st.session_state.selected_table_view,
                    'timestamp_column': st.session_state.timestamp_column,
                    'target_column': st.session_state.target_column,
                    'series_column': st.session_state.series_column,
                    'exogenous_columns': st.session_state.exogenous_columns
                }
                
                with open('forecast_config.yaml', 'w') as f:
                    yaml.dump(config, f)
                st.success("Forecast configuration saved. You can now proceed to the Model Execution page.")
                st.session_state.config_step = 7
                st.session_state.forecast_config = config

        return config

    forecast_config = create_forecast_config()

    st.sidebar.write(f"Current Step: {st.session_state.config_step + 1}/6")