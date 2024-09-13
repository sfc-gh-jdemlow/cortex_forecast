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

st.title("Model Execution")

def get_fully_qualified_name(database, schema, object_name):
    return f"{database}.{schema}.{object_name}"

display_state_sidebar()

if 'forecast_config' not in st.session_state:
    st.warning("Please configure the forecast on the Forecast Configuration page before proceeding.")
else:
    st.write("Forecast Configuration:")
    with st.expander("View Current Forecast Configuration", expanded=False):
        st.json(st.session_state.forecast_config)

    if st.button("Execute Forecast Model"):
        try:
            # Get the fully qualified name for the table/view
            fully_qualified_table = get_fully_qualified_name(
                st.session_state.selected_database,
                st.session_state.selected_schema,
                st.session_state.selected_table_view
            )
            # Update the forecast_config with the fully qualified table name and input_data
            st.session_state.forecast_config['input_data'] = {
                'table': fully_qualified_table,
                'table_type': "view" if "view" in st.session_state.selected_table_view.lower() else "table",
                'timestamp_column': st.session_state.timestamp_column,
                'target_column': st.session_state.target_column,
                'series_column': st.session_state.get('series_column', ''),
                'exogenous_columns': st.session_state.exogenous_columns
            }

            # Create SnowflakeMLForecast instance
            forecast_model = SnowflakeMLForecast(
                config=st.session_state.forecast_config,
                connection_config=st.session_state.connection_config,
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
            st.error("Please check your configuration and try again.")
            
            # Display the forecast configuration for debugging
            st.subheader("Current Forecast Configuration:")
            st.json(st.session_state.forecast_config)
            
            # Display the connection configuration for debugging (make sure to hide sensitive information)
            st.subheader("Current Connection Configuration:")
            safe_connection_config = {k: v for k, v in st.session_state.connection_config.items() if k not in ['password', 'private_key']}
            st.json(safe_connection_config)
            
            st.error("If the problem persists, please contact your streamlit developer with the above information")