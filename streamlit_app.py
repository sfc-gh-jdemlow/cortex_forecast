import streamlit as st
import os
from snowflake.snowpark.version import VERSION
from cortex_forecast.connection import SnowparkConnection

st.set_page_config(page_title="Snowflake ML Forecast", layout="wide")

st.title("Snowflake ML Forecast - Home")

st.markdown("""
## Welcome to the Snowflake ML Forecast Application

This application leverages Snowflake's ML capabilities to create, run, and schedule time series forecasts using your data. Our intuitive interface guides you through the process of selecting your data, configuring your forecast model, executing the forecast, and setting up automated schedules.

### Key Features:

1. **Data Selection**: 
   - Choose your Snowflake database, schema, and table/view.
   - Preview your data and select relevant columns for forecasting.

2. **Forecast Configuration**:
   - Set up your forecast model with customizable parameters.
   - Define timestamp, target, and optional series columns.
   - Select exogenous variables to enhance your forecast.

3. **Model Execution**:
   - Run your configured forecast model on demand.
   - View forecast results and visualizations.

4. **Scheduling**: (Coming in another release)


### How to Use:

1. Start by connecting to your Snowflake account (if not already connected).
2. Navigate through the pages using the sidebar:
   - **Database and Table Selection**: Choose your data source.
   - **Forecast Configuration**: Set up your forecast model parameters.
   - **Model Execution**: Run your forecast and view results.

3. Follow the prompts on each page to complete the process.

### Getting Started:

To begin, ensure you have a valid Snowflake connection. If you're not yet connected, you'll be prompted to enter your Snowflake credentials.

Once connected, use the sidebar to navigate to the "Database and Table Selection" page to start setting up your forecast.

### Need Help?

If you encounter any issues or have questions, please contact our support team or refer to the documentation for more detailed information on using this application.

Let's get started with your time series forecasting!
""")


st.markdown("""
### Next Steps:

1. Use the sidebar to navigate to "Database and Table Selection".
2. Select your data source for forecasting.
3. Configure your forecast model parameters.
4. Run your forecast and view the results.
5. Optionally, set up a schedule for automated forecasting.

Good luck with your forecasting project!
""")

# Initialize session state
if 'snowpark_connection' not in st.session_state:
    st.session_state.snowpark_connection = None

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

def display_connection_info(session, config):
    snowflake_environment = session.sql('SELECT current_user(), current_version()').collect()
    snowpark_version = VERSION
    
    st.write("Connection Established with the following parameters:")
    st.write(f"User: {snowflake_environment[0][0]}")
    st.write(f"Role: {config['role']}")
    st.write(f"Database: {config['database']}")
    st.write(f"Schema: {config['schema']}")
    st.write(f"Warehouse: {config['warehouse']}")
    st.write(f"Snowflake version: {snowflake_environment[0][1]}")
    st.write(f"Snowpark for Python version: {snowpark_version[0]}.{snowpark_version[1]}.{snowpark_version[2]}")

# Create SnowparkConnection if not exists
if st.session_state.snowpark_connection is None:
    try:
        # Use default SIS connection parameters
        connection_config = {
            'user': os.getenv('SNOWFLAKE_USER', ''),
            'password': os.getenv('SNOWFLAKE_PASSWORD', ''),
            'account': os.getenv('SNOWFLAKE_ACCOUNT', ''),
            'database': 'CORTEX',
            'warehouse': 'CORTEX_WH',
            'schema': 'DEV',
            'role': 'CORTEX_USER_ROLE'
        }
        
        connection = SnowparkConnection(connection_config=connection_config)
        session = connection.get_session()

        st.session_state.connection_config = connection_config
        st.session_state.snowpark_connection = connection
        display_connection_info(session, connection_config)
        
    except Exception as e:
        st.error(f"Failed to establish connection using default SIS parameters: {str(e)}")
        st.write("Please provide connection details:")
        
        user_config = create_connection_config()
        
        if st.button("Connect"):
            try:
                connection = SnowparkConnection(connection_config=user_config)
                session = connection.get_session()
                
                st.session_state.snowpark_connection = connection
                st.session_state.connection_config = user_config
                display_connection_info(session, user_config)
            except Exception as e:
                st.error(f"Failed to establish connection with provided details: {str(e)}")

# Add options to change database, schema, and warehouse
if st.session_state.snowpark_connection is not None:
    st.header("Update Connection Parameters")
    
    new_database = st.text_input("Change Database", value=st.session_state.connection_config['database'])
    new_schema = st.text_input("Change Schema", value=st.session_state.connection_config['schema'])
    new_warehouse = st.text_input("Change Warehouse", value=st.session_state.connection_config['warehouse'])
    
    if st.button("Update Connection"):
        st.session_state.connection_config['database'] = new_database
        st.session_state.connection_config['schema'] = new_schema
        st.session_state.connection_config['warehouse'] = new_warehouse
        
        st.success("Connection parameters updated successfully.")
        display_connection_info(st.session_state.snowpark_connection.get_session(), st.session_state.connection_config)

if st.session_state.snowpark_connection is None:
    st.warning("You are not connected to Snowflake. Please connect to proceed.")
else:
    st.success("You are connected to Snowflake. You can proceed to the next steps using the sidebar navigation.")
