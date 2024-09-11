import streamlit as st
import pandas as pd
import os

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

def get_fully_qualified_name(database, schema, object_name):
    return f"{database}.{schema}.{object_name}"

st.title("Database, Schema, and Table/View Selection")
st.write(f"Is token there {os.path.isfile('/snowflake/session/token')}")

display_state_sidebar()

if 'snowpark_connection' not in st.session_state or st.session_state.snowpark_connection is None:
    st.error("No Snowpark connection available. Please return to the Home page to establish a connection.")
else:
    conn = st.session_state.snowpark_connection.get_session()

    # Initialize session state variables if they don't exist
    if 'databases' not in st.session_state:
        if os.path.isfile("/snowflake/session/token"):
            st.session_state.databases = pd.DataFrame([os.getenv('SNOWFLAKE_DATABASE')], columns=['name'])
        else:
            databases_result = conn.sql("SHOW DATABASES").collect()
            st.session_state.databases = pd.DataFrame(databases_result, columns=['created_on', 'name', 'is_default', 'is_current', 'origin', 'owner', 'comment', 'options', 'retention_time', 'kind', 'budget', 'owner_role_type'])
    
    # Step 1: Database selection
    database_names = st.session_state.databases['name'].tolist()
    selected_database = st.selectbox("Step 1: Select a database", database_names, key="database_select")

    if selected_database:
        if selected_database != st.session_state.get('selected_database'):
            st.session_state.selected_database = selected_database
            st.session_state.pop('schemas', None)
            st.session_state.pop('selected_schema', None)
            st.session_state.pop('tables_views', None)
            st.session_state.pop('selected_table_view', None)
            st.rerun()

        # Step 2: Schema selection
        if 'schemas' not in st.session_state:
            schemas_result = conn.sql(f"SHOW SCHEMAS IN DATABASE {selected_database}").collect()
            st.session_state.schemas = [row['name'] for row in schemas_result]
        
        selected_schema = st.selectbox("Step 2: Select a schema", st.session_state.schemas, key="schema_select")

        if selected_schema:
            if selected_schema != st.session_state.get('selected_schema'):
                st.session_state.selected_schema = selected_schema
                st.session_state.pop('tables_views', None)
                st.session_state.pop('selected_table_view', None)
                st.rerun()

            # Step 3: Table/View selection
            if 'tables_views' not in st.session_state:
                tables_result = conn.sql(f"SHOW TABLES IN {selected_database}.{selected_schema}").collect()
                views_result = conn.sql(f"SHOW VIEWS IN {selected_database}.{selected_schema}").collect()
                st.session_state.tables_views = [row['name'] for row in tables_result] + [row['name'] for row in views_result]
            
            selected_table_view = st.selectbox("Step 3: Select a table or view", st.session_state.tables_views, key="table_view_select")

            if selected_table_view:
                if selected_table_view != st.session_state.get('selected_table_view'):
                    st.session_state.selected_table_view = selected_table_view
                    st.session_state.pop('preview', None)
                    st.session_state.pop('columns', None)
                    st.rerun()

                if 'preview' not in st.session_state:
                    fully_qualified_name = get_fully_qualified_name(selected_database, selected_schema, selected_table_view)
                    st.session_state.preview = conn.table(fully_qualified_name).limit(5).to_pandas()
                
                st.write("Table/View preview:")
                st.dataframe(st.session_state.preview)

                if 'columns' not in st.session_state:
                    st.session_state.columns = st.session_state.preview.columns.tolist()

                # Step 4: Select timestamp column
                timestamp_column = st.selectbox("Step 4: Select timestamp column", st.session_state.columns, key="timestamp_select")
                if timestamp_column:
                    st.session_state.timestamp_column = timestamp_column

                    # Step 5: Select target column
                    target_column = st.selectbox("Step 5: Select target column", [col for col in st.session_state.columns if col != timestamp_column], key="target_select")
                    if target_column:
                        st.session_state.target_column = target_column

                        # Step 6: Multi Forecast option
                        series_or_not = st.radio("Step 6: Multi Forecast", ["Yes", "No"], key="multi_forecast_radio")
                        if series_or_not == "Yes":
                            series_column = st.selectbox("Select series column", [col for col in st.session_state.columns if col not in [timestamp_column, target_column]], key="series_select")
                            st.session_state.series_column = series_column
                        else:
                            st.session_state.series_column = None

                        # Step 7: Exogenous variables selection
                        st.subheader("Step 7: Exogenous Variables Selection")
                        exog_option = st.radio("Choose exogenous variables option", ["Select all other columns", "Choose specific columns"])
                        
                        if exog_option == "Select all other columns":
                            exog_columns = [col for col in st.session_state.columns if col not in [timestamp_column, target_column, st.session_state.series_column]]
                            st.write(f"Selected exogenous variables: {', '.join(exog_columns)}")
                        else:
                            available_exog_columns = [col for col in st.session_state.columns if col not in [timestamp_column, target_column, st.session_state.series_column]]
                            exog_columns = st.multiselect("Select exogenous variables", available_exog_columns, key="exog_select")

                        st.session_state.exogenous_columns = exog_columns

                        # Save selections to session state
                        if st.button("Confirm Selection"):
                            st.success("Selection confirmed. Please proceed to the Forecast Configuration page.")

    # Save selected database and schema in session state
    if selected_database and selected_schema:
        st.session_state.selected_database = selected_database
        st.session_state.selected_schema = selected_schema

# Update session configuration
st.subheader("Update Session Configuration")
new_database = st.text_input("New Database", value=st.session_state.get('selected_database', ''))
new_schema = st.text_input("New Schema", value=st.session_state.get('selected_schema', ''))
new_warehouse = st.text_input("New Warehouse", value=st.session_state.connection_config.get('warehouse', ''))

if st.button("Update Session Configuration"):
    st.session_state.connection_config['database'] = new_database
    st.session_state.connection_config['schema'] = new_schema
    st.session_state.connection_config['warehouse'] = new_warehouse
    st.success("Session configuration updated successfully.")
    st.rerun()