import streamlit as st
import pandas as pd
import os

from snowflake.snowpark.exceptions import SnowparkSQLException

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

def ensure_fully_qualified_name(database, schema, object_name):
    return f"{database}.{schema}.{object_name}"

def on_database_select():
    try:
        if st.session_state.database_select != "":
            st.session_state.selected_database = st.session_state.database_select
            st.session_state.selection_step = 2
            st.success(f"Successfully selected database: {st.session_state.database_select}")
            st.session_state.schemas = None  # Reset schemas for the new database
    except SnowparkSQLException as e:
        st.error(f"Error accessing database {st.session_state.database_select}: {str(e)}")
        st.session_state.database_select = ""

def on_schema_select():
    try:
        if st.session_state.schema_select != "":
            st.session_state.selected_schema = st.session_state.schema_select
            st.session_state.selection_step = 3
            st.success(f"Successfully selected schema: {st.session_state.schema_select}")
            st.session_state.tables_views = None  # Reset tables/views for the new schema
    except SnowparkSQLException as e:
        st.error(f"Error accessing schema {st.session_state.schema_select}: {str(e)}")
        st.session_state.schema_select = ""

def on_table_view_select():
    if st.session_state.table_view_select != "":
        st.session_state.selected_table_view = st.session_state.table_view_select
        st.session_state.selection_step = 4
        st.success(f"Successfully selected table/view: {st.session_state.table_view_select}")
        st.session_state.preview = None  # Reset preview for the new table/view

st.title("Database, Schema, and Table/View Selection")

display_state_sidebar()

if 'snowpark_connection' not in st.session_state or st.session_state.snowpark_connection is None:
    st.error("No Snowpark connection available. Please return to the Home page to establish a connection.")
else:
    conn = st.session_state.snowpark_connection.get_session()

    # Initialize step if it doesn't exist
    if 'selection_step' not in st.session_state:
        st.session_state.selection_step = 1

    # Initialize databases
    if 'databases' not in st.session_state:
        if os.path.isfile("/snowflake/session/token"):
            st.session_state.databases = pd.DataFrame([os.getenv('SNOWFLAKE_DATABASE')], columns=['name'])
        else:
            databases_result = conn.sql("SHOW DATABASES").collect()
            st.session_state.databases = pd.DataFrame(databases_result, columns=['created_on', 'name', 'is_default', 'is_current', 'origin', 'owner', 'comment', 'options', 'retention_time', 'kind', 'budget', 'owner_role_type'])

    # Step 1: Database selection
    st.subheader("Step 1: Select a database")
    database_names = [""] + st.session_state.databases['name'].tolist()
    st.selectbox("Choose a database", database_names, key="database_select", on_change=on_database_select)

    # Step 2: Schema selection
    if st.session_state.selection_step >= 2:
        st.subheader("Step 2: Select a schema")
        if 'schemas' not in st.session_state or st.session_state.schemas is None:
            schemas_result = conn.sql(f"SHOW SCHEMAS IN DATABASE {st.session_state.selected_database}").collect()
            st.session_state.schemas = [""] + [row['name'] for row in schemas_result]
        
        st.selectbox("Choose a schema", st.session_state.schemas, key="schema_select", on_change=on_schema_select)

    # Step 3: Table/View selection
    if st.session_state.selection_step >= 3:
        st.subheader("Step 3: Select a table or view")
        if 'tables_views' not in st.session_state or st.session_state.tables_views is None:
            tables_result = conn.sql(f"SHOW TABLES IN {st.session_state.selected_database}.{st.session_state.selected_schema}").collect()
            views_result = conn.sql(f"SHOW VIEWS IN {st.session_state.selected_database}.{st.session_state.selected_schema}").collect()
            st.session_state.tables_views = [""] + [row['name'] for row in tables_result] + [row['name'] for row in views_result]
        
        st.selectbox("Choose a table or view", st.session_state.tables_views, key="table_view_select", on_change=on_table_view_select)

    # Step 4: Preview and Column Selection
    if st.session_state.selection_step >= 4:
        st.subheader("Step 4: Preview and Column Selection")
        if 'preview' not in st.session_state or st.session_state.preview is None:
            fully_qualified_name = ensure_fully_qualified_name(st.session_state.selected_database, st.session_state.selected_schema, st.session_state.selected_table_view)
            st.session_state.preview = conn.table(fully_qualified_name).limit(5).to_pandas()
        
        st.write("Table/View preview:")
        st.dataframe(st.session_state.preview)

        if 'columns' not in st.session_state:
            st.session_state.columns = [""] + st.session_state.preview.columns.tolist()

        st.selectbox("Select timestamp column", st.session_state.columns, key="timestamp_select")
        st.selectbox("Select target column", st.session_state.columns, key="target_select")
        
        series_or_not = st.radio("Multi Forecast", ["No", "Yes"])
        if series_or_not == "Yes":
            st.selectbox("Select series column", st.session_state.columns, key="series_select")

        # Exogenous variables selection
        st.subheader("Exogenous Variables Selection")
        exog_option = st.radio("Choose exogenous variables option", ["Select all other columns", "Choose specific columns"])
        
        if exog_option == "Select all other columns":
            excluded_columns = [st.session_state.timestamp_select, st.session_state.target_select]
            if series_or_not == "Yes":
                excluded_columns.append(st.session_state.series_select)
            exog_columns = [col for col in st.session_state.columns[1:] if col not in excluded_columns]  # Exclude blank option
            st.write(f"Selected exogenous variables: {', '.join(exog_columns)}")
            st.session_state.exog_select = exog_columns
        else:
            available_exog_columns = [col for col in st.session_state.columns[1:] if col not in [st.session_state.timestamp_select, st.session_state.target_select, st.session_state.get('series_select')]]
            st.session_state.exog_select = st.multiselect("Select exogenous variables", available_exog_columns, key="exog_multiselect")

        if st.button("Confirm Selection"):
            st.session_state.timestamp_column = st.session_state.timestamp_select
            st.session_state.target_column = st.session_state.target_select
            st.session_state.series_column = st.session_state.series_select if series_or_not == "Yes" else None
            st.session_state.exogenous_columns = st.session_state.exog_select
            st.success("Selection confirmed. Please proceed to the Forecast Configuration page.")


    # Display current step
    st.sidebar.write(f"Current Step: {st.session_state.selection_step}")