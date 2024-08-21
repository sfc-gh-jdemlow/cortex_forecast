# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/01_cortex_forecast.ipynb.

# %% auto 0
__all__ = ['SnowflakeMLForecast']

# %% ../nbs/01_cortex_forecast.ipynb 4
import yaml
import random
import string
import logging
import numpy as np
import streamlit as st
import altair as alt
import pandas as pd

from datetime import datetime
from cortex_forecast.connection import SnowparkConnection


logging.getLogger('snowflake.snowpark').setLevel(logging.WARNING)
 

# %% ../nbs/01_cortex_forecast.ipynb 5
class SnowflakeMLForecast(SnowparkConnection):
    def __init__(self, config_file, connection_config=None):
        super().__init__(connection_config=connection_config)
        with open(config_file, 'r') as file:
            self.config = yaml.safe_load(file)
        self.model_name = self._generate_unique_model_name()

    def _generate_unique_model_name(self):
        suffix = ''.join(random.choices(string.ascii_lowercase, k=5))
        timestamp = datetime.now().strftime("%Y%m%d")
        return f"{self.config['model']['name']}_{timestamp}_{suffix}"

    def _generate_input_data_sql(self):
        table = self.config['input_data']['table']
        timestamp_col = self.config['input_data']['timestamp_column']
        target_col = self.config['input_data']['target_column']
        exogenous_cols = self.config['input_data'].get('exogenous_columns') or []
        
        training_days = self.config['forecast_config']['training_days']

        columns = [f"TO_TIMESTAMP_NTZ({timestamp_col}) AS {timestamp_col}",
                f"{target_col} AS target"]
        columns.extend(exogenous_cols)

        sql = f"""
        CREATE OR REPLACE TRANSIENT TABLE {self.model_name}_train AS
        SELECT {', '.join(columns)}
        FROM {table}
        WHERE TO_TIMESTAMP_NTZ({timestamp_col}) < DATEADD(day, -{training_days}, CURRENT_DATE())
        """
        return sql

    def _generate_create_model_sql(self):
        # Use the table name from the config file for the training data
        input_data = f"SYSTEM$REFERENCE('{self.config['input_data']['table_type']}', '{self.config['input_data']['table']}')"
        timestamp_col = self.config['input_data']['timestamp_column']
        target_col = self.config['input_data']['target_column']
        series_col = self.config['input_data'].get('series_column')
        config_object = self.config['forecast_config'].get('config_object', {})
        
        # Start building the SQL
        sql = f"""
        CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {self.model_name}(
            INPUT_DATA => {input_data},
            TIMESTAMP_COLNAME => '{timestamp_col}',
            TARGET_COLNAME => '{target_col}',
        """
        
        # Add optional series column; use NULL if series_col is None
        if series_col:
            sql += f"SERIES_COLNAME => '{series_col}',"
        
        # Construct the CONFIG_OBJECT as a JSON-like string
        config_sql = "{"
        for key, value in config_object.items():
            if isinstance(value, dict):
                nested_config = "{"
                nested_config += ", ".join([f"'{k}': {self._format_value(v)}" for k, v in value.items()])
                nested_config += "}"
                config_sql += f"'{key}': {nested_config}, "
            else:
                config_sql += f"'{key}': {self._format_value(value)}, "
        config_sql = config_sql.rstrip(", ") + "}"

        sql += f"CONFIG_OBJECT => {config_sql},"
        
        sql = sql.rstrip(',')  # Clean up trailing commas
        sql += ")"
        
        # Handle optional TAG and COMMENT
        tags = self.config['model'].get('tags')
        comment = self.config['model'].get('comment')
        
        if tags:
            tag_str = ", ".join([f"{k} = '{v}'" for k, v in tags.items()])
            sql += f" WITH TAG ({tag_str})"
        
        if comment:
            sql += f" COMMENT = '{comment}'"
        
        sql += ";"

        # Debug prints
        print("Generated SQL:")
        print(sql)
        
        return sql

    def create_tags(self):
        """
        Create the necessary tags in Snowflake before running the forecast creation.
        If a tag already exists, it will notify the user instead of raising an error.
        """
        tags = self.config['model'].get('tags')
        if not tags:
            self.display("No tags to create.", content_type="text")
            return

        for tag_name, tag_comment in tags.items():
            create_tag_sql = f"CREATE TAG {tag_name} COMMENT = 'Specifies the {tag_comment.lower()}';"
            try:
                self.display(f"Attempting to create tag: {tag_name}", content_type="text")
                self.run_command(create_tag_sql)
                self.display(f"Tag '{tag_name}' created successfully.", content_type="text")
            except Exception as e:
                if "already exists" in str(e):
                    self.display(f"Tag '{tag_name}' already exists.", content_type="text")
                else:
                    self.display(f"Error creating tag '{tag_name}': {e}", content_type="text")

    def _format_value(self, value):
        """
        Helper function to format values for SQL. Converts Python None to SQL NULL,
        and ensures strings are correctly quoted.
        """
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, str):
            return f"'{value}'"
        return str(value)

    def _generate_forecast_sql(self):
        try:
            forecast_days = self.config['forecast_config']['forecast_days']
            output_table = self.config['output']['table']
            
            # Extract the evaluation config from the nested config_object
            config_object = self.config['forecast_config'].get('config_object', {})
            evaluation_config = config_object.get('evaluation_config', {})

            print("Configuration Details:")
            print(f"Forecast Days: {forecast_days}")
            print(f"Output Table: {output_table}")
            print(f"Evaluation Config: {evaluation_config}")

            # Ensure 'prediction_interval' is present in evaluation_config
            prediction_interval = evaluation_config.get('prediction_interval', 0.95)  # Default to 0.95 if not provided

            sql = f"""
            CREATE OR REPLACE TABLE {output_table} AS
            SELECT
                ts AS forecast_date,
                forecast,
                lower_bound,
                upper_bound
            FROM
                TABLE({self.model_name}!FORECAST(
                    FORECASTING_PERIODS => {forecast_days},
                    CONFIG_OBJECT => {{'prediction_interval': {prediction_interval}}}
                ));
            """
            print("Generated Forecast SQL:")
            print(sql)
            return sql
        except KeyError as e:
            print(f"KeyError encountered: {e}")
            raise e

    def run_query(self, query):
        """
        Execute a query and return the result as a Pandas DataFrame.
        """
        df = self.session.sql(query).to_pandas() if self.session else None
        return df

    def run_command(self, query):
        """
        Execute a command and return the result.
        """
        result = self.session.sql(query).collect() if self.session else None
        return result

    def create_and_run_forecast(self):
        self.create_tags()

        print("Step 1/4: Creating training table...")
        self.run_command(self._generate_input_data_sql())

        print("Step 2/4: Creating forecast model...")
        self.run_command(self._generate_create_model_sql())

        print("Step 3/4: Generating forecasts...")
        self.run_command(self._generate_forecast_sql())

        print("Step 4/4: Fetching forecast results...")
        forecast_data = self.run_query(f"SELECT * FROM {self.config['output']['table']} ORDER BY forecast_date")

        return forecast_data

    def cleanup(self):
        print("Cleaning up temporary tables and models...")
        cleanup_commands = f"""
        DROP TABLE IF EXISTS {self.model_name}_train;
        DROP TABLE IF EXISTS {self.config['output']['table']};
        """
        # DROP MODEL IF EXISTS {self.model_name};

        for command in cleanup_commands.split(';'):
            if command.strip():
                self.run_command(command)

    # Other existing methods...

    def is_streamlit(self):
        """
        Check if the environment is Streamlit.
        """
        try:
            return st._is_running_with_streamlit
        except AttributeError:
            return False

    def display(self, content, content_type="text", **kwargs):
        """
        Display content based on the environment (Streamlit or console).
        """
        if self.is_streamlit():
            if content_type == "text":
                st.write(content)
            elif content_type == "chart":
                st.altair_chart(content, use_container_width=True)
            elif content_type == "dataframe":
                st.write(content)
            elif content_type == "code":
                st.code(content, language=kwargs.get('language', ''))
        else:
            if content_type == "text":
                print(content)
            elif content_type == "chart":
                content.show()
            elif content_type == "dataframe":
                print(content)
            elif content_type == "code":
                print(content)

    def create_visualization(self, df, max_historic_date):
        max_historic_date_rule = alt.Chart(pd.DataFrame({'x': [max_historic_date]})).mark_rule(color='orange', strokeDash=[5, 5]).encode(x='x:T')
        max_historic_date_label = alt.Chart(pd.DataFrame({'x': [max_historic_date], 'label': ['Forecast -->']})).mark_text(
            align='left', baseline='bottom', dx=5, dy=5, fontSize=12
        ).encode(x='x:T', y=alt.value(5), text='label:N')

        line_chart = (
            alt.Chart(df)
            .mark_line(point=True)
            .encode(
                x=alt.X("TS:T", axis=alt.Axis(title="Date")),
                y=alt.Y("Volume:Q"),
                color=alt.Color('Value Type:N', legend=alt.Legend(title="Forecast Type")),
                strokeDash=alt.StrokeDash('Type:N', legend=alt.Legend(title="Data Type"))
            ).properties(
                title={
                    "text": ["Forecast and Historic Volume"], 
                    "subtitle": ["Comparing forecasted volume with historic data"],
                    "color": "black",
                    "subtitleColor": "gray"
                },
                width=800,
                height=400
            )
        )

        return line_chart, max_historic_date_rule, max_historic_date_label

    def generate_forecast_and_visualization(self, forecasting_period, confidence_interval):
        df_forecast = self.session.sql(f"""
            CALL {self.model_name}!FORECAST(
                FORECASTING_PERIODS => {forecasting_period},
                CONFIG_OBJECT => {{'prediction_interval': {confidence_interval}}}
            );
        """).collect()
        df_forecast = pd.DataFrame(df_forecast)
        df_actuals = self.load_historic_actuals()
        timestamp_col = self.config['input_data']['timestamp_column']
        target_col = self.config['input_data']['target_column']
        df_actuals = df_actuals.rename(columns={timestamp_col.upper(): 'TS', target_col.upper(): 'FORECAST'})

        try:
            print('Getting historical max date') 
            max_historic_date = df_actuals['TS'].max()
            df_actuals['LOWER_BOUND'] = np.NaN
            df_actuals['UPPER_BOUND'] = np.NaN
            df_actuals['Type'] = 'Historic'
            df_forecast['Type'] = 'Forecast'
            df_combined = pd.concat([df_forecast, df_actuals], ignore_index=True)
            df_combined['LOWER_BOUND'] = np.where(df_combined['LOWER_BOUND'] < 0, 0, df_combined['LOWER_BOUND'])
            df = df_combined.melt(id_vars=['TS', 'Type'], value_vars=['FORECAST', 'LOWER_BOUND', 'UPPER_BOUND'], var_name='Value Type', value_name='Volume')
            df = df.dropna(subset=['Volume'])
            line_chart, max_historic_date_rule, max_historic_date_label = self.create_visualization(df, max_historic_date)
            if self.is_streamlit():
                st.session_state['chart'] = alt.layer(line_chart, max_historic_date_rule, max_historic_date_label)
                st.session_state['df'] = df
            else:
                self.display(alt.layer(line_chart, max_historic_date_rule, max_historic_date_label), content_type="chart")
                self.display(df, content_type="dataframe")
            self.show_key_data_aspects()
        except KeyError as e:
            print(f"KeyError encountered: {e}")

    def show_key_data_aspects(self):
        self.display("Top 10 Feature Importances", content_type="text")
        feature_importance = f"CALL {self.model_name}!EXPLAIN_FEATURE_IMPORTANCE();"
        f_i = self.session.sql(feature_importance).collect()[:10]
        df_fi = pd.DataFrame(f_i)
        df_fi = df_fi.drop(columns=['SERIES'])
        chart = alt.Chart(df_fi).mark_bar().encode(
            x=alt.X('SCORE:Q', title='Feature Importance'),
            y=alt.Y('FEATURE_NAME:N', title='Feature', sort='-x')
        ).properties(
            title="Feature Importance Plot",
            width=600,
            height=300
        )
        self.display(chart, content_type="chart")
        self.display(df_fi, content_type="dataframe")
        
        self.display("Underlying Model Metrics", content_type="text")
        metric_call = f"CALL {self.model_name}!SHOW_EVALUATION_METRICS();"
        metrics = self.session.sql(metric_call).collect()
        metrics = [metric.as_dict() for metric in metrics]
        metrics = pd.DataFrame(metrics)
        metrics = metrics.drop(columns=['SERIES'])
        self.display(metrics, content_type="dataframe")

    # Custom method to load historical data
    def load_historic_actuals(self):
        return self.session.table(self.config['input_data']['table']).to_pandas()
