# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/01_cortex_forecast.ipynb.

# %% auto 0
__all__ = ['SnowflakeMLForecast']

# %% ../nbs/01_cortex_forecast.ipynb 4
import yaml
import random
import string
import logging
import numpy as np
import pandas as pd
import altair as alt
import logging

from datetime import datetime
from .connection import SnowparkConnection

logging.getLogger('snowflake.snowpark').setLevel(logging.WARNING)

# %% ../nbs/01_cortex_forecast.ipynb 5
class SnowflakeMLForecast(SnowparkConnection):
    def __init__(self, config_file, connection_config=None, is_streamlit=False):
        super().__init__(connection_config=connection_config)
        with open(config_file, 'r') as file:
            self.config = yaml.safe_load(file)
        self.model_name = self._generate_unique_model_name()
        self.training_data_query = None
        self.is_streamlit = is_streamlit

    def _generate_unique_model_name(self):
        suffix = ''.join(random.choices(string.ascii_lowercase, k=5))
        timestamp = datetime.now().strftime("%Y%m%d")
        return f"{self.config['model']['name']}_{timestamp}_{suffix}"

    def _generate_input_data_sql(self):
        table = self.config['input_data']['table']
        timestamp_col = self.config['input_data']['timestamp_column']
        target_col = self.config['input_data']['target_column']
        exogenous_cols = self.config['input_data'].get('exogenous_columns') or []
        training_days = self.config['forecast_config'].get('training_days')

        columns = [f"TO_TIMESTAMP_NTZ({timestamp_col}) AS {timestamp_col}",
                   f"{target_col} AS {target_col}"]

        if exogenous_cols:
            columns.extend(exogenous_cols)
        else:
            columns.append("*")

        sql = f"""
        CREATE OR REPLACE TEMPORARY TABLE {self.model_name}_train AS
        SELECT {', '.join(columns)} EXCLUDE ({timestamp_col}, {target_col})
        FROM {table}
        """

        if training_days:
            sql += f"""
            WHERE TO_TIMESTAMP_NTZ({timestamp_col}) 
            BETWEEN 
            DATEADD(day, -{training_days}, (SELECT MAX({timestamp_col}) FROM {table})) 
            AND 
            (SELECT MAX({timestamp_col}) FROM {table})
            """

        sql += ";"

        self.training_data_query = sql
        self.display("Generated SQL:", content_type="text")
        self.display(sql, content_type="code", language="sql")
        return sql

    def _generate_create_model_sql(self):
        input_data = f"SYSTEM$REFERENCE('{self.config['input_data']['table_type']}', '{self.model_name}_train')"
        timestamp_col = self.config['input_data']['timestamp_column']
        target_col = self.config['input_data']['target_column']
        series_col = self.config['input_data'].get('series_column')
        config_object = self.config['forecast_config'].get('config_object', {})
    
        sql = f"""
        CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {self.model_name}(
            INPUT_DATA => {input_data},
            TIMESTAMP_COLNAME => '{timestamp_col}',
            TARGET_COLNAME => '{target_col}',
        """
        
        if series_col:
            sql += f"""SERIES_COLNAME => '{series_col}',\n"""
        
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
        tags = self.config['model'].get('tags')
        comment = self.config['model'].get('comment')
        
        if tags:
            tag_str = ", ".join([f"{k} = '{v}'" for k, v in tags.items()])
            sql += f" WITH TAG ({tag_str})"
        
        if comment:
            sql += f" COMMENT = '{comment}'"
        
        sql += ";"

        self.display("Generated SQL:", content_type="text")
        self.display(sql, content_type="code", language="sql")
        
        return sql

    def _format_value(self, value):
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
            forecast_days = self.config['forecast_config'].get('forecast_days')
            output_table = self.config['output']['table']
            input_data_table = self.config['forecast_config'].get('table')
            config_object = self.config['forecast_config'].get('config_object', {})
            evaluation_config = config_object.get('evaluation_config', {})
            prediction_interval = evaluation_config.get('prediction_interval', 0.95)
            series_col = self.config['input_data'].get('series_column')
            timestamp_col = self.config['input_data']['timestamp_column']

            # Check if the table exists
            check_table_sql = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{output_table}'"
            table_exists = self.session.sql(check_table_sql).collect()[0][0] > 0

            if table_exists:
                sql = f"INSERT INTO {output_table} "
            else:
                sql = f"CREATE OR REPLACE TABLE {output_table} AS "

            sql += "SELECT "

            if series_col:
                sql += f"series::string as {series_col}, "
    
            sql += f"""
                ts AS {timestamp_col},
                CASE WHEN forecast < 0 THEN 0 ELSE forecast END AS forecast,
                CASE WHEN lower_bound < 0 THEN 0 ELSE lower_bound END AS lower_bound,
                CASE WHEN upper_bound < 0 THEN 0 ELSE upper_bound END AS upper_bound,
                '{self.model_name}' AS model_name,
                CURRENT_TIMESTAMP() AS creation_date,
                '{self.config['model']['comment']}' AS model_comment
            FROM
                TABLE({self.model_name}!FORECAST(
            """

            if input_data_table:
                sql += f"""
                INPUT_DATA => SYSTEM$REFERENCE('TABLE', '{input_data_table}'),
                TIMESTAMP_COLNAME => '{timestamp_col}',\n"""

            if series_col:
                sql += f"SERIES_COLNAME => '{series_col}',\n"

            sql += f"CONFIG_OBJECT => {{'prediction_interval': {prediction_interval}}}\n"
            
            if forecast_days:
                sql += f", FORECASTING_PERIODS => {forecast_days}"
            
            sql += "));"

            self.display("Generated Forecast SQL:", content_type="text")
            self.display(sql, content_type="code", language="sql")
            return sql

        except KeyError as e:
            self.display(f"KeyError encountered: {e}", content_type="text")
            raise e

    def run_query(self, query):
        df = self.session.sql(query).to_pandas() if self.session else None
        return df

    def run_command(self, query):
        result = self.session.sql(query).collect() if self.session else None
        return result

    def create_and_run_forecast(self):
        self.create_tags()

        self.display("Step 1/4: Creating training table...", content_type="text")
        sql = self._generate_input_data_sql()
        self.run_command(sql)

        self.display("Step 2/4: Creating forecast model...", content_type="text")
        sql = self._generate_create_model_sql()
        self.run_command(sql)

        self.display("Step 3/4: Generating forecasts...", content_type="text")
        sql = self._generate_forecast_sql()
        self.run_command(sql)

        self.display("Step 4/4: Fetching forecast results...", content_type="text")
        forecast_data = self.run_query(f"SELECT * FROM {self.config['output']['table']} ORDER BY {self.config['input_data']['timestamp_column']}")

        return forecast_data

    def cleanup(self):
        self.display("Cleaning up temporary tables and models...", content_type="text")
        cleanup_commands = f"""
        DROP TABLE IF EXISTS {self.model_name}_train;
        DROP TABLE IF EXISTS {self.config['output']['table']};
        """
        for command in cleanup_commands.split(';'):
            if command.strip():
                self.run_command(command)

    def create_tags(self):
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


    def get_training_data_query(self):
        if self.training_data_query is None:
            self.display("Training data query has not been generated yet.", content_type="text")
        return self.training_data_query

    def load_historic_actuals(self, historical_steps_back: int):
        table = self.config['input_data']['table']
        timestamp_col = self.config['input_data']['timestamp_column']
        target_col = self.config['input_data']['target_column']
        series_col = self.config['input_data'].get('series_column')

        columns = [timestamp_col, target_col]
        if series_col:
            columns.append(series_col)

        if series_col:
            query = f"""
            WITH ranked_data AS (
                SELECT 
                    {', '.join(columns)},
                    ROW_NUMBER() OVER (PARTITION BY {series_col} ORDER BY {timestamp_col} DESC) as rn
                FROM {table}
            )
            SELECT {', '.join(columns)}
            FROM ranked_data
            WHERE rn <= {historical_steps_back}
            ORDER BY {series_col}, {timestamp_col} DESC
            """
        else:
            query = f"""
            SELECT {', '.join(columns)}
            FROM {table}
            ORDER BY {timestamp_col} DESC
            LIMIT {historical_steps_back}
            """

        self.display("Executing historic actuals query:", content_type="text")
        self.display(query, content_type="code", language="sql")
        
        df_actuals = self.session.sql(query).collect()
        return pd.DataFrame(df_actuals)

    def generate_forecast_and_visualization(self, show_historical=True, historical_steps_back=21):
        series_col = self.config['input_data'].get('series_column')
        timestamp_col = self.config['input_data']['timestamp_column']
        target_col = self.config['input_data']['target_column']
        output_table = self.config['output']['table']

        # Fetch forecast data
        forecast_query = f"""
            SELECT *
            FROM {output_table}
            WHERE model_name = '{self.model_name}'
            ORDER BY {timestamp_col} DESC
        """
        if series_col:
            forecast_query += f", {series_col}"

        self.display("Executing forecast query:", content_type="text")
        self.display(forecast_query, content_type="code", language="sql")
        
        df_forecast = self.session.sql(forecast_query).collect()
        df_forecast = pd.DataFrame(df_forecast)
        
        self.display("Forecast data preview (last 5 rows):", content_type="text")
        self.display(df_forecast.tail(), content_type="dataframe")

        # Fetch and prepare historical data
        df_actuals = self.load_historic_actuals(historical_steps_back)
        self.display("Historical data preview (last 5 rows):", content_type="text")
        self.display(df_actuals.tail(), content_type="dataframe")

        # Ensure column names are consistent
        df_forecast.columns = df_forecast.columns.str.upper()
        df_actuals.columns = df_actuals.columns.str.upper()

        # Identify key columns
        ts_col = next(col for col in df_forecast.columns if col in [timestamp_col.upper(), 'TS', 'TIMESTAMP', 'DATE'])
        forecast_col = next(col for col in df_forecast.columns if col in ['FORECAST', 'PREDICTION'])
        lower_bound_col = next(col for col in df_forecast.columns if col in ['LOWER_BOUND', 'LOWER'])
        upper_bound_col = next(col for col in df_forecast.columns if col in ['UPPER_BOUND', 'UPPER'])

        try:
            self.display('Getting historical max date', content_type="text")
            max_historic_date = df_actuals[ts_col].max()
            self.display(f"Max historical date: {max_historic_date}", content_type="text")

            # Prepare forecast data
            df_forecast['TYPE'] = 'Forecast'
            df_forecast[lower_bound_col] = np.maximum(df_forecast[lower_bound_col], 0)
            df_forecast[upper_bound_col] = np.maximum(df_forecast[upper_bound_col], 0)
            df_forecast[forecast_col] = np.maximum(df_forecast[forecast_col], 0)

            # Prepare historical data
            df_actuals['TYPE'] = 'Historic'
            df_actuals[forecast_col] = df_actuals[target_col.upper()]
            df_actuals[lower_bound_col] = np.NaN
            df_actuals[upper_bound_col] = np.NaN

            # Combine data
            if show_historical:
                df_combined = pd.concat([df_forecast, df_actuals], ignore_index=True)
            else:
                df_combined = df_forecast

            # Melt the dataframe
            id_vars = [ts_col, 'TYPE']
            if series_col:
                id_vars.append(series_col.upper())

            df_melted = df_combined.melt(id_vars=id_vars, 
                                        value_vars=[forecast_col, lower_bound_col, upper_bound_col], 
                                        var_name='VALUE_TYPE', value_name='VOLUME')
            
            df_melted = df_melted.dropna(subset=['VOLUME'])
            
            self.display("Combined data preview (last 5 rows):", content_type="text")
            self.display(df_melted.tail(), content_type="dataframe")

            # Create and display charts
            charts = self.create_altair_visualization(df_melted, max_historic_date, series_col, ts_col)
            self.display_charts(charts, series_col)

            # Display key data aspects
            self.show_key_data_aspects(series_col)

        except KeyError as e:
            self.display(f"KeyError encountered: {e}", content_type="text")

    def create_altair_visualization(self, df, max_historic_date, series_col, ts_col):
        if series_col:
            charts = {}
            for series in df[series_col.upper()].unique():
                series_df = df[df[series_col.upper()] == series]
                charts[series] = self.create_single_chart(series_df, max_historic_date, series, ts_col)
            return charts
        else:
            return self.create_single_chart(df, max_historic_date, timestamp_col=ts_col)

    def create_single_chart(self, df, max_historic_date, series=None, timestamp_col='TS'):
        max_historic_date_rule = alt.Chart(pd.DataFrame({'x': [max_historic_date]})).mark_rule(
            color='orange', 
            strokeDash=[5, 5]
        ).encode(x='x:T')

        max_historic_date_label = alt.Chart(pd.DataFrame({'x': [max_historic_date], 'label': ['Forecast -->']})).mark_text(
            align='left', 
            baseline='bottom', 
            dx=5, 
            dy=5, 
            fontSize=12
        ).encode(x='x:T', y=alt.value(5), text='label:N')

        line_chart = alt.Chart(df).mark_line(point=True).encode(
            x=alt.X(f"{timestamp_col}:T", axis=alt.Axis(title="Date")),
            y=alt.Y("VOLUME:Q"),
            color=alt.Color('VALUE_TYPE:N', legend=alt.Legend(title="Forecast Type")),
            strokeDash=alt.StrokeDash('TYPE:N', legend=alt.Legend(title="Data Type"))
        ).properties(
            title={
                "text": ["Forecast and Historic Volume" + (f" for {series}" if series else "")],
                "subtitle": ["Comparing forecasted volume with historic data"],
                "color": "black",
                "subtitleColor": "gray"
            },
            width=800,
            height=400
        )

        return alt.layer(line_chart, max_historic_date_rule, max_historic_date_label)

    def display_charts(self, charts, series_col):
        if isinstance(charts, dict):
            for series, chart in charts.items():
                self.display(f"Forecast for {series}", content_type="text")
                self.display(chart, content_type="chart")
        else:
            self.display(charts, content_type="chart")


    def streamlit_display(self, charts, series_col):
        import streamlit as st
        if series_col:
            for series, chart in charts.items():
                st.write(f"Forecast for {series}")
                st.altair_chart(chart, use_container_width=True)
        else:
            st.altair_chart(charts, use_container_width=True)

    def jupyter_display(self, charts, series_col):
        from IPython.display import display
        if series_col:
            for series, chart in charts.items():
                print(f"Forecast for {series}")
                display(chart)
        else:
            display(charts)

    def show_key_data_aspects(self, series_col=None):
        self.display("Top 10 Feature Importances", content_type="text")
        feature_importance = f"CALL {self.model_name}!EXPLAIN_FEATURE_IMPORTANCE();"
        f_i = self.session.sql(feature_importance).collect()
        df_fi = pd.DataFrame(f_i)
        
        if series_col and 'SERIES' in df_fi.columns:
            for series in df_fi['SERIES'].unique():
                series_df = df_fi[df_fi['SERIES'] == series].sort_values('SCORE', ascending=False).head(10)
                chart = self.create_feature_importance_chart(series_df, series)
                self.display(f"Feature Importance for {series}", content_type="text")
                self.display(chart, content_type="chart")
                self.display(series_df, content_type="dataframe")
        else:
            df_fi = df_fi.sort_values('SCORE', ascending=False).head(10)
            chart = self.create_feature_importance_chart(df_fi)
            self.display(chart, content_type="chart")
            self.display(df_fi, content_type="dataframe")

        self.display("Underlying Model Metrics", content_type="text")
        metric_call = f"CALL {self.model_name}!SHOW_EVALUATION_METRICS();"
        metrics = self.session.sql(metric_call).collect()
        metrics = [metric.as_dict() for metric in metrics]
        metrics_df = pd.DataFrame(metrics)
        
        if series_col and 'SERIES' in metrics_df.columns:
            for series in metrics_df['SERIES'].unique():
                series_metrics = metrics_df[metrics_df['SERIES'] == series]
                self.display(f"Metrics for {series}", content_type="text")
                self.display(series_metrics, content_type="dataframe")
        else:
            self.display(metrics_df, content_type="dataframe")

    def create_feature_importance_chart(self, df, series=None):
        title = f"Feature Importance Plot{' for ' + series if series else ''}"
        return alt.Chart(df).mark_bar().encode(
            x=alt.X('SCORE:Q', title='Feature Importance'),
            y=alt.Y('FEATURE_NAME:N', title='Feature', sort='-x')
        ).properties(
            title=title,
            width=600,
            height=300
        )

    def display(self, content, content_type="text", **kwargs):
        if self.is_streamlit:
            import streamlit as st
            if content_type == "text":
                st.write(content)
            elif content_type == "chart":
                st.altair_chart(content, use_container_width=True)
            elif content_type == "dataframe":
                st.dataframe(content)
            elif content_type == "code":
                st.code(content, language=kwargs.get('language', ''))
        else:
            if content_type == "text":
                print(content)
            elif content_type == "chart":
                from IPython.display import display
                display(content)
            elif content_type == "dataframe":
                from IPython.display import display
                display(content)
            elif content_type == "code":
                print(content)
