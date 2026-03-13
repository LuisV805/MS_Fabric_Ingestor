
import requests
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace, current_timestamp, lit, make_date, to_date
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class Ingestor():
    ''' A metadata-driven framework for ingesting and transforming CMS API data. '''
    def __init__(self):
        super.__init__()

    # -------------------------------------------

    def fetch_data(self, spark: SparkSession,  url: str, table_name: str, batch_size: int, total_limit: int = None) -> DataFrame:
        ''' Fetches data from the API, handling pagination if necessary, and saves to a Delta table. '''
        all_data = []
        offset = 0

        if (
            batch_size is not None and 
            total_limit is not None
        ): 
            while len(all_data) < total_limit:
                params = {'size': batch_size, 'offset': offset}
                response = requests.get(url, params=params)
                if response.status_code != 200: break
                data = response.json()
                if not data: break
                all_data.extend(data)
                offset += batch_size
                print(f"Rows fetched: {len(all_data)}")
        else:
            response = requests.get(url)
            if response.status_code == 200:
                all_data = response.json()

        df = spark.createDataFrame(all_data)
        return df
    
    # -------------------------------------------
    # TABLE TRANSFORMATIONS

    def add_audit_metadata(self, df: DataFrame, source_url: str) -> DataFrame:
        ''' Adds traceability: useful for Data Governance and troubleshooting. '''
        return (df.withColumn("load_timestamp", current_timestamp())
            .withColumn("source_system", lit(source_url)))

    def handle_duplicates(self, df: DataFrame, pk_cols: list) -> DataFrame:
        ''' Drops duplicates based on a list of Primary Key columns. '''
        return df.dropDuplicates(pk_cols)

    def update_column_names(self, bronze_table: DataFrame, column_mapping: dict) -> DataFrame:
        ''' Rename columns in the bronze table based on the provided mapping. '''
        df = bronze_table.select(
            [col(old).alias(new) 
                for old, new 
                in column_mapping.items()
            ]
        )
        return df
    
    # -------------------------------------------
    # DATA TYPE TRANSFORMATIONS
    
    def transform_date_columns(self, df: DataFrame, date_cols: list, date_format: str = "yyyy-MM-dd") -> DataFrame:
        ''' Standardizes date strings into Spark DateType.'''
        for d in date_cols:
            df = df.withColumn(d, to_date(col(d), date_format))
        return df

    def transform_currency_columns(self, df: DataFrame, currency_cols: list) -> DataFrame:
        ''' Transforms currency columns by removing symbols and converting to decimal. '''
        for c in currency_cols:
            df = df.withColumn(c, regexp_replace(col(c), "[$,]", "").cast("decimal(18,2)"))
        return df
    
    def transform_int_columns(self, df: DataFrame, int_cols: list) -> DataFrame:
        ''' Transforms integer columns by casting to int type. '''
        for i in int_cols:
            df = df.withColumn(i, col(i).cast("int"))
        return df
    
    def transform_boolean_columns(self, df: DataFrame, bool_mapping: dict) -> DataFrame:
        """
        Standardizes string/int flags into true Booleans.
        Example mapping: {"is_drug_indicator": {"Y": True, "N": False}}
        """
        for col_name, mapping in bool_mapping.items():
            # Use the Spark 'create_map' and 'lit' functions to transform
            mapping_expr = F.create_map([F.lit(x) for x in sum(mapping.items(), ())])
            df = df.withColumn(col_name, mapping_expr.getItem(F.col(col_name)))
        
        return df

    # -------------------------------------------
    # DATA QUALITY
    
    def flag_outliers(self, df: DataFrame, metric_cols: list, group_cols: list = None) -> DataFrame:
        """
        Identifies outliers using the IQR method. 
        Optional: group_cols allows you to find outliers within a specific group 
        (e.g., outliers for a specific HCPCS code rather than the whole dataset).
        """
        # Define the window (global or grouped)
        window = Window.partitionBy(*group_cols) if group_cols else Window.orderBy(F.lit(1))

        # Calculate Quartiles
        for metric_col in metric_cols:
            df_stats = (df
                .withColumn("q1", F.percentile_approx(metric_col, 0.25).over(window))
                .withColumn("q3", F.percentile_approx(metric_col, 0.75).over(window))
            )

            # Calculate IQR and Bounds
            df_stats = df_stats.withColumn("iqr", col("q3") - col("q1"))
            df_stats = df_stats.withColumn("lower_bound", col("q1") - (col("iqr") * 1.5))
            df_stats = df_stats.withColumn("upper_bound", col("q3") + (col("iqr") * 1.5))

            # Add the flag
            df_stats.withColumn(
                f"is_outlier_{metric_col}",
                (col(metric_col) < col("lower_bound")) | (col(metric_col) > col("upper_bound"))
            ).drop("q1", "q3", "iqr", "lower_bound", "upper_bound")
        
        return df_stats

    def flag_date_outliers(self, df: DataFrame, date_cols: list, min_year: int = 1965, max_year: int = 2030) -> DataFrame:
        """
        Flags dates that are logically or contextually 'impossible'.
        Useful for catching '20224' instead of '2024' or '1900' default values.
        """
        for date_col in date_cols:
            df = df.withColumn(
                f"is__typo{date_col}",
                (F.year(col(date_col)) < min_year) | 
                (F.year(col(date_col)) > max_year) |
                (col(date_col).isNull()) # Catches 'Feb 30th' which Spark casts to Null automatically
            )
            
        return df

    def flag_range_violations(self, df: DataFrame, col_names: list, min_val: float = None, max_val: float = None) -> DataFrame:
        """
        Flags records that violate a fixed business range. 
        Useful for 'impossible' values that aren't necessarily statistical outliers.
        """
        for col_name in col_names:
            conditions = []
            if min_val is not None:
                conditions.append(F.col(col_name) < min_val)
            if max_val is not None:
                conditions.append(F.col(col_name) > max_val)
            
            # If no bounds provided, return original DF
            if not conditions:
                return df

            # Create a single boolean flag for any violation
            violation_expr = conditions[0]
            for cond in conditions[1:]:
                violation_expr = violation_expr | cond
                
            df = df.withColumn(f"is_{col_name}_range_violation", violation_expr)
        
        return df

    # -------------------------------------------
    # NULL VALUES
    
    def generate_completeness_report(self, df: DataFrame) -> None:
        """
        Calculates the percentage of NULL values for every column.
        Returns a dictionary: {column_name: null_percentage}
        """
        total_rows = df.count()
        if total_rows == 0:
            return {"Error": "DataFrame is empty"}

        # Calculate null counts for all columns in a single pass
        null_counts = df.select([
            F.count(F.when(F.col(c).isNull() | F.nanvl(F.col(c), F.lit(None)).isNull(), c)).alias(c)
            for c in df.columns
        ]).collect()[0].asDict()

        # Convert counts to percentages
        completeness_report = {
            col: round((count / total_rows) * 100, 2) 
            for col, count in null_counts.items()
        }
        
        print("--- Data Completeness Report (% Null) ---")
        for col, pct in completeness_report.items():
            print(f"{col}: {pct}%")
            
        return completeness_report
    
    def fill_nulls(self, df: DataFrame, subset_cols: list, default_val: str = "Unknown") -> DataFrame:
        ''' Replaces NULLs in dimensions to prevent Power BI 'Blank' values. '''
        return df.fillna(default_val, subset=subset_cols)
    
    # -------------------------------------------
    
    def save_to_table(self, df: DataFrame, table_name: str):
        ''' Saves the DataFrame to a Delta table. '''
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print( f"Table saved: {table_name}" )
        return df
