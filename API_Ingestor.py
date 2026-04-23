
import requests
import ast
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace, current_timestamp, lit, make_date, to_date
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class Ingestor():
    ''' A metadata-driven framework for ingesting and transforming CMS API data. '''
    def __init__(self):
        self.api_url = ''

        print(f'Ingestor initialized')

    # -------------------------------------------
    # COLUMN MAPPING MANIPULATIONS

    def _create_column_mapping( self, spark: SparkSession, df: DataFrame, filename: str) -> DataFrame:
        ''' 
            Creates a column mapping csv file listing the columns in the provided DataFrame and includes the following empty columns:
            - Table: Indicates if the column is to be moved to a separate dimension table: If empty, column remains in the main fact table.
            - New Name: User-friendly name for column to be used in transformed tables. If empty, column is removed from transformed tables.
            - Data Type: Indicates the type of data in the column, which can be used to drive specific transformations and quality checks. Expected values include 'Key', 'Text', 'Metric', and 'Boolean'.
            
            This serves as a template for users to fill in with the appropriate transformations and metadata.
        '''
        column_mapping = []

        # create mapping template
        for c in df.columns:
            column_mapping.append({
                "column_name"       : c,
                "table"             : '', 
                "new_name"          : '', 
                "data_type"         : '',
                "intended_format"   : ''
            })
        df_mapping = spark.createDataFrame(column_mapping)
        
        # Save the mapping template to a CSV file for user input
        (df_mapping
            .coalesce(1).write
            .format(    "csv")
            .option(    "header", "true")
            .mode(      "overwrite")
            .save(      filename)
        )
        print(f"{datetime.now().date()} | Column mapping template created at: \n{filename}")

        return df_mapping

    def _get_column_mapping(self, spark: SparkSession, table_name: str) -> dict:
        ''' Reads the column mapping from a user-maintained CSV file and returns it as a dictionary. '''
        mapping_dict = {}
        
        # Read the mapping file for the given table name
        df_mapping = (spark.read
            .format(    "csv")
            .option(    "header", "true")
            .load(      f"/Files/column_mapping/{table_name}.csv")
        )

        # Convert the DataFrame to a dictionary for easy access during transformations
        for row in df_mapping.collect():
            mapping_dict[row['column_name']] = (
                row['table'], 
                row['new_name'], 
                row['data_type'],
                row['intended_format']
            )

        return mapping_dict

    # -------------------------------------------
    # TRANSFORMATIONS
    
    def _transform_date_columns(self, 
        df              : DataFrame, 
        column_mapping  : dict
    ) -> DataFrame:
        ''' Standardizes date strings into Spark DateType.'''

        # Identify columns marked as 'Date' in the mapping
        date_cols = [(col_name, col_info[3])
            for col_name, col_info 
            in column_mapping.items() 
            if col_info[2] == 'Date'
        ]

        # Transform identified date columns using the specified format
        for d, date_format in date_cols:
            if date_format == '': date_format = "yyyy-MM-dd" # Default format if not specified
            df = df.withColumn(d, to_date(col(d), date_format))

        return df

    def _transform_currency_columns(self, 
        df              : DataFrame, 
        column_mapping  : dict
    ) -> DataFrame:
        ''' Transforms currency columns by removing symbols and converting to decimal. '''

        # Identify columns marked as 'Currency' in the mapping
        currency_cols = [
            (col_name, col_info[3])
            for col_name, col_info 
            in column_mapping.items() 
            if col_info[2] == 'Currency'
        ]

        # Transform identified currency columns by 
        # removing common symbols and 
        # casting to the specified decimal type
        for c, currency_format in currency_cols:
            if currency_format == '': currency_format = "decimal(18,2)" # Default format if not specified
            df = df.withColumn(c, regexp_replace(col(c), "[$,]", "").cast(currency_format))

        return df
    
    def _transform_int_columns(self, 
        df              : DataFrame, 
        column_mapping  : dict
    ) -> DataFrame:
        ''' Transforms integer columns by casting to int type. '''
        
        # Identify columns marked as 'Integer' in the mapping
        int_cols = [
            (col_name, col_info[3])
            for col_name, col_info 
            in column_mapping.items() 
            if col_info[2] == 'Integer'
        ]

        # Transform identified integer columns by casting to int type
        for i, int_format in int_cols:
            if int_format == '': int_format = "int" # Default format if not specified
            df = df.withColumn(i, col(i).cast(int_format))
        return df
    
    def _transform_boolean_columns(self, 
        df: DataFrame, 
        column_mapping: dict
    ) -> DataFrame:
        """
        Standardizes string/int flags into true Booleans.
        Example mapping: {"is_drug_indicator": {"Y": True, "N": False}}
        """

        # Identify columns marked as 'Boolean' in the mapping
        bool_cols = [
            (col_name, col_info[3]) 
            for col_name, col_info
            in column_mapping.items()
            if col_info[2] == 'Boolean'
        ]

        # For each boolean column, apply the corresponding mapping to convert values to true Booleans
        for col_name, bool_format in bool_cols:
            actual_dict = ast.literal_eval(bool_format)
            mapping_expr = F.create_map([F.lit(x) for x in sum(actual_dict.items(), ())])
            df = df.withColumn(col_name, mapping_expr.getItem(F.col(col_name)))
        
        return df

    def _fill_null_key_columns(self, 
        df              : DataFrame, 
        column_mapping  : dict,
        default_val     : str = "Unknown"
    ) -> DataFrame:
        ''' Replaces NULLs in key columns to prevent Power BI 'Blank' values. '''
        # Identify columns marked as 'Text' or 'Key' in the mapping
        subset_cols = [
            col_name 
            for col_name, col_info 
            in column_mapping.items() 
            if col_info[2] in ['Text', 'Key']
        ]

        # Fill NULLs in the identified columns with the specified default value
        df = df.fillna(default_val, subset=subset_cols)
        return df
    
    def add_surrogate_key(self, 
        df                  : DataFrame, 
        primary_key_cols    : list
    ) -> DataFrame:
        ''' Adds a surrogate key column named "ID" by hashing the concatenated values of the specified business key columns. '''
        
        # confirm that all primary key columns exist in the DataFrame
        for col_name in primary_key_cols:
            if col_name not in df.columns:
                raise ValueError(f"Primary key column '{col_name}' not found in DataFrame columns: {df.columns}")

        # Drop duplicates based on the specified primary key columns and 
        # create a new "ID" column by hashing the concatenated values of those columns
        df = (df
            .dropDuplicates(primary_key_cols)
            .withColumn(
                "ID", 
                F.sha2(F.concat_ws("||", *[F.col(c) for c in primary_key_cols]), 256)
            ))
        
        return df

    # -------------------------------------------
    # TABLE OPERATIONS

    def fetch_data(self,
        spark       : SparkSession, 
        url         : str, 
        batch_size  : int, 
        total_limit : int = None
    ) -> DataFrame:
        ''' Fetches data from the API, handling pagination if necessary, and saves to a Delta table. 
        '''
        all_data    = []
        offset      = 0

        # If both batch_size and total_limit are provided, fetch in batches until total_limit is reached.
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
        
        # If only one of the parameters is provided, print a warning and skip batching to avoid partial batch runs.
        elif (
            batch_size is not None or
            total_limit is not None
        ): 
            print(f'''
            One of two batch parameters provided. To run a batch process, please ensure both are provided:
                  batch_size  = {batch_size}
                  total_limit = {total_limit}
            ''')

        # If neither parameter is provided, fetch all data in a single request (not recommended for large datasets).
        else:
            response = requests.get(url)
            if response.status_code == 200:
                all_data = response.json()

        # Convert the list of dictionaries to a Spark DataFrame
        df = spark.createDataFrame(all_data)
        print(f"{datetime.now().date()} | Rows fetched: {len(all_data)}")

        return df
    
    def save_to_table(self, 
        spark       : SparkSession, 
        df          : DataFrame, 
        table_name  : str,
        source_url  : str = None,
        generate_column_mapping : bool = False
    ):
        ''' Saves the DataFrame to a Delta table. '''

        # Save to Delta Lake
        (df.write
            .format(    "delta")
            .option(    "overwriteSchema", "true")
            .mode(      "overwrite")
            .saveAsTable(table_name)
        )
        print( f"{datetime.now().date()} | Table saved: {table_name}" )

        # Generate column mapping template
        if generate_column_mapping:
            self._create_column_mapping(spark, df, table_name)
            print( f"{datetime.now().date()} | Column mapping template generated for {table_name}" )

        # Update audit log
        if spark != None and source_url != None: 
            self.log_update_tables(spark, table_name, source_url, df.count())
            print( f"{datetime.now().date()} | Audit log updated for {table_name}" )

        return df
    
    def apply_column_mapping(self, 
        spark                       : SparkSession, 
        df                          : DataFrame, 
        table_name                  : str,        
        transform_date_columns      : bool = True,
        transform_currency_columns  : bool = True,
        transform_int_columns       : bool = True,
        transform_boolean_columns   : bool = True,
        transform_key_columns       : bool = True,
    ) -> DataFrame:
        ''' Rename columns in the bronze table based on the provided mapping. '''

        # Get the column mapping for the current table
        column_mapping = self._get_column_mapping(spark, table_name)

        # Select and rename columns that are present in the mapping and have a non-empty new name
        df = df.select(
            [col(old).alias(new[1]) 
                for old, new 
                in column_mapping.items()
                if new[1] != '' and old in df.columns
            ])

        # Apply column transformations if specified in the mapping
        if transform_date_columns:      df = self._transform_date_columns(      df, column_mapping)
        if transform_currency_columns:  df = self._transform_currency_columns(  df, column_mapping)
        if transform_int_columns:       df = self._transform_int_columns(       df, column_mapping)
        if transform_boolean_columns:   df = self._transform_boolean_columns(   df, column_mapping)
        if transform_key_columns:       df = self._fill_null_key_columns(       df, column_mapping)

        return df
    
    def log_update_tables(self, 
        spark       : SparkSession, 
        table_name  : str, 
        source      : str, 
        row_count   : int
    ):
        """
        Records a single entry into a centralized audit log table.
        Ensures traceability without bloating individual data rows.
        """
        # Create the base data with standard Python types
        log_data = [{
            "table_name"            : table_name,
            "source_system"         : source,
            "row_count"             : row_count
        }]
        
        # 2. Create the DataFrame
        df_log = (
            spark.createDataFrame(log_data)
            .withColumn("load_timestamp", current_timestamp())
        )
        df_log.write.format("delta").mode("append").saveAsTable("z_model_tables")
        
        print(f"{datetime.now().date()} | Audit log updated for {table_name}")

#------------------------------------------------------------------------------