# SQL Server Change Tracking Ingestion to Databricks
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# ============================================================================
# CONFIGURATION
# ============================================================================

# SQL Server Configuration for JDBC
SQL_SERVER = "SERVERNAME"
SQL_PORT = "1433"
SQL_DATABASE = "WorldEconomicData"
SQL_USERNAME = "DatabricksUser"
SQL_PASSWORD = "password"  # Consider using Databricks secrets instead
SQL_TABLE = "dbo.world_economic_data_r02"
SQL_TABLE_SCHEMA = "dbo"
SQL_TABLE_NAME = "world_economic_data_r02"

# Primary key columns (for composite keys, list all columns)
PRIMARY_KEY_COLUMNS = ["iso_code", "year"]

# Databricks Configuration
DATABRICKS_CATALOG = "workspace"
DATABRICKS_SCHEMA = "default"
DATABRICKS_TABLE = "world_economic_data_r02_dbx"
SYNC_VERSION_TABLE = "sync_versions"  # Table to track last sync version

# JDBC Connection Properties
JDBC_URL = f"jdbc:sqlserver://{SQL_SERVER}:{SQL_PORT};databaseName={SQL_DATABASE};encrypt=true;trustServerCertificate=true"
JDBC_PROPERTIES = {
    "user": SQL_USERNAME,
    "password": SQL_PASSWORD,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# ============================================================================
# FUNCTIONS
# ============================================================================

def get_last_sync_version(spark):
    """
    Retrieve the last synchronized change tracking version.
    Returns 0 if no previous sync exists.
    """
    try:
        sync_table = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{SYNC_VERSION_TABLE}"
        
        if spark.catalog.tableExists(sync_table):
            result = spark.sql(f"""
                SELECT sync_version 
                FROM {sync_table} 
                WHERE table_name = '{SQL_TABLE}'
                ORDER BY sync_timestamp DESC 
                LIMIT 1
            """).collect()
            
            if result:
                return result[0]['sync_version']
        
        return 0
    except Exception as e:
        print(f"Warning: Could not retrieve last sync version: {e}")
        return 0


def save_sync_version(spark, version):
    """Save the current sync version to tracking table"""
    sync_table = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{SYNC_VERSION_TABLE}"
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {sync_table} (
            table_name STRING,
            sync_version BIGINT,
            sync_timestamp TIMESTAMP,
            records_processed INT
        )
    """)
    
    spark.sql(f"""
        INSERT INTO {sync_table}
        VALUES ('{SQL_TABLE}', {version}, current_timestamp(), 0)
    """)


def get_changed_records_jdbc(spark, last_sync_version):
    """
    Query SQL Server change tracking to get changed records using JDBC.
    Returns a DataFrame of changed records and the current version.
    """
    # Get current change tracking version
    current_version_df = spark.read.jdbc(
        url=JDBC_URL,
        table="(SELECT CHANGE_TRACKING_CURRENT_VERSION() AS current_version) AS cv",
        properties=JDBC_PROPERTIES
    )
    current_version = current_version_df.collect()[0]['current_version']
    
    if current_version is None:
        raise Exception("Change tracking is not enabled on the database")
    
    print(f"Last sync version: {last_sync_version}")
    print(f"Current version: {current_version}")
    
    # Build column list for primary key columns
    pk_columns = ", ".join([f"ct.{col}" for col in PRIMARY_KEY_COLUMNS])
    
    # Query change tracking for modified records
    query = f"(SELECT {pk_columns}, ct.SYS_CHANGE_OPERATION FROM CHANGETABLE(CHANGES {SQL_TABLE}, {last_sync_version}) AS ct) AS changes"
    changed_records_df = spark.read.jdbc(
        url=JDBC_URL,
        table=query,
        properties=JDBC_PROPERTIES
    )
    
    record_count = changed_records_df.count()
    print(f"Found {record_count} changed records")
    
    return changed_records_df, current_version


def fetch_full_records_jdbc(spark, changed_records_df):
    """
    Fetch complete records from SQL Server for the given IDs using JDBC.
    Returns a DataFrame of full records.
    """
    if changed_records_df is None or changed_records_df.count() == 0:
        return None
    
    # Filter out deleted records and collect the primary key values
    changed_rows = [row for row in changed_records_df.collect() if row['SYS_CHANGE_OPERATION'] != 'D']
    if not changed_rows:
        return None
    
    # Build WHERE clause for composite primary key
    # For each row, create a condition like: (iso_code = 'USA' AND year = 2020)
    conditions = []
    for row in changed_rows:
        condition_parts = []
        for col in PRIMARY_KEY_COLUMNS:
            value = row[col]
            # Handle string vs numeric values
            if isinstance(value, str):
                condition_parts.append(f"{col} = '{value}'")
            else:
                condition_parts.append(f"{col} = {value}")
        conditions.append(f"({' AND '.join(condition_parts)})")
    
    where_clause = " OR ".join(conditions)
    query = f"(SELECT * FROM {SQL_TABLE} WHERE {where_clause}) AS full_records"
    
    records_df = spark.read.jdbc(
        url=JDBC_URL,
        table=query,
        properties=JDBC_PROPERTIES
    )
    return records_df


def ingest_to_databricks(spark, records_df):
    """
    Ingest records into Databricks Delta table using merge operation.
    """
    if records_df is None or records_df.count() == 0:
        print("No records to ingest")
        return 0
    
    target_table = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{DATABRICKS_TABLE}"
    records_df = records_df.withColumn("_ingestion_timestamp", current_timestamp())
    
    if not spark.catalog.tableExists(target_table):
        records_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        print(f"Created new table {target_table} with {records_df.count()} records")
        return records_df.count()
    
    records_df.createOrReplaceTempView("updates")
    
    # Build merge condition for composite primary key
    merge_conditions = " AND ".join([f"target.{col} = source.{col}" for col in PRIMARY_KEY_COLUMNS])
    
    merge_query = f"""
        MERGE INTO {target_table} AS target
        USING updates AS source
        ON {merge_conditions}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """
    
    spark.sql(merge_query)
    record_count = records_df.count()
    print(f"Merged {record_count} records into {target_table}")
    
    return record_count


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function"""
    print(f"Starting change tracking ingestion: {datetime.datetime.now()}")
    
    # Get spark session from global scope (available in Databricks REPL)
    try:
        spark_session = spark
    except NameError:
        # If global spark doesn't exist, try to get active session
        spark_session = SparkSession.getActiveSession()
        if spark_session is None:
            raise RuntimeError(
                "No Spark session available. This script requires a Databricks environment "
                "with an active Spark session. Please run this in a Databricks notebook or "
                "ensure the 'spark' variable is available."
            )
    
    try:
        last_sync_version = get_last_sync_version(spark_session)
        
        # Get changed record IDs from change tracking using JDBC
        changed_records_df, current_version = get_changed_records_jdbc(spark_session, last_sync_version)
        
        if changed_records_df.count() > 0:
            # Fetch full records for changed IDs using JDBC
            records_df = fetch_full_records_jdbc(spark_session, changed_records_df)
            
            # Ingest to Databricks
            record_count = ingest_to_databricks(spark_session, records_df)
            
            # Update sync version
            save_sync_version(spark_session, current_version)
            print(f"Successfully processed {record_count} records")
        else:
            print("No changes detected since last sync")
            save_sync_version(spark_session, current_version)
        
        print(f"Completed: {datetime.datetime.now()}")
        
    except Exception as e:
        print(f"Error during ingestion: {e}")
        raise


if __name__ == "__main__":
    main()
