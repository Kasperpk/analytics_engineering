# Databricks notebook source
# MAGIC %md
# MAGIC # Example: Using Dataplatform Libraries
# MAGIC 
# MAGIC This notebook demonstrates how to use the dataplatform libraries for:
# MAGIC - Environment-aware path configuration
# MAGIC - Reading and writing data
# MAGIC - Performing transformations
# MAGIC 
# MAGIC The code automatically detects the environment (dev/test/prod) from the workspace URL
# MAGIC and configures paths accordingly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

# Add libraries to path
import sys
sys.path.append('/Workspace/Repos/<your-repo>/analytics_engineering/dataplatform/libraries')

from environment_config import EnvironmentConfig, get_config
from data_operations import DataOperations
from transformation_utils import TransformationUtils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Environment Configuration
# MAGIC 
# MAGIC The EnvironmentConfig automatically detects which environment you're in based on the workspace URL

# COMMAND ----------

# Initialize environment configuration from Spark
config = get_config(spark)

# Display environment information
print(f"Environment: {config.environment}")
print(f"Root Path: {config.root_path}")
print(f"\nAvailable paths:")
for layer, path in config.paths.items():
    print(f"  {layer}: {path}")

print(f"\nAvailable catalogs:")
for layer, catalog in config.catalogs.items():
    print(f"  {layer}: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1: Bronze Layer - Reading Raw Data
# MAGIC 
# MAGIC Read data from bronze layer using environment-aware paths

# COMMAND ----------

# Initialize data operations
data_ops = DataOperations(spark)

# Get bronze path for a specific schema and table
bronze_path = config.get_path('bronze', 'aarsleff', 'customers')
print(f"Reading from: {bronze_path}")

# Or use Unity Catalog table name
bronze_table = config.get_full_table_name('bronze', 'aarsleff', 'customers')
print(f"Table name: {bronze_table}")

# Read data (uncomment when table exists)
# df_bronze = data_ops.read_delta(table_name=bronze_table)
# display(df_bronze.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 2: Transforming Data
# MAGIC 
# MAGIC Apply transformations using TransformationUtils

# COMMAND ----------

# Example: Create sample data for demonstration
sample_data = [
    (1, "John Doe", "  customer@email.com  ", "2024-01-15", 1000),
    (2, "Jane Smith", "another@EMAIL.COM", "2024-02-20", 2500),
    (1, "John Doe", "  customer@email.com  ", "2024-03-10", 1500),  # duplicate
]

df = spark.createDataFrame(sample_data, 
                          ["customer_id", "name", "email", "order_date", "amount"])

print("Original data:")
display(df)

# COMMAND ----------

# Apply transformations
transform_utils = TransformationUtils()

# 1. Clean string columns (trim spaces, standardize)
df_clean = transform_utils.clean_string_columns(
    df, 
    columns=['email'],
    trim_spaces=True
)

# 2. Standardize column names
df_clean = transform_utils.standardize_column_names(df_clean, case='snake')

# 3. Parse dates
df_clean = transform_utils.parse_dates(
    df_clean,
    date_columns={'order_date': 'yyyy-MM-dd'}
)

# 4. Add date dimensions
df_clean = transform_utils.add_date_dimensions(
    df_clean,
    date_column='order_date',
    prefix='order_'
)

# 5. Add surrogate key
df_clean = transform_utils.add_surrogate_key(
    df_clean,
    key_columns=['customer_id'],
    key_name='customer_sk'
)

# 6. Deduplicate
df_clean = data_ops.deduplicate(
    df_clean,
    partition_cols=['customer_id'],
    order_col='order_date'
)

# 7. Add metadata columns
df_clean = data_ops.add_metadata_columns(
    df_clean,
    source_system='AARSLEFF_ERP'
)

print("Transformed data:")
display(df_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 3: Writing to Silver Layer

# COMMAND ----------

# Get silver layer path
silver_table = config.get_full_table_name('silver', 'aarsleff', 'customers')
print(f"Writing to: {silver_table}")

# Write data to silver layer (uncomment to execute)
# data_ops.write_delta(
#     df=df_clean,
#     table_name=silver_table,
#     mode='overwrite',
#     partition_by=['order_year', 'order_month'],
#     optimize=True,
#     z_order_by=['customer_id']
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 4: Incremental Load Pattern

# COMMAND ----------

# Simulating incremental load with merge
# In real scenario, you would filter source data based on watermark

# Get existing data
# existing_df = data_ops.read_delta(table_name=silver_table)

# Get new/updated records (filtered by watermark date)
# new_df = ... your source data filtered by date ...

# Perform merge (upsert)
# data_ops.merge_delta(
#     source_df=new_df,
#     target_table=silver_table,
#     merge_keys=['customer_id']
# )

# Or use incremental_load helper
# data_ops.incremental_load(
#     source_df=new_df,
#     target_table=silver_table,
#     merge_keys=['customer_id'],
#     date_column='order_date',
#     watermark_table=config.get_full_table_name('metadata', 'control', 'watermarks')
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 5: Environment-Specific Logic

# COMMAND ----------

# Execute environment-specific logic
if config.is_production():
    print("Running in PRODUCTION - using production configurations")
    # Add production-specific logic
    
elif config.is_test():
    print("Running in TEST - using test configurations")
    # Add test-specific logic
    
else:  # development
    print("Running in DEVELOPMENT - using dev configurations")
    # Add dev-specific logic like sampling
    # df_clean = df_clean.sample(0.1)  # 10% sample in dev

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 6: Reading from Different Layers

# COMMAND ----------

# Read from bronze
# bronze_df = data_ops.read_delta(
#     table_name=config.get_full_table_name('bronze', 'aarsleff', 'raw_customers')
# )

# Read from silver
# silver_df = data_ops.read_delta(
#     table_name=config.get_full_table_name('silver', 'aarsleff', 'customers')
# )

# Read from gold
# gold_df = data_ops.read_delta(
#     table_name=config.get_full_table_name('gold', 'reporting', 'customer_metrics')
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC This notebook demonstrated:
# MAGIC 
# MAGIC 1. **Environment Detection**: Automatic detection of dev/test/prod from workspace URL
# MAGIC 2. **Path Management**: Environment-aware paths for data lake and Unity Catalog
# MAGIC 3. **Data Operations**: Reading, writing, merging with Delta Lake
# MAGIC 4. **Transformations**: Common data transformation patterns
# MAGIC 5. **Metadata**: Adding audit columns and tracking
# MAGIC 6. **Optimization**: Table optimization and Z-ordering
# MAGIC 
# MAGIC The libraries enable you to write code once and deploy across all environments without changes.
