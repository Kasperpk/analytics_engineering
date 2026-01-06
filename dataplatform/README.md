# Enterprise Data Platform - Databricks Solution

This repository contains the core libraries and structure for an enterprise data platform built on Azure Databricks following the medallion architecture (Bronze, Silver, Gold).

## Architecture Overview

The platform follows a metadata-driven, environment-aware architecture with the following components:

```
┌─────────────────┐
│  Source Systems │
│  (SQL, APIs,    │
│   Files, etc.)  │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│               Azure Data Lake Gen2                       │
│  ┌────────┐  ┌────────┐  ┌────────┐  ┌──────────────┐ │
│  │ Bronze │─▶│ Silver │─▶│  Gold  │  │   Metadata   │ │
│  │  (Raw) │  │(Clean) │  │(Curated│  │   & Shared   │ │
│  └────────┘  └────────┘  └────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────┐
│   Databricks    │
│  Unity Catalog  │
│   + Notebooks   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Power BI /     │
│  Tabular Editor │
│  (Consumption)  │
└─────────────────┘
```

## Folder Structure

```
dataplatform/
├── libraries/                      # Core Python libraries
│   ├── __init__.py                # Package initialization
│   ├── environment_config.py      # Environment detection and path management
│   ├── data_operations.py         # Data read/write operations
│   └── transformation_utils.py    # Common transformation functions
│
├── notebooks/                     # Databricks notebooks
│   ├── bronze/                    # Raw data ingestion notebooks
│   ├── silver/                    # Data cleansing notebooks
│   ├── gold/                      # Business logic notebooks
│   ├── metadata/                  # Metadata management notebooks
│   └── example_usage.py           # Example notebook showing library usage
│
├── config/                        # Configuration files
│   ├── environments.yaml          # Environment-specific configurations (planned)
│   └── pipeline_config.yaml       # Pipeline configurations (planned)
│
└── utilities/                     # Helper scripts and tools
```

## Key Features

### 1. Environment-Aware Configuration

The `environment_config.py` module automatically detects the environment (dev, test, prod) from the Databricks workspace URL and provides appropriate paths:

```python
from environment_config import get_config

# Initialize from Spark session
config = get_config(spark)

# Automatically detects environment
print(config.environment)  # 'dev', 'test', or 'prod'

# Get environment-specific paths
bronze_path = config.get_path('bronze', 'aarsleff', 'customers')
# Returns: abfss://dataplatform@storage.../dev/bronze/aarsleff/customers

# Get Unity Catalog table names
table_name = config.get_full_table_name('silver', 'aarsleff', 'customers')
# Returns: dev_silver.aarsleff.customers
```

**Key Methods:**
- `get_path(layer, *subdirs)` - Get data lake path for any layer
- `get_catalog(layer)` - Get Unity Catalog name
- `get_full_table_name(layer, schema, table)` - Get full table name
- `is_production()`, `is_test()`, `is_development()` - Environment checks

### 2. Data Operations

The `data_operations.py` module provides common data operations:

```python
from data_operations import DataOperations

ops = DataOperations(spark)

# Read data
df = ops.read_delta(table_name='dev_bronze.aarsleff.customers')

# Write data with optimization
ops.write_delta(
    df=df,
    table_name='dev_silver.aarsleff.customers',
    mode='overwrite',
    partition_by=['year', 'month'],
    optimize=True,
    z_order_by=['customer_id']
)

# Merge (upsert) data
ops.merge_delta(
    source_df=new_df,
    target_table='dev_silver.aarsleff.customers',
    merge_keys=['customer_id']
)

# Incremental load with watermark
ops.incremental_load(
    source_df=incremental_df,
    target_table='dev_silver.aarsleff.orders',
    merge_keys=['order_id'],
    date_column='order_date',
    watermark_table='dev_metadata.control.watermarks'
)

# Deduplicate data
clean_df = ops.deduplicate(
    df=df,
    partition_cols=['customer_id'],
    order_col='modified_date'
)
```

### 3. Transformation Utilities

The `transformation_utils.py` module provides common transformations:

```python
from transformation_utils import TransformationUtils

utils = TransformationUtils()

# Standardize column names
df = utils.standardize_column_names(df, case='snake')

# Clean string columns
df = utils.clean_string_columns(df, trim_spaces=True)

# Add surrogate keys
df = utils.add_surrogate_key(
    df=df,
    key_columns=['customer_id', 'country'],
    key_name='customer_sk'
)

# Parse dates
df = utils.parse_dates(df, date_columns={'order_date': 'yyyy-MM-dd'})

# Add date dimensions
df = utils.add_date_dimensions(df, date_column='order_date', prefix='order_')

# Cast columns
df = utils.cast_columns(df, {'age': 'int', 'salary': 'double'})
```

## Usage Patterns

### Bronze Layer (Raw Data Ingestion)

```python
# Bronze notebook example
import sys
sys.path.append('/Workspace/Repos/<your-repo>/analytics_engineering/dataplatform/libraries')

from environment_config import get_config
from data_operations import DataOperations

# Initialize
config = get_config(spark)
ops = DataOperations(spark)

# Read from source
source_df = spark.read.jdbc(url=jdbc_url, table=source_table, properties=props)

# Add metadata
source_df = ops.add_metadata_columns(source_df, source_system='AARSLEFF_ERP')

# Write to bronze
ops.write_delta(
    df=source_df,
    table_name=config.get_full_table_name('bronze', 'aarsleff', 'raw_customers'),
    mode='append'
)
```

### Silver Layer (Data Cleansing)

```python
# Silver notebook example
from environment_config import get_config
from data_operations import DataOperations
from transformation_utils import TransformationUtils

config = get_config(spark)
ops = DataOperations(spark)
utils = TransformationUtils()

# Read from bronze
bronze_df = ops.read_delta(
    table_name=config.get_full_table_name('bronze', 'aarsleff', 'raw_customers')
)

# Transform
silver_df = (bronze_df
    .transform(lambda df: utils.standardize_column_names(df))
    .transform(lambda df: utils.clean_string_columns(df))
    .transform(lambda df: ops.deduplicate(df, ['customer_id'], 'modified_date'))
)

# Write to silver with merge
ops.merge_delta(
    source_df=silver_df,
    target_table=config.get_full_table_name('silver', 'aarsleff', 'customers'),
    merge_keys=['customer_id']
)
```

### Gold Layer (Business Logic)

```python
# Gold notebook example
from environment_config import get_config
from data_operations import DataOperations

config = get_config(spark)
ops = DataOperations(spark)

# Read from silver
customers_df = ops.read_delta(
    table_name=config.get_full_table_name('silver', 'aarsleff', 'customers')
)
orders_df = ops.read_delta(
    table_name=config.get_full_table_name('silver', 'aarsleff', 'orders')
)

# Business logic - customer metrics
gold_df = (
    orders_df
    .groupBy('customer_id')
    .agg(
        count('order_id').alias('total_orders'),
        sum('amount').alias('total_revenue'),
        max('order_date').alias('last_order_date')
    )
    .join(customers_df, 'customer_id')
)

# Write to gold
ops.write_delta(
    df=gold_df,
    table_name=config.get_full_table_name('gold', 'reporting', 'customer_metrics'),
    mode='overwrite',
    optimize=True
)
```

## Deployment Across Environments

The libraries enable single codebase deployment across all environments:

1. **Development**: Code is developed and tested in dev workspace
2. **Test**: Same code promoted to test workspace via DevOps
3. **Production**: Same code deployed to production workspace

The `EnvironmentConfig` automatically adjusts paths and catalog names based on workspace URL:
- `dev-workspace.databricks.com` → uses `dev_bronze`, `dev_silver`, etc.
- `test-workspace.databricks.com` → uses `test_bronze`, `test_silver`, etc.
- `prod-workspace.databricks.com` → uses `prod_bronze`, `prod_silver`, etc.

## Best Practices

1. **Always use EnvironmentConfig** for paths - never hardcode environments
2. **Use Unity Catalog** for data governance and lineage
3. **Optimize Delta tables** regularly with Z-ordering on filter columns
4. **Add metadata columns** (`_load_timestamp`, `_source_system`) for tracking
5. **Deduplicate** data at silver layer before analytics
6. **Use merge operations** for incremental loads
7. **Partition** large tables by date columns
8. **Version control** all notebooks and configurations

## Integration with Azure Data Factory

The notebooks can be orchestrated from Azure Data Factory pipelines using:
- Databricks notebook activities
- Parameters passed via `dbutils.widgets`
- Metadata-driven pipeline patterns

Example parameter usage in notebook:
```python
# Create widgets
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("load_date", "")

# Get parameters
source_system = dbutils.widgets.get("source_system")
load_date = dbutils.widgets.get("load_date")
```

## Metadata Management

The platform uses metadata tables to drive execution:
- `metadata.bronze.execution_metadata` - Bronze load configurations
- `metadata.silver.execution_metadata` - Silver transformation configs
- `metadata.control.watermarks` - Incremental load tracking

## Contributing

When adding new functionality:
1. Add functions to appropriate library module
2. Follow existing patterns and naming conventions
3. Document with docstrings and type hints
4. Create example notebooks demonstrating usage
5. Test across all environments

## Support

For questions or issues, contact the Data Platform team or refer to the architecture documentation in the OneNote package.
