"""
Data Operations Module
Provides general functions for reading and writing data across the data platform.
Supports Delta Lake operations with various patterns (full load, incremental, SCD2).
"""

from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, md5, concat_ws, row_number
from pyspark.sql.window import Window
from typing import List, Optional, Dict
from delta.tables import DeltaTable


@dataclass
class DataOperations:
    """
    Handles common data read/write operations across the platform.
    """
    
    spark: SparkSession
    
    def read_delta(self, path: str = None, table_name: str = None) -> DataFrame:
        """
        Read data from Delta Lake table or path.
        
        Args:
            path: ABFSS path to Delta table
            table_name: Unity Catalog table name (catalog.schema.table)
            
        Returns:
            DataFrame with table data
            
        Example:
            >>> df = ops.read_delta(table_name='dev_bronze.aarsleff.customers')
            >>> df = ops.read_delta(path='abfss://...bronze/aarsleff/customers')
        """
        if table_name:
            return self.spark.table(table_name)
        elif path:
            return self.spark.read.format("delta").load(path)
        else:
            raise ValueError("Either path or table_name must be provided")
    
    def read_parquet(self, path: str) -> DataFrame:
        """
        Read Parquet files from path.
        
        Args:
            path: Path to parquet files
            
        Returns:
            DataFrame with data
        """
        return self.spark.read.parquet(path)
    
    def read_csv(self, path: str, **options) -> DataFrame:
        """
        Read CSV files with options.
        
        Args:
            path: Path to CSV files
            **options: Additional read options (header, inferSchema, delimiter, etc.)
            
        Returns:
            DataFrame with data
        """
        return self.spark.read.options(**options).csv(path)
    
    def read_json(self, path: str, **options) -> DataFrame:
        """
        Read JSON files with options.
        
        Args:
            path: Path to JSON files
            **options: Additional read options
            
        Returns:
            DataFrame with data
        """
        return self.spark.read.options(**options).json(path)
    
    def write_delta(
        self,
        df: DataFrame,
        path: str = None,
        table_name: str = None,
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None,
        optimize: bool = False,
        z_order_by: Optional[List[str]] = None
    ) -> None:
        """
        Write DataFrame to Delta Lake.
        
        Args:
            df: DataFrame to write
            path: ABFSS path for Delta table
            table_name: Unity Catalog table name
            mode: Write mode (overwrite, append, merge)
            partition_by: List of columns to partition by
            optimize: Whether to optimize table after write
            z_order_by: Columns for Z-ordering (requires optimize=True)
            
        Example:
            >>> ops.write_delta(df, table_name='dev_silver.aarsleff.customers', 
            ...                 mode='overwrite', partition_by=['country'])
        """
        writer = df.write.format("delta").mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        if table_name:
            writer.saveAsTable(table_name)
            target = table_name
        elif path:
            writer.save(path)
            target = path
        else:
            raise ValueError("Either path or table_name must be provided")
        
        # Optimize if requested
        if optimize:
            self.optimize_table(path=path, table_name=table_name, z_order_by=z_order_by)
    
    def write_parquet(
        self,
        df: DataFrame,
        path: str,
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None
    ) -> None:
        """
        Write DataFrame to Parquet format.
        
        Args:
            df: DataFrame to write
            path: Output path
            mode: Write mode
            partition_by: Columns to partition by
        """
        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.parquet(path)
    
    def merge_delta(
        self,
        source_df: DataFrame,
        target_path: str = None,
        target_table: str = None,
        merge_keys: List[str] = None,
        update_condition: str = None,
        insert_condition: str = None,
        delete_condition: str = None
    ) -> None:
        """
        Perform Delta Lake merge (upsert) operation.
        
        Args:
            source_df: Source DataFrame
            target_path: Target Delta table path
            target_table: Target Unity Catalog table name
            merge_keys: List of columns to match on
            update_condition: Additional SQL condition for updates (optional)
            insert_condition: Additional SQL condition for inserts (optional)
            delete_condition: SQL condition for deletes (optional)
            
        Example:
            >>> ops.merge_delta(source_df, target_table='dev_silver.aarsleff.customers',
            ...                 merge_keys=['customer_id'])
        """
        if not merge_keys:
            raise ValueError("merge_keys must be provided for merge operation")
        
        # Get target Delta table
        if target_table:
            target_delta = DeltaTable.forName(self.spark, target_table)
        elif target_path:
            target_delta = DeltaTable.forPath(self.spark, target_path)
        else:
            raise ValueError("Either target_path or target_table must be provided")
        
        # Build merge condition
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
        
        # Start merge builder
        merge_builder = target_delta.alias("target").merge(
            source_df.alias("source"),
            merge_condition
        )
        
        # Add update clause
        if update_condition:
            merge_builder = merge_builder.whenMatchedUpdate(
                condition=update_condition,
                set=self._get_update_set(source_df)
            )
        else:
            merge_builder = merge_builder.whenMatchedUpdateAll()
        
        # Add delete clause if specified
        if delete_condition:
            merge_builder = merge_builder.whenMatchedDelete(condition=delete_condition)
        
        # Add insert clause
        if insert_condition:
            merge_builder = merge_builder.whenNotMatchedInsert(
                condition=insert_condition,
                values=self._get_insert_values(source_df)
            )
        else:
            merge_builder = merge_builder.whenNotMatchedInsertAll()
        
        # Execute merge
        merge_builder.execute()
    
    def _get_update_set(self, df: DataFrame) -> Dict[str, str]:
        """Generate update set dictionary for merge."""
        return {col_name: f"source.{col_name}" for col_name in df.columns}
    
    def _get_insert_values(self, df: DataFrame) -> Dict[str, str]:
        """Generate insert values dictionary for merge."""
        return {col_name: f"source.{col_name}" for col_name in df.columns}
    
    def incremental_load(
        self,
        source_df: DataFrame,
        target_path: str = None,
        target_table: str = None,
        merge_keys: List[str] = None,
        date_column: str = None,
        watermark_table: str = None
    ) -> None:
        """
        Perform incremental load with watermark tracking.
        
        Args:
            source_df: Source DataFrame (should be filtered for incremental data)
            target_path: Target Delta path
            target_table: Target table name
            merge_keys: Keys for merging
            date_column: Date column for watermark
            watermark_table: Table to store watermark values
            
        Example:
            >>> ops.incremental_load(df, target_table='dev_silver.aarsleff.orders',
            ...                      merge_keys=['order_id'], date_column='order_date')
        """
        # Perform merge
        self.merge_delta(
            source_df=source_df,
            target_path=target_path,
            target_table=target_table,
            merge_keys=merge_keys
        )
        
        # Update watermark if tracking
        if watermark_table and date_column:
            max_date = source_df.agg({date_column: "max"}).collect()[0][0]
            if max_date:
                self._update_watermark(watermark_table, target_table or target_path, max_date)
    
    def _update_watermark(self, watermark_table: str, source_name: str, max_date) -> None:
        """Update watermark value for incremental loads."""
        watermark_df = self.spark.createDataFrame([
            (source_name, max_date, current_timestamp())
        ], ["source_name", "max_date", "updated_at"])
        
        self.merge_delta(
            source_df=watermark_df,
            target_table=watermark_table,
            merge_keys=["source_name"]
        )
    
    def optimize_table(
        self,
        path: str = None,
        table_name: str = None,
        z_order_by: Optional[List[str]] = None
    ) -> None:
        """
        Optimize Delta table and optionally Z-order.
        
        Args:
            path: Delta table path
            table_name: Unity Catalog table name
            z_order_by: Columns for Z-ordering
            
        Example:
            >>> ops.optimize_table(table_name='dev_silver.aarsleff.customers',
            ...                    z_order_by=['customer_id', 'country'])
        """
        if table_name:
            target = table_name
            delta_table = DeltaTable.forName(self.spark, table_name)
        elif path:
            target = f"`{path}`"
            delta_table = DeltaTable.forPath(self.spark, path)
        else:
            raise ValueError("Either path or table_name must be provided")
        
        # Run optimize
        optimize_cmd = f"OPTIMIZE {target}"
        
        if z_order_by:
            optimize_cmd += f" ZORDER BY ({', '.join(z_order_by)})"
        
        self.spark.sql(optimize_cmd)
    
    def vacuum_table(
        self,
        path: str = None,
        table_name: str = None,
        retention_hours: int = 168  # 7 days default
    ) -> None:
        """
        Vacuum Delta table to remove old files.
        
        Args:
            path: Delta table path
            table_name: Unity Catalog table name
            retention_hours: Retention period in hours (default 168 = 7 days)
            
        Example:
            >>> ops.vacuum_table(table_name='dev_bronze.aarsleff.raw_data', 
            ...                  retention_hours=24)
        """
        if table_name:
            target = table_name
        elif path:
            target = f"`{path}`"
        else:
            raise ValueError("Either path or table_name must be provided")
        
        self.spark.sql(f"VACUUM {target} RETAIN {retention_hours} HOURS")
    
    def deduplicate(
        self,
        df: DataFrame,
        partition_cols: List[str],
        order_col: str,
        order_desc: bool = True
    ) -> DataFrame:
        """
        Deduplicate DataFrame based on partition columns.
        
        Args:
            df: Input DataFrame
            partition_cols: Columns to partition by for deduplication
            order_col: Column to order by (usually timestamp)
            order_desc: Whether to order descending (default True for latest record)
            
        Returns:
            Deduplicated DataFrame
            
        Example:
            >>> clean_df = ops.deduplicate(df, partition_cols=['customer_id'],
            ...                            order_col='modified_date')
        """
        window_spec = Window.partitionBy(*partition_cols).orderBy(
            col(order_col).desc() if order_desc else col(order_col).asc()
        )
        
        return (df
                .withColumn("_row_num", row_number().over(window_spec))
                .filter(col("_row_num") == 1)
                .drop("_row_num"))
    
    def add_metadata_columns(
        self,
        df: DataFrame,
        source_system: str = None,
        load_timestamp: bool = True
    ) -> DataFrame:
        """
        Add standard metadata columns to DataFrame.
        
        Args:
            df: Input DataFrame
            source_system: Source system name
            load_timestamp: Whether to add load timestamp
            
        Returns:
            DataFrame with metadata columns
            
        Example:
            >>> df_with_meta = ops.add_metadata_columns(df, source_system='AARSLEFF_ERP')
        """
        result_df = df
        
        if load_timestamp:
            result_df = result_df.withColumn("_load_timestamp", current_timestamp())
        
        if source_system:
            result_df = result_df.withColumn("_source_system", lit(source_system))
        
        return result_df
