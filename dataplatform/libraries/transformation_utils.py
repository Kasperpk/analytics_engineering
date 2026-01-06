"""
Transformation Utilities Module
Provides common transformation functions for data processing.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, coalesce, trim, upper, lower, regexp_replace,
    concat, concat_ws, substring, length, to_date, to_timestamp,
    current_timestamp, datediff, months_between, year, month, dayofmonth,
    md5, sha2, hash
)
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, TimestampType
from typing import List, Dict, Optional


class TransformationUtils:
    """
    Common transformation utilities for data processing.
    """
    
    @staticmethod
    def standardize_column_names(df: DataFrame, case: str = 'lower') -> DataFrame:
        """
        Standardize column names (remove spaces, special chars, apply case).
        
        Args:
            df: Input DataFrame
            case: Target case ('lower', 'upper', 'snake')
            
        Returns:
            DataFrame with standardized column names
            
        Example:
            >>> df = utils.standardize_column_names(df, case='snake')
        """
        new_columns = []
        for col_name in df.columns:
            # Remove special characters and spaces
            clean_name = regexp_replace(col_name, r'[^a-zA-Z0-9_]', '_')
            clean_name = regexp_replace(clean_name, r'_{2,}', '_')
            clean_name = clean_name.strip('_')
            
            if case == 'lower':
                clean_name = clean_name.lower()
            elif case == 'upper':
                clean_name = clean_name.upper()
            elif case == 'snake':
                # Convert camelCase to snake_case
                import re
                clean_name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', clean_name)
                clean_name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', clean_name).lower()
            
            new_columns.append(clean_name)
        
        # Rename columns
        for old_name, new_name in zip(df.columns, new_columns):
            df = df.withColumnRenamed(old_name, new_name)
        
        return df
    
    @staticmethod
    def clean_string_columns(
        df: DataFrame,
        columns: List[str] = None,
        trim_spaces: bool = True,
        remove_nulls: bool = True,
        null_replacement: str = ''
    ) -> DataFrame:
        """
        Clean string columns by trimming, replacing nulls, etc.
        
        Args:
            df: Input DataFrame
            columns: List of columns to clean (None = all string columns)
            trim_spaces: Whether to trim leading/trailing spaces
            remove_nulls: Whether to replace nulls
            null_replacement: Value to replace nulls with
            
        Returns:
            DataFrame with cleaned string columns
        """
        if columns is None:
            # Get all string columns
            columns = [field.name for field in df.schema.fields 
                      if field.dataType == StringType()]
        
        for col_name in columns:
            if col_name in df.columns:
                col_expr = col(col_name)
                
                if trim_spaces:
                    col_expr = trim(col_expr)
                
                if remove_nulls:
                    col_expr = coalesce(col_expr, lit(null_replacement))
                
                df = df.withColumn(col_name, col_expr)
        
        return df
    
    @staticmethod
    def cast_columns(df: DataFrame, column_types: Dict[str, str]) -> DataFrame:
        """
        Cast multiple columns to specified types.
        
        Args:
            df: Input DataFrame
            column_types: Dictionary mapping column names to target types
                         ('string', 'int', 'double', 'date', 'timestamp')
            
        Returns:
            DataFrame with casted columns
            
        Example:
            >>> df = utils.cast_columns(df, {'age': 'int', 'salary': 'double'})
        """
        type_map = {
            'string': StringType(),
            'int': IntegerType(),
            'integer': IntegerType(),
            'double': DoubleType(),
            'float': DoubleType(),
            'date': DateType(),
            'timestamp': TimestampType()
        }
        
        for col_name, target_type in column_types.items():
            if col_name in df.columns:
                spark_type = type_map.get(target_type.lower())
                if spark_type:
                    df = df.withColumn(col_name, col(col_name).cast(spark_type))
        
        return df
    
    @staticmethod
    def add_surrogate_key(
        df: DataFrame,
        key_columns: List[str],
        key_name: str = 'surrogate_key',
        hash_function: str = 'md5'
    ) -> DataFrame:
        """
        Add surrogate key based on natural key columns.
        
        Args:
            df: Input DataFrame
            key_columns: Columns to use for generating key
            key_name: Name of the surrogate key column
            hash_function: Hash function to use ('md5', 'sha256')
            
        Returns:
            DataFrame with surrogate key column
            
        Example:
            >>> df = utils.add_surrogate_key(df, ['customer_id', 'country'])
        """
        # Concatenate key columns
        concat_expr = concat_ws('|', *[coalesce(col(c).cast('string'), lit('')) 
                                       for c in key_columns])
        
        # Apply hash function
        if hash_function.lower() == 'md5':
            key_expr = md5(concat_expr)
        elif hash_function.lower() == 'sha256':
            key_expr = sha2(concat_expr, 256)
        else:
            key_expr = hash(concat_expr)
        
        return df.withColumn(key_name, key_expr)
    
    @staticmethod
    def parse_dates(
        df: DataFrame,
        date_columns: Dict[str, str],
        date_format: str = None
    ) -> DataFrame:
        """
        Parse string columns to date columns.
        
        Args:
            df: Input DataFrame
            date_columns: Dictionary mapping column names to date formats
            date_format: Default date format (if not specified per column)
            
        Returns:
            DataFrame with parsed date columns
            
        Example:
            >>> df = utils.parse_dates(df, {'order_date': 'yyyy-MM-dd'})
        """
        for col_name, fmt in date_columns.items():
            if col_name in df.columns:
                date_fmt = fmt or date_format
                if date_fmt:
                    df = df.withColumn(col_name, to_date(col(col_name), date_fmt))
                else:
                    df = df.withColumn(col_name, to_date(col(col_name)))
        
        return df
    
    @staticmethod
    def add_date_dimensions(
        df: DataFrame,
        date_column: str,
        prefix: str = ''
    ) -> DataFrame:
        """
        Add date dimension columns (year, month, day, etc.) from a date column.
        
        Args:
            df: Input DataFrame
            date_column: Name of the date column
            prefix: Prefix for new columns (e.g., 'order_' -> 'order_year')
            
        Returns:
            DataFrame with date dimension columns
            
        Example:
            >>> df = utils.add_date_dimensions(df, 'order_date', prefix='order_')
        """
        if date_column not in df.columns:
            raise ValueError(f"Column {date_column} not found in DataFrame")
        
        date_col = col(date_column)
        
        df = (df
              .withColumn(f"{prefix}year", year(date_col))
              .withColumn(f"{prefix}month", month(date_col))
              .withColumn(f"{prefix}day", dayofmonth(date_col))
              .withColumn(f"{prefix}quarter", 
                         when(month(date_col) <= 3, 1)
                         .when(month(date_col) <= 6, 2)
                         .when(month(date_col) <= 9, 3)
                         .otherwise(4)))
        
        return df
    
    @staticmethod
    def apply_business_rules(
        df: DataFrame,
        rules: List[Dict[str, any]]
    ) -> DataFrame:
        """
        Apply business rules to DataFrame.
        
        Args:
            df: Input DataFrame
            rules: List of rule dictionaries with keys:
                   - column: target column name
                   - condition: SQL condition string
                   - value: value to set when condition is true
                   - else_value: value when condition is false (optional)
            
        Returns:
            DataFrame with rules applied
            
        Example:
            >>> rules = [
            ...     {'column': 'status', 'condition': 'amount > 1000', 
            ...      'value': 'HIGH', 'else_value': 'LOW'}
            ... ]
            >>> df = utils.apply_business_rules(df, rules)
        """
        for rule in rules:
            col_name = rule['column']
            condition = rule['condition']
            value = rule['value']
            else_value = rule.get('else_value')
            
            if else_value is not None:
                df = df.withColumn(col_name, 
                                  when(expr(condition), lit(value))
                                  .otherwise(lit(else_value)))
            else:
                df = df.withColumn(col_name, 
                                  when(expr(condition), lit(value))
                                  .otherwise(col(col_name)))
        
        return df
    
    @staticmethod
    def pivot_table(
        df: DataFrame,
        group_by_cols: List[str],
        pivot_col: str,
        agg_col: str,
        agg_func: str = 'sum'
    ) -> DataFrame:
        """
        Pivot DataFrame.
        
        Args:
            df: Input DataFrame
            group_by_cols: Columns to group by
            pivot_col: Column to pivot on
            agg_col: Column to aggregate
            agg_func: Aggregation function ('sum', 'avg', 'count', etc.)
            
        Returns:
            Pivoted DataFrame
            
        Example:
            >>> pivoted = utils.pivot_table(df, ['customer'], 'product', 'revenue')
        """
        return (df
                .groupBy(*group_by_cols)
                .pivot(pivot_col)
                .agg({agg_col: agg_func}))
    
    @staticmethod
    def unpivot_table(
        df: DataFrame,
        id_cols: List[str],
        value_cols: List[str],
        var_name: str = 'variable',
        value_name: str = 'value'
    ) -> DataFrame:
        """
        Unpivot (melt) DataFrame.
        
        Args:
            df: Input DataFrame
            id_cols: Columns to keep as identifiers
            value_cols: Columns to unpivot
            var_name: Name for the variable column
            value_name: Name for the value column
            
        Returns:
            Unpivoted DataFrame
            
        Example:
            >>> unpivoted = utils.unpivot_table(df, ['id'], ['col1', 'col2'])
        """
        from pyspark.sql.functions import expr, array, explode, struct, lit as _lit
        
        # Create array of structs
        unpivot_expr = explode(array([
            struct(_lit(c).alias(var_name), col(c).alias(value_name))
            for c in value_cols
        ])).alias("_unpivot")
        
        # Select id columns and unpivoted columns
        return (df
                .select(*id_cols, unpivot_expr)
                .select(*id_cols, f"_unpivot.{var_name}", f"_unpivot.{value_name}"))
    
    @staticmethod
    def fill_nulls(
        df: DataFrame,
        fill_values: Dict[str, any] = None,
        fill_strategy: str = 'forward'
    ) -> DataFrame:
        """
        Fill null values in DataFrame.
        
        Args:
            df: Input DataFrame
            fill_values: Dictionary mapping columns to fill values
            fill_strategy: Strategy for filling ('forward', 'backward', 'mean', 'zero')
            
        Returns:
            DataFrame with nulls filled
        """
        if fill_values:
            return df.fillna(fill_values)
        
        # Implement forward fill strategy if needed
        # This is a simplified version
        if fill_strategy == 'zero':
            numeric_cols = [field.name for field in df.schema.fields 
                          if field.dataType in [IntegerType(), DoubleType()]]
            return df.fillna(0, subset=numeric_cols)
        
        return df


# Convenience function
from pyspark.sql.functions import expr

def apply_transformation(df: DataFrame, transformation: str) -> DataFrame:
    """
    Apply a transformation expression to DataFrame.
    
    Args:
        df: Input DataFrame
        transformation: SQL transformation expression
        
    Returns:
        Transformed DataFrame
    """
    return df.selectExpr(transformation)
