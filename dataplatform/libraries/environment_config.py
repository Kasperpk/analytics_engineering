"""
Environment Configuration Module
Provides dynamic environment detection and path configuration based on workspace URL.
Enables code to be deployed across dev, test, and production environments.
"""

import re
from typing import Dict, Tuple


class EnvironmentConfig:
    """
    Manages environment-specific configurations and paths.
    Detects environment from Databricks workspace URL and provides appropriate paths.
    """
    
    def __init__(self, workspace_url: str = None):
        """
        Initialize environment configuration.
        
        Args:
            workspace_url: Databricks workspace URL. If None, will attempt to get from spark config.
        """
        self.workspace_url = workspace_url
        self.environment = None
        self.root_path = None
        self.catalog_prefix = None
        
        if workspace_url:
            self._detect_environment()
    
    def _detect_environment(self) -> None:
        """
        Detect environment (dev, test, prod) from workspace URL.
        Sets environment, root_path, and catalog_prefix accordingly.
        """
        url_lower = self.workspace_url.lower()
        
        if 'dev' in url_lower or 'development' in url_lower:
            self.environment = 'dev'
        elif 'test' in url_lower or 'uat' in url_lower or 'staging' in url_lower:
            self.environment = 'test'
        elif 'prod' in url_lower or 'production' in url_lower:
            self.environment = 'prod'
        else:
            # Default to dev for safety
            self.environment = 'dev'
        
        # Set environment-specific paths
        self._set_paths()
    
    def _set_paths(self) -> None:
        """Set environment-specific paths for data storage."""
        base_path = 'abfss://dataplatform@datalakestorage.dfs.core.windows.net'
        
        self.root_path = f'{base_path}/{self.environment}'
        self.catalog_prefix = self.environment
        
        # Define standard container paths
        self.paths = {
            'bronze': f'{self.root_path}/bronze',
            'silver': f'{self.root_path}/silver',
            'gold': f'{self.root_path}/gold',
            'metadata': f'{self.root_path}/metadata',
            'shared': f'{self.root_path}/shared',
            'raw': f'{self.root_path}/raw',
            'archive': f'{self.root_path}/archive',
            'temp': f'{self.root_path}/temp'
        }
        
        # Define Unity Catalog names
        self.catalogs = {
            'bronze': f'{self.catalog_prefix}_bronze',
            'silver': f'{self.catalog_prefix}_silver',
            'gold': f'{self.catalog_prefix}_gold',
            'metadata': f'{self.catalog_prefix}_metadata'
        }
    
    @staticmethod
    def from_spark(spark) -> 'EnvironmentConfig':
        """
        Create EnvironmentConfig from Spark session.
        
        Args:
            spark: Active Spark session
            
        Returns:
            EnvironmentConfig instance
        """
        workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
        return EnvironmentConfig(workspace_url)
    
    def get_path(self, layer: str, *subdirs: str) -> str:
        """
        Get path for a specific data layer with optional subdirectories.
        
        Args:
            layer: Data layer name (bronze, silver, gold, metadata, etc.)
            *subdirs: Optional subdirectory path components
            
        Returns:
            Full path string
            
        Example:
            >>> config.get_path('bronze', 'aarsleff', 'customers')
            'abfss://dataplatform@...dfs.../dev/bronze/aarsleff/customers'
        """
        if layer not in self.paths:
            raise ValueError(f"Unknown layer: {layer}. Available: {list(self.paths.keys())}")
        
        base = self.paths[layer]
        if subdirs:
            return f"{base}/{'/'.join(subdirs)}"
        return base
    
    def get_catalog(self, layer: str) -> str:
        """
        Get Unity Catalog name for a specific layer.
        
        Args:
            layer: Data layer name (bronze, silver, gold, metadata)
            
        Returns:
            Catalog name string
            
        Example:
            >>> config.get_catalog('silver')
            'dev_silver'
        """
        if layer not in self.catalogs:
            raise ValueError(f"Unknown catalog layer: {layer}. Available: {list(self.catalogs.keys())}")
        
        return self.catalogs[layer]
    
    def get_table_path(self, layer: str, schema: str, table: str) -> str:
        """
        Get full table path for a specific layer, schema, and table.
        
        Args:
            layer: Data layer (bronze, silver, gold)
            schema: Schema/database name
            table: Table name
            
        Returns:
            Full table path
            
        Example:
            >>> config.get_table_path('silver', 'aarsleff', 'customers')
            'abfss://dataplatform@...dfs.../dev/silver/aarsleff/customers'
        """
        return self.get_path(layer, schema, table)
    
    def get_full_table_name(self, layer: str, schema: str, table: str) -> str:
        """
        Get full Unity Catalog table name (catalog.schema.table).
        
        Args:
            layer: Data layer (bronze, silver, gold)
            schema: Schema name
            table: Table name
            
        Returns:
            Full table name in catalog.schema.table format
            
        Example:
            >>> config.get_full_table_name('silver', 'aarsleff', 'customers')
            'dev_silver.aarsleff.customers'
        """
        catalog = self.get_catalog(layer)
        return f"{catalog}.{schema}.{table}"
    
    def is_production(self) -> bool:
        """Check if current environment is production."""
        return self.environment == 'prod'
    
    def is_development(self) -> bool:
        """Check if current environment is development."""
        return self.environment == 'dev'
    
    def is_test(self) -> bool:
        """Check if current environment is test."""
        return self.environment == 'test'
    
    def get_environment_info(self) -> Dict[str, str]:
        """
        Get comprehensive environment information.
        
        Returns:
            Dictionary with environment details
        """
        return {
            'environment': self.environment,
            'workspace_url': self.workspace_url,
            'root_path': self.root_path,
            'catalog_prefix': self.catalog_prefix,
            'paths': self.paths,
            'catalogs': self.catalogs
        }
    
    def __repr__(self) -> str:
        """String representation of environment config."""
        return f"EnvironmentConfig(environment='{self.environment}', root_path='{self.root_path}')"


# Convenience function for quick initialization
def get_config(spark=None, workspace_url: str = None) -> EnvironmentConfig:
    """
    Convenience function to get environment configuration.
    
    Args:
        spark: Spark session (optional)
        workspace_url: Workspace URL (optional, required if spark is None)
        
    Returns:
        EnvironmentConfig instance
        
    Example:
        >>> config = get_config(spark)
        >>> bronze_path = config.get_path('bronze', 'aarsleff')
    """
    if spark:
        return EnvironmentConfig.from_spark(spark)
    elif workspace_url:
        return EnvironmentConfig(workspace_url)
    else:
        raise ValueError("Either spark session or workspace_url must be provided")
