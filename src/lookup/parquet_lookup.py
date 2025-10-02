#!/usr/bin/env python3
"""
Base ParquetLookup class for querying data from parquet files.
"""

import logging
import os.path
from abc import ABC, abstractmethod
from typing import Any

import boto3
import duckdb

logger = logging.getLogger(__name__)


class ParquetLookup(ABC):
    """
    A base class for performing lookups on parquet files.

    This class provides common functionality for:
    - Loading parquet files from local paths or S3
    - Setting up DuckDB connection and indexing
    - Downloading files from S3 with caching support
    - Context manager and connection lifecycle management
    """

    def __init__(self, file_path: str, tmp_dir: str = None, aws_credentials: dict = None):
        """
        Initialize the ParquetLookup with data from a parquet file.

        Args:
            file_path (str): Path to the parquet file (local or S3)
            tmp_dir (str, optional): Directory for temporary files
            aws_credentials (dict, optional): AWS credentials for S3 access
        Raises:
            FileNotFoundError: If the parquet file doesn't exist
            ValueError: If the parquet file doesn't have required columns
        """
        self.tmp_dir = tmp_dir

        if file_path.startswith("s3://"):
            file_path_split = file_path.split("/")
            self.bucket = file_path_split[2]
            self.file_path = "/".join(file_path_split[3:])
        else:
            self.bucket = None
            self.file_path = file_path
        
        self.s3 = boto3.client("s3", **(aws_credentials or {}))
        self.local_file_path = self._get_local_path()
        self._load_data()

    def _get_local_path(self) -> str:
        """Load the parquet file from local path or S3."""
        if self.bucket:
             return self._read_s3_file(self.file_path)
        elif os.path.exists(self.file_path):
            return self.file_path

        logger.error(f"Parquet file path not found: {self.local_file_path}")
        raise FileNotFoundError(f"Parquet file not found: {self.local_file_path}")

    def _load_data(self):
        """Load the data from parquet file and create indexes."""
        try:
            logger.info(f"Loading parquet file: {self.local_file_path}")
            self.con = duckdb.connect(':memory:')

            # Load the data
            table_name = self._get_table_name()
            self.con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM '{self.local_file_path}'")

            # Validate required columns exist
            columns = self.con.execute(f"PRAGMA table_info({table_name})").fetchall()
            column_names = {col[1] for col in columns}  # col[1] is the column name
            required_columns = set(self._get_required_columns())

            missing_columns = required_columns - column_names
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")

            logger.info(f"Validated schema - found columns: {column_names}")

            # Create indexes for better performance
            self._create_indexes(table_name)

        except Exception as e:
            logger.error(f"Error loading parquet file: {e}")
            raise ValueError(f"Error loading parquet file: {e}")

    def _read_s3_file(self, s3_path: str) -> str:
        """Download file from S3 to local temporary directory."""
        if not self.bucket:
            raise ValueError("S3 bucket not configured for this instance.")
        try:
            # Fetch the file from S3 to a local path
            local_file_path = os.path.join(self.tmp_dir, s3_path)
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            # TODO: Add caching logic to avoid re-downloading files
            logger.info(f"Downloading from S3: s3://{self.bucket}/{s3_path}")
            with open(local_file_path, "wb") as data:
                self.s3.download_fileobj(self.bucket, s3_path, data)
            return local_file_path

        except Exception as e:
            logger.error(f"Failed to download from S3: {e}")
            raise ValueError(f"S3 download failed: {e}")

    @abstractmethod
    def _get_table_name(self) -> str:
        """Return the table name to use in DuckDB."""
        pass

    @abstractmethod
    def _get_required_columns(self) -> list[str]:
        """Return the list of required columns for this lookup type. Keeping it a list to preserve order."""
        pass

    @abstractmethod
    def _get_queryable_columns(self) -> set[str]:
        """Return the set of columns that can be predicated on for this lookup type."""
        pass

    @abstractmethod
    def _create_indexes(self, table_name: str):
        """Create appropriate indexes for the table."""
        pass

    def _query_data(self, query: str, params: list = None) -> list:
        """Execute a query and return the results."""
        return self.con.execute(query, params).fetchall()

    def _query(self, query_parameters: dict[str, Any]) -> list:
        """
        Get the records for a combination of query parameters.
        Args:
            query_parameters: dict[str, Any]: A dictionary of column names and their values to query
        Returns:
            list: A list of objects matching the query
        """
        if not query_parameters:
            raise ValueError("At least one query parameter must be provided.")
        if any(key not in self._get_queryable_columns() for key in query_parameters.keys()):
            raise ValueError(f"query parameters must be one of {self._get_queryable_columns()}")
        query = [f"SELECT {', '.join(self._get_required_columns())} FROM {self._get_table_name()} WHERE"]
        params = []

        first_param = True
        for key, value in query_parameters.items():
            if not value:
                continue
            if not first_param:
                query.append("AND")
            query.append(f"{key} = ?")
            params.append(value)
            first_param = False
        return self._query_data(" ".join(query), params)

    def _exists(self, query_parameters: dict[str, Any]) -> bool:
        """
        Check if a combination of query parameters exists in the data.

        Args:
        Returns:
            bool: True if the combination exists, False otherwise
        """
        result = self._query(query_parameters)
        return len(result) > 0

    def get_unique(self, column_name: str) -> list[str]:
        if column_name not in self._get_queryable_columns():
            raise ValueError(f"column_name must be one of {self._get_queryable_columns()}")

        result = self._query_data(f"SELECT DISTINCT {column_name} FROM {self._get_table_name()}")
        logger.info(f"Found {len(result)} distinct values for {column_name}")
        return [row[0] for row in result]

    def close(self):
        """Close the DuckDB connection."""
        if hasattr(self, 'con') and self.con:
            self.con.close()
            self.con = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def __del__(self):
        """Destructor to ensure connection is closed."""
        self.close()

    def __repr__(self) -> str:
        """Representation of the ParquetLookup object."""
        return f"{self.__class__.__name__}('{self.file_path}')"
