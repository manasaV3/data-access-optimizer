#!/usr/bin/env python3
"""
ManifestLookup class for querying gene and tissue data from parquet file.
"""

import logging

from parquet_lookup import ParquetLookup

logger = logging.getLogger(__name__)


class Record:
    def __init__(self, tissue_id: int, gene_id: str, file_path: str):
        self.tissue_id = tissue_id
        self.gene_id = gene_id
        self.file_path = file_path

    def __repr__(self):
        return f"Record(gene_id={self.gene_id}, tissue_id={self.tissue_id}, file_path={self.file_path})"


class ManifestLookup(ParquetLookup):
    """
    A class to perform lookups on parquet files containing gene_id, tissue_id, and file_path data.

    This class provides methods to:
    - Check if gene_id and tissue_id combinations exist
    - Get file paths for specific gene/tissue combinations
    - List all tissues with path for a given gene
    - List all genes with path  for a given tissue
    - List all unique genes and tissues in the dataset
    """

    def __init__(self, manifest_file_path: str, tmp_dir: str = None, aws_credentials: dict = None):
        """
        Initialize the ManifestLookup with data from a parquet file.

        Args:
            manifest_file_path (str): Path to the parquet file
            tmp_dir (str, optional): Directory for temporary files
            aws_credentials (dict, optional): AWS credentials for S3 access
        Raises:
            FileNotFoundError: If the parquet file doesn't exist
            ValueError: If the parquet file doesn't have required columns
        """
        super().__init__(manifest_file_path, tmp_dir or "/tmp/data_lookup_v1/", aws_credentials)

    def _get_table_name(self) -> str:
        """Return the table name to use in DuckDB."""
        return "manifest"

    def _get_required_columns(self) -> list[str]:
        """Return the set of required columns for this lookup type."""
        return ['tissue_id', 'gene_id', 'file_path']

    def _get_queryable_columns(self) -> set[str]:
        return {'gene_id', 'tissue_id'}

    def _create_indexes(self, table_name: str):
        """Create appropriate indexes for the table."""
        self.con.execute(f"CREATE INDEX idx_gene_id ON {table_name}(gene_id)")
        self.con.execute(f"CREATE INDEX idx_tissue_id ON {table_name}(tissue_id)")

    @classmethod
    def _standardize_tissue_id(cls, tissue_id: str | int) -> str:
        if isinstance(tissue_id, str):
            if tissue_id.startswith("model_"):
                tissue_id = tissue_id.replace("model_", "")
            if tissue_id.startswith("tissue_"):
                tissue_id = tissue_id.replace("tissue_", "")
        return str(tissue_id)

    def exists(self, gene_id: str, tissue_id: str | int) -> bool:
        """
        Check if a combination of gene_id and tissue_id exists in the data.

        Args:
            gene_id (str): The gene identifier
            tissue_id (str|int): The tissue identifier

        Returns:
            bool: True if the combination exists, False otherwise
        """
        tissue_id = self._standardize_tissue_id(tissue_id)
        return self._exists({'gene_id': gene_id, 'tissue_id': tissue_id})

    def get_s3_file_path(self, gene_id: str, tissue_id: str | int) -> str:
        """
        Returns s3 file_path if a combination of gene_id and tissue_id exists in the data else returns None.

        Args:
            gene_id (str): The gene identifier
            tissue_id (str|int): The tissue identifier

        Returns:
            str: Returns file_path if exists else None
        """
        result = self.query(gene_id=gene_id, tissue_id=tissue_id)
        return result[0].file_path if result else None

    def get_file_path(self, gene_id: str, tissue_id: str | int) -> str | None:
        """
        Returns file_path of local folder if a combination of gene_id and tissue_id exists in the s3, after downloading
         it to local directory else returns None.

        Args:
            gene_id (str): The gene identifier
            tissue_id (str|int): The tissue identifier

        Returns:
            str: Returns local file_path of the downloaded object if combination of gene_id and tissue_id exists else None
        """
        s3_file_path = self.get_s3_file_path(gene_id=gene_id, tissue_id=tissue_id)
        if not s3_file_path:
            return None
        return self._read_s3_file(s3_file_path)

    def get_records_for_gene(self, gene_id: str) -> list[Record]:
        """
        Get all records associated with a specific gene ID.

        Args:
            gene_id (str): The gene identifier

        Returns:
            list[Record]: List of records associated with the gene
        """
        return self.query(gene_id=gene_id)

    def get_records_for_tissue(self, tissue_id: str | int) -> list[Record]:
        """
        Get all records associated with a specific tissue ID.

        Args:
            tissue_id (str|int): The tissue identifier

        Returns:
            list[Record]: List of records associated with the tissue
        """
        return self.query(tissue_id=tissue_id)

    def query(self, gene_id: str = None, tissue_id: str | int = None) -> list[Record]:
        """
        Get the file path for a specific gene_id and tissue_id combination.
        Args:
            gene_id (str): The gene identifier
            tissue_id (str, int): The tissue identifier

        Returns:
            list[Record]: A list of Record objects matching the query
        """
        if tissue_id is not None:
            tissue_id = self._standardize_tissue_id(tissue_id)
        result = self._query({'gene_id': gene_id, 'tissue_id': tissue_id})

        # Convert to list of Record objects
        return [Record(*row) for row in result]
