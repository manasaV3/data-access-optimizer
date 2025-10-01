#!/usr/bin/env python3
"""
ManifestLookup class for querying gene and tissue data from parquet file.
"""

import logging
import os.path

import boto3
import duckdb

logger = logging.getLogger(__name__)


class Record:
    def __init__(self, tissue_id: int, gene_id: str, file_path: str):
        self.tissue_id = tissue_id
        self.gene_id = gene_id
        self.file_path = file_path

    def __repr__(self):
        return f"Record(gene_id={self.gene_id}, tissue_id={self.tissue_id}, file_path={self.file_path})"


class ManifestLookup:
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

        self.tmp_dir = tmp_dir or "/tmp/data_lookup_v1/"

        if manifest_file_path.startswith("s3://"):
            manifest_file_path_split = manifest_file_path.split("/")
            self.bucket = manifest_file_path_split[2]
            self.manifest_file_path = "/".join(manifest_file_path_split[3:])
        else:
            self.bucket = None
            self.manifest_file_path = manifest_file_path
        self.s3 = boto3.client("s3", **(aws_credentials or {}))
        self._load_file()
        self._load_data()

    def exists(self, gene_id: str, tissue_id: str | int) -> bool:
        """
        Check if a combination of gene_id and tissue_id exists in the data.

        Args:
            gene_id (str): The gene identifier
            tissue_id (str|int): The tissue identifier

        Returns:
            bool: True if the combination exists, False otherwise
        """
        result = self._query(gene_id=gene_id, tissue_id=tissue_id)
        return len(result) > 0

    def get_s3_file_path(self, gene_id: str, tissue_id: str | int) -> str:
        """
        Returns s3 file_path if a combination of gene_id and tissue_id exists in the data else returns None.

        Args:
            gene_id (str): The gene identifier
            tissue_id (str|int): The tissue identifier

        Returns:
            str: Returns file_path if exists else None
        """
        result = self._query(gene_id=gene_id, tissue_id=tissue_id)
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
        return self._query(gene_id=gene_id)

    def get_records_for_tissue(self, tissue_id: str | int) -> list[Record]:
        """
        Get all records associated with a specific tissue ID.

        Args:
            tissue_id (str|int): The tissue identifier

        Returns:
            list[Record]: List of records associated with the tissue
        """
        return self._query(tissue_id=tissue_id)

    def get_unique(self, column_name: str) -> list[str]:
        if column_name not in {"gene_id", "tissue_id"}:
            raise ValueError("column_name must be 'gene_id' or 'tissue_id'")

        result = self.con.execute(f"SELECT DISTINCT {column_name} FROM manifest").fetchall()
        logger.info(f"Found {len(result)} distinct values for {column_name}")
        return [row[0] for row in result]

    def _read_s3_file(self, s3_path: str) -> str:
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

    def _load_file(self):
        self.local_file_path = None
        if self.bucket:
            self.local_file_path = self._read_s3_file(self.manifest_file_path)
        else:
            self.local_file_path = self.manifest_file_path

        if not self.local_file_path or not os.path.exists(self.local_file_path):
            logger.error(f"Parquet file not found: {self.local_file_path}")
            raise FileNotFoundError(f"Parquet file not found: {self.local_file_path}")

    def _load_data(self):
        """Load the data from parquet file."""
        try:
            logger.info(f"Loading parquet file: {self.local_file_path}")
            self.con = duckdb.connect(':memory:')

            # Load and index the data
            self.con.execute(f"CREATE TABLE manifest AS SELECT * FROM '{self.local_file_path}'")

            # Validate required columns exist
            columns = self.con.execute("PRAGMA table_info(manifest)").fetchall()
            column_names = {col[1] for col in columns}  # col[1] is the column name
            required_columns = {'gene_id', 'tissue_id', 'file_path'}

            missing_columns = required_columns - column_names
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")

            logger.info(f"Validated schema - found columns: {column_names}")

            # Create indexes for better performance
            self.con.execute(f"CREATE INDEX idx_gene_id ON manifest(gene_id)")
            self.con.execute(f"CREATE INDEX idx_tissue_id ON manifest(tissue_id)")

        except Exception as e:
            logger.error(f"Error loading manifest file: {e}")
            raise ValueError(f"Error loading manifest file: {e}")

    def _query(self, gene_id: str = None, tissue_id: str | int = None) -> list[Record]:
        """
        Get the file path for a specific gene_id and tissue_id combination.
        Args:
            gene_id (str): The gene identifier
            tissue_id (str, int): The tissue identifier

        Returns:
            list[Record]: A list of Record objects matching the query
        """
        if not (gene_id or tissue_id):
            raise ValueError("At least one of gene_id or tissue_id must be provided.")

        query = ["SELECT tissue_id, gene_id, file_path FROM manifest WHERE"]
        params = []

        if gene_id is not None:
            query.append("gene_id = ?")
            params.append(gene_id)

        if tissue_id is not None:
            if isinstance(tissue_id, str):
                if tissue_id.startswith("model_"):
                    tissue_id = tissue_id.replace("model_", "")
                if tissue_id.startswith("tissue_"):
                    tissue_id = tissue_id.replace("tissue_", "")
                tissue_id = int(tissue_id)
            if params:
                query.append("AND")
            query.append("tissue_id = ?")
            params.append(tissue_id)

        result = self.con.execute(" ".join(query), params).fetchall()

        # Convert to list of Record objects
        return [Record(*row) for row in result]

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
        """Representation of the ManifestLookup object."""
        return f"ManifestLookup('{self.manifest_file_path}')"
