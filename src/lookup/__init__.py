"""
Manifest lookup module for querying gene and tissue data from parquet files.
"""

from .manifest_lookup import ManifestLookup, Record

__version__ = "1.0.0"
__all__ = ["ManifestLookup", "Record"]
