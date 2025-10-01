#!/usr/bin/env python3
"""
Script to generate a parquet file from a list of file paths.
Extracts gene_id and tissue_id from file paths, and generates an optimized parquet file.
"""

import logging
import pandas as pd
import re
import argparse
import sys
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import time
from abc import ABC, abstractmethod


logger = logging.getLogger(__name__)


class ManifestGenerator(ABC):
    """
    Base class for generating optimized parquet files from input files.
    Handles file I/O, parquet optimization, and data validation.
    Subclasses must implement line processing and schema definition.
    """

    def __init__(self):
        pass

    @abstractmethod
    def process_line(self, line: str, line_num: int) -> dict | None:
        """
        Process a single line and convert it to a record dictionary.
        
        Args:
            line (str): Input line to process
            line_num (int): Line number for error reporting
            
        Returns:
            dict | None: Record dictionary or None if line should be skipped
        """
        pass

    @abstractmethod
    def get_schema(self) -> pa.Schema:
        """
        Get the PyArrow schema for the output parquet file.
        
        Returns:
            pa.Schema: PyArrow schema defining the output structure
        """
        pass

    def _process_chunk(self, lines_chunk: list[str], chunk_start: int) -> list[dict]:
        """
        Process a chunk of lines and extract data.
        
        Args:
            lines_chunk: List of file path lines to process
            chunk_start: Starting line number for this chunk
            
        Returns:
            List of dictionaries containing extracted data
        """
        data = []
        invalid_count = 0
        
        for i, line in enumerate(lines_chunk):
            line_num = chunk_start + i + 1
            line = line.strip()

            # Skip empty lines
            if not line:
                continue

            record = self.process_line(line, line_num)
            
            if record is None:
                invalid_count += 1
                continue

            data.append(record)
        
        if invalid_count > 0:
            logging.info(f"Processed chunk: {len(data)} valid records, {invalid_count} invalid records")
            
        return data

    def process_file_list(self, input_file_path: Path, chunk_size: int = 10000) -> pd.DataFrame:
        """
        Process the input file containing file paths and extract data using chunked processing.

        Args:
            input_file_path (Path): Path to the input file containing file paths
            chunk_size (int): Number of lines to process in each chunk

        Returns:
            pd.DataFrame: DataFrame with processed records
        """
        all_data = []
        total_lines = 0
        total_valid = 0
        start_time = time.time()

        try:
            # Get file size for progress tracking
            file_size = input_file_path.stat().st_size
            logging.info(f"Processing file: {input_file_path} ({file_size:,} bytes)")
            
            with open(input_file_path, 'r') as file:
                while True:
                    lines_chunk = []
                    for _ in range(chunk_size):
                        line = file.readline()
                        if not line:
                            break
                        lines_chunk.append(line)
                    
                    if not lines_chunk:
                        break
                    
                    # Process this chunk
                    chunk_data = self._process_chunk(lines_chunk, total_lines)
                    all_data.extend(chunk_data)
                    
                    total_lines += len(lines_chunk)
                    total_valid += len(chunk_data)
                    
                    # Progress logging every 50k lines
                    if total_lines % 50000 == 0:
                        elapsed = time.time() - start_time
                        rate = total_lines / elapsed if elapsed > 0 else 0
                        logging.info(f"Progress: {total_lines:,} lines processed, {total_valid:,} valid records, {rate:.0f} lines/sec")
    
        except FileNotFoundError:
            logging.error(f"Input file '{input_file_path}' not found.")
            sys.exit(1)
        except Exception as e:
            logging.error(f"Error reading input file: {e}")
            sys.exit(1)

        if not all_data:
            logging.warning("No valid file paths were processed.")
            return pd.DataFrame()
        
        elapsed = time.time() - start_time
        logging.info(f"File processing completed: {total_lines:,} lines, {total_valid:,} valid records in {elapsed:.2f}s")

        return pd.DataFrame(all_data)

    def _validate_dataframe(self, df: pd.DataFrame) -> None:
        """
        Validate DataFrame before writing to parquet.
        
        Args:
            df: DataFrame to validate
            
        Raises:
            ValueError: If validation fails
        """
        if df.empty:
            raise ValueError("DataFrame is empty")
            
        logging.info(f"DataFrame validation passed: {len(df):,} records")

    def write_optimized_parquet(self, df: pd.DataFrame, output_file: Path) -> None:
        """
        Write DataFrame to parquet with optimized settings for performance and storage.

        Args:
            df (pd.DataFrame): DataFrame to write
            output_file (str): Output file path
        """
        # Validate data before writing
        self._validate_dataframe(df)
        
        # Get schema from subclass
        schema = self.get_schema()

        # Convert DataFrame to PyArrow table with schema
        table = pa.Table.from_pandas(df, schema=schema)

        # Calculate optimal row group size
        optimal_row_group_size = min(50000, max(1000, len(df) // 10))
        
        # Write with optimized settings
        pq.write_table(
            table,
            output_file,
            compression='snappy',  # Fast compression/decompression
            row_group_size=optimal_row_group_size,  # Optimize for query performance
        )
        file_size = output_file.stat().st_size
        
        logging.info(f"Parquet file written with optimized settings:")
        logging.info(f"  - Compression: snappy")
        logging.info(f"  - Row group size: {optimal_row_group_size:,}")
        logging.info(f"  - File size: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")

    def print_stats(self, df: pd.DataFrame) -> None:
        """Print summary statistics for the processed data."""
        logging.info("Summary:")
        logging.info(f"Total records: {len(df)}")

    def generate_parquet_file(self, input_path: Path, output_path: Path) -> None:
        """
        Main method to generate parquet file from input file.
        
        Args:
            input_path (Path): Input file path
            output_path (Path): Output parquet file path
        """
        # Process the file list
        df = self.process_file_list(input_path)

        if df.empty:
            logging.error("No data to write to parquet file.")
            sys.exit(1)

        # Save to parquet with optimizations
        try:
            transformed_df = self._df_transform(df)
            self.write_optimized_parquet(transformed_df, output_path)
            logging.info(f"Successfully created optimized parquet file: {output_path}")
            self.print_stats(transformed_df)
        except Exception as e:
            logging.error(f"Error writing parquet file: {e}")
            sys.exit(1)

    def _df_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Hook for subclasses to transform DataFrame before writing.
        Args:
            df: DataFrame to transform
        Returns:
            Transformed DataFrame
        """
        return df

class GeneTissueManifestGenerator(ManifestGenerator):
    """
    Specialized manifest generator for gene-tissue data extraction.
    Extracts gene_id and tissue_id from file paths with specific pattern matching.
    """

    def __init__(self, pattern: str):
        super().__init__()
        self.pattern = re.compile(pattern)

    def extract_gene_and_tissue_id(self, file_path: str) -> tuple[str|None, str|None]:
        """
        Extract gene_id and tissue_id from file path.

        Args:
            file_path (str): File path containing gene and tissue_id
        Returns:
            tuple: (gene_id, tissue_id) or (None, None) if pattern doesn't match
        """
        match = re.search(self.pattern, file_path)
        if match:
            gene_id = match.group(1)
            tissue_id = match.group(2)
            return gene_id, tissue_id
        else:
            return None, None

    def process_line(self, line: str, line_num: int) -> dict | None:
        """
        Process a single line to extract gene and tissue information.
        
        Args:
            line (str): Input line containing file path
            line_num (int): Line number for error reporting
        Returns:
            dict | None: Record dictionary or None if line should be skipped
        """
        gene_id, tissue_id = self.extract_gene_and_tissue_id(line)

        if gene_id is None or tissue_id is None:
            logging.warning(f"Could not extract gene_id and tissue_id from line {line_num}: {line}")
            return None

        # Convert tissue_id to int32 immediately for memory efficiency
        try:
            tissue_id_int = int(tissue_id)
        except ValueError:
            logging.warning(f"Invalid tissue_id '{tissue_id}' at line {line_num}: {line}")
            return None

        return {
            'gene_id': gene_id,
            'tissue_id': tissue_id_int,
            'file_path': line,
        }

    def get_schema(self) -> pa.Schema:
        """
        Create an optimized PyArrow schema for the parquet file.
        Returns:
            pa.Schema: Optimized PyArrow schema
        """
        # Use dictionary encoding for gene_id (alphanumeric strings)
        # Use int32 for tissue_id (integers)
        return pa.schema([
            pa.field('gene_id', pa.dictionary(pa.int32(), pa.string())),
            pa.field('tissue_id', pa.int32()),
            pa.field('file_path', pa.string()),
        ])

    def _validate_dataframe(self, df: pd.DataFrame) -> None:
        """
        Enhanced validation for gene-tissue specific data.
        Args:
            df: DataFrame to validate
        Raises:
            ValueError: If validation fails
        """
        # Call parent validation
        super()._validate_dataframe(df)
        
        # Check for required columns
        required_columns = {'gene_id', 'tissue_id', 'file_path'}
        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
            
        # Check for null values
        null_counts = df.isnull().sum()
        if null_counts.any():
            logging.warning(f"Found null values: {null_counts[null_counts > 0].to_dict()}")
            
        # Validate tissue_id values
        invalid_tissues = df[~df['tissue_id'].between(-2147483648, 2147483647)]
        if len(invalid_tissues) > 0:
            raise ValueError(f"Found {len(invalid_tissues)} tissue_id values outside int32 range")
            
        # Check for duplicate file paths
        duplicate_count = df['file_path'].duplicated().sum()
        if duplicate_count > 0:
            logging.warning(f"Found {duplicate_count} duplicate file paths")

    def _df_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Hook for subclasses to transform DataFrame before writing.
        Args:
            df: DataFrame to transform
        Returns:
            Transformed DataFrame
        """
        # Data sorted by gene_id for optimal row group clustering
        return df.sort_values('gene_id')

    def print_stats(self, df: pd.DataFrame) -> None:
        """Print gene-tissue specific statistics."""
        super().print_stats(df)
        logging.info(f"Unique genes: {df['gene_id'].nunique()}")
        logging.info(f"Unique tissues: {df['tissue_id'].nunique()}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate parquet file from list of file paths",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
    Example usage:
    python generate_parquet.py ../data/list_of_paths output.parquet
        """
    )
    parser.add_argument(
        "input_file",
        help="Path to the input file containing file paths",
    )
    parser.add_argument(
        'output_file',
        nargs='?',
        default="../data/manifest.parquet",
        help='Output parquet file path (default: manifest.parquet)'
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    
    input_path = Path(args.input_file)
    output_file = args.output_file
    # Determine output file name
    if output_file is None:
        output_file = input_path.with_suffix('.parquet')

    logging.info(f"Processing file: {input_path}")
    logging.info(f"Output will be saved to: {output_file}")

    manifest_generator = GeneTissueManifestGenerator(
        pattern=r'.*/v1/ad/([^/]+)/model_tissue_(\d+)\.tl$'
    )
    manifest_generator.generate_parquet_file(input_path, Path(output_file))


if __name__ == "__main__":
    main()
