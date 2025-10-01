#!/usr/bin/env python3
"""
Script to generate a parquet file from a list of file paths.
Extracts gene_id and tissue_id from file paths with the structure:
foo...bar/predictions_dna2cell_v4_pcg/{gene_id}/model_tissue_{tissue_id}.tl
"""

import logging
import pandas as pd
import re
import argparse
import sys
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq


logger = logging.getLogger(__name__)

class ManifestGenerator:

    def __init__(self, pattern: str):
        self.pattern = re.compile(pattern)

    def extract_gene_and_tissue_id(self, file_path: str) -> tuple[str|None, str|None]:
        """
        Extract gene_id and tissue_id from file path.

        Args:
            file_path (str): File path with structure alzhimers_disease/v1/ad/{gene_id}/model_tissue_{tissue_id}.tl

        Returns:
            tuple: (gene_id, tissue_id) or (None, None) if pattern doesn't match
        """
        # Pattern to match the file structure
        match = re.search(self.pattern, file_path)
        if match:
            gene_id = match.group(1)
            tissue_id = match.group(2)
            return gene_id, tissue_id
        else:
            return None, None


    def process_file_list(self, input_file_path: Path) -> pd.DataFrame:
        """
        Process the input file containing file paths and extract data.

        Args:
            input_file_path (Path): Path to the input file containing file paths

        Returns:
            pd.DataFrame: DataFrame with columns gene_id, tissue_id
        """
        data = []

        try:
            with open(input_file_path, 'r') as file:
                for line_num, line in enumerate(file, 1):
                    file_path = line.strip()

                    # Skip empty lines
                    if not file_path:
                        continue

                    gene_id, tissue_id = self.extract_gene_and_tissue_id(file_path)

                    if gene_id is None or tissue_id is None:
                        logging.warning(f"Could not extract gene_id and tissue_id from line {line_num}: {file_path}")
                        continue

                    data.append({
                        'gene_id': gene_id,
                        'tissue_id': tissue_id,
                        "file_path": file_path,
                    })
    
        except FileNotFoundError:
            logging.error(f"Input file '{input_file_path}' not found.")
            sys.exit(1)
        except Exception as e:
            logging.error(f"Error reading input file: {e}")
            sys.exit(1)

        if not data:
            logging.warning("No valid file paths were processed.")

        return pd.DataFrame(data, columns=['gene_id', 'tissue_id', 'file_path'])

    @classmethod
    def create_optimized_parquet_schema(cls) -> pa.Schema:
        """
        Create an optimized PyArrow schema for the parquet file.
        Returns:
            pa.Schema: Optimized PyArrow schema
        """
        # Use dictionary encoding for gene_id (alphanumeric strings)
        # Use int32 for tissue_id (integers)
        return pa.schema([
            pa.field('gene_id', pa.string()),
            pa.field('tissue_id', pa.int32()),
            pa.field('file_path', pa.string()),
        ])

    @classmethod
    def write_optimized_parquet(cls, df: pd.DataFrame, output_file: Path) -> None:
        """
        Write DataFrame to parquet with optimized settings for performance and storage.

        Args:
            df (pd.DataFrame): DataFrame to write
            output_file (str): Output file path
        """
        # Convert tissue_id to int32 for optimal storage
        df_optimized = df.copy()
        df_optimized['tissue_id'] = df_optimized['tissue_id'].astype('int32')

        # Create optimized schema
        schema = cls.create_optimized_parquet_schema()

        # Convert DataFrame to PyArrow table with schema
        table = pa.Table.from_pandas(df_optimized, schema=schema)

        # Write with optimized settings
        pq.write_table(
            table,
            output_file,
            compression='snappy',  # Fast compression/decompression
        )
        logging.info(f"Parquet file written with optimized settings:")
        logging.info(f"  - Compression: snappy")
        logging.info(f"  - Dictionary encoding: enabled for gene_id")
        logging.info(f"  - Row group size: {min(100000, len(df_optimized)):,}")

    @classmethod
    def print_stats(cls, df: pd.DataFrame) -> None:
        logging.info("Summary:")
        logging.info(f"Total records: {len(df)}")
        logging.info(f"Unique genes: {df['gene_id'].nunique()}")
        logging.info(f"Unique tissues: {df['tissue_id'].nunique()}")

    def generate_parquet_file(self, input_path: Path, output_path: Path) -> None:
        # Process the file list
        df = self.process_file_list(input_path)

        if df.empty:
            logging.error("No data to write to parquet file.")
            sys.exit(1)

        # Save to parquet with optimizations
        try:
            self.write_optimized_parquet(df, output_path)
            logging.info(f"Successfully created optimized parquet file: {output_path}")
            self.print_stats(df)
        except Exception as e:
            logging.error(f"Error writing parquet file: {e}")
            sys.exit(1)


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
        type=str,
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
    
    input_path = args.input_file
    output_file = args.output_file
    # Determine output file name
    if output_file is None:
        output_file = input_path.with_suffix('.parquet')

    logging.info(f"Processing file: {input_path}")
    logging.info(f"Output will be saved to: {output_file}")

    manifest_generator = ManifestGenerator(
        pattern=r'.*/v1/ad/([^/]+)/model_tissue_(\d+)\.tl$'
    )
    manifest_generator.generate_parquet_file(input_path, Path(output_file))


if __name__ == "__main__":
    main()
