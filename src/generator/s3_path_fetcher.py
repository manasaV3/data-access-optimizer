#!/usr/bin/env python3
"""
Script to list S3 files in a prefix, save paths to a file.
"""
import logging

import boto3
from typing import List, Optional
import argparse

logger = logging.getLogger(__name__)

class S3PathProcessor:
    def __init__(self, aws_profile: Optional[str] = None):
        """
        Initialize S3 client and bucket.
        
        Args:
            aws_profile: AWS profile to use (optional)
        """
        if aws_profile:
            session = boto3.Session(profile_name=aws_profile)
            self.s3_client = session.client('s3')
        else:
            self.s3_client = boto3.client('s3')

    def fetch_file_paths_list(self, bucket_name: str, prefix: str, output_file: str):
        logger.info(f"Listing files in s3://{bucket_name}/{prefix}")

        file_paths = self._list_files(bucket_name, prefix)
        logger.info(f"Found {len(file_paths)} files")
        if not file_paths:
            logger.warning("No files found!")
            return
        self._write_to_file(file_paths, output_file)
    
    def _list_files(self, bucket_name: str, prefix: str, file_extension: Optional[str] = None) -> list[str]:
        """
        List all files in the given S3 prefix.
        
        Args:
            bucket_name: S3 bucket name
            prefix: S3 prefix to search in
            file_extension: Optional filter for file extension (e.g., '.csv')
        
        Returns:
            list of S3 object keys
        """
        files = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    # Skip directories (keys ending with /)
                    if not key.endswith('/'):
                        if file_extension is None or key.endswith(file_extension):
                            files.append(key)
        
        return files
    
    def _write_to_file(self, paths: list[str], output_file: str) -> None:
        """
        Write S3 paths to a text file.
        Args:
            paths: List of S3 paths
            output_file: Output file path
        """
        with open(output_file, 'w') as f:
            for path in paths:
                f.write(f"{path}\n")
        logger.info(f"Written {len(paths)} paths to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description='List S3 files and extract path variables to parquet'
    )
    parser.add_argument('bucket', help='S3 bucket name')
    parser.add_argument('prefix', default='', help='S3 prefix to search')
    parser.add_argument(
        '--paths-file',
        default='../data/s3_paths.txt',
        help='Output file for S3 paths (default: s3_paths.txt)'
    )
    parser.add_argument(
        '--aws-profile',
        default=None,
        help='AWS profile to use'
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    processor = S3PathProcessor(args.aws_profile)
    processor.fetch_file_paths_list(
        bucket_name=args.bucket,
        prefix=args.prefix,
        output_file=args.paths_file
    )

if __name__ == "__main__":
    main()
