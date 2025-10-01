import logging
import os
from pathlib import Path

from s3_path_fetcher import S3PathProcessor
from parquet_indexer import GeneTissueManifestGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)



def generate_manifest(source_bucket: str, source_prefix: str, output_file: str, aws_profile: str = None):

    s3_path_processor = S3PathProcessor(aws_profile)

    tmp_dir = f"/tmp/data_lookup_v1/{source_bucket}/{source_prefix}/"
    os.makedirs(tmp_dir)

    s3_files_list_path = f"{tmp_dir}/s3_files_list.txt"
    s3_path_processor.fetch_file_paths_list(
        bucket_name=source_bucket,
        prefix=source_prefix,
        output_file=s3_files_list_path
    )

    manifest_generator =  GeneTissueManifestGenerator(
        pattern=r'.*/v1/ad/([^/]+)/model_tissue_(\d+)\.tl$'
    )
    manifest_generator.generate_parquet_file(
        input_path=Path(s3_files_list_path),
        output_path=Path(output_file)
    )
