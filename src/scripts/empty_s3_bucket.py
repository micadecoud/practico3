import logging

import sys
import os

sys.path.append(os.getcwd())

from src.settings import load_settings, custom_logger
from src.connectors.s3_client import S3Client


def main():
    # Set up logging
    logger = custom_logger("empty_s3_bucket")

    try:
        # Load storage configuration
        storage_config = load_settings("Storage")

        if storage_config["Type"] != "s3":
            logger.error("Storage type is not S3. Please check your configuration.")
            return

        # Initialize S3 client
        s3_client = S3Client(
            bucket_name=storage_config["S3"]["Bucket"],
            region_name=storage_config["S3"]["Region"],
        )

        # Empty the bucket
        logger.info(f"Starting to empty bucket {storage_config['S3']['Bucket']}")
        success = s3_client.empty_bucket()

        if success:
            logger.info("Successfully emptied the bucket")
        else:
            logger.error("Failed to empty the bucket")

    except Exception as e:
        logger.error(f"Error: {str(e)}")


if __name__ == "__main__":
    main()