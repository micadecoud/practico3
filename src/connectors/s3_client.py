import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import logging
from typing import Optional, BinaryIO, List, Dict
import json
import io
import os


class S3Client:
    def __init__(
        self,
        bucket_name: str,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        region_name: str = "us-east-1",
    ):
        """
        Initialize S3 client.

        Args:
            bucket_name (str): Name of the S3 bucket
            aws_access_key_id (str, optional): AWS access key ID
            aws_secret_access_key (str, optional): AWS secret access key
            region_name (str): AWS region name
        """
        self.bucket_name = bucket_name

        # Try to get credentials from environment variables if not provided
        if not aws_access_key_id:
            aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        if not aws_secret_access_key:
            aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        if not aws_session_token:
            aws_session_token = os.getenv("AWS_SESSION_TOKEN")

        # Log credential status (without exposing actual values)
        logging.info(
            f"AWS Access Key ID present: {'Yes' if aws_access_key_id else 'No'}"
        )
        logging.info(
            f"AWS Secret Access Key present: {'Yes' if aws_secret_access_key else 'No'}"
        )
        logging.info(
            f"AWS Session Token present: {'Yes' if aws_session_token else 'No'}"
        )
        logging.info(f"Using region: {region_name}")
        logging.info(f"Using bucket: {bucket_name}")

        if not aws_access_key_id or not aws_secret_access_key:
            logging.error(
                "AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables."
            )
            raise NoCredentialsError()

        try:
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                region_name=region_name,
            )
            # Test the connection
            logging.info("Testing S3 connection...")
            self.s3_client.head_bucket(Bucket=bucket_name)
            logging.info("Successfully connected to S3 bucket")
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "403":
                logging.error(
                    f"Access denied to bucket {bucket_name}. Please check your credentials and permissions."
                )
            elif error_code == "404":
                logging.error(
                    f"Bucket {bucket_name} not found. Please check the bucket name."
                )
            else:
                logging.error(f"Error connecting to S3: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error initializing S3 client: {e}")
            raise

    def upload_image(
        self, file_obj: BinaryIO, key: str, content_type: str = "image/jpeg"
    ) -> bool:
        """
        Upload an image to S3.

        Args:
            file_obj (BinaryIO): File object containing the image data
            key (str): S3 object key (path where the file will be stored)
            content_type (str): MIME type of the file

        Returns:
            bool: True if upload was successful, False otherwise
        """
        try:
            self.s3_client.upload_fileobj(
                file_obj, self.bucket_name, key, ExtraArgs={"ContentType": content_type}
            )
            return True
        except ClientError as e:
            logging.error(f"Error uploading file to S3: {e}")
            return False

    def save_jsonl(self, data: List[Dict], key: str) -> bool:
        """
        Save a list of dictionaries as a JSONL file to S3.

        Args:
            data (List[Dict]): List of dictionaries to save as JSONL
            key (str): S3 object key (path where the file will be stored)

        Returns:
            bool: True if upload was successful, False otherwise
        """
        try:
            # Convert data to JSONL format (one JSON object per line)
            jsonl_content = (
                "\n".join(json.dumps(item, ensure_ascii=False) for item in data) + "\n"
            )
            jsonl_bytes = jsonl_content.encode("utf-8")
            jsonl_file = io.BytesIO(jsonl_bytes)

            # Ensure the key ends with .jsonl
            if not key.endswith(".jsonl"):
                key = f"{key}.jsonl"

            # Upload to S3
            self.s3_client.upload_fileobj(
                jsonl_file,
                self.bucket_name,
                key,
                ExtraArgs={"ContentType": "application/json"},
            )
            return True
        except ClientError as e:
            logging.error(f"Error saving JSONL file to S3: {e}")
            return False

    def download_image(self, key: str) -> Optional[BinaryIO]:
        """
        Download an image from S3.

        Args:
            key (str): S3 object key (path of the file to download)

        Returns:
            Optional[BinaryIO]: File object containing the image data if successful, None otherwise
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            return response["Body"]
        except ClientError as e:
            logging.error(f"Error downloading file from S3: {e}")
            return None

    def delete_image(self, key: str) -> bool:
        """
        Delete an image from S3.

        Args:
            key (str): S3 object key (path of the file to delete)

        Returns:
            bool: True if deletion was successful, False otherwise
        """
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as e:
            logging.error(f"Error deleting file from S3: {e}")
            return False

    def empty_bucket(self) -> bool:
        """
        Delete all objects in the S3 bucket.

        Returns:
            bool: True if all objects were deleted successfully, False otherwise
        """
        try:
            # List all objects in the bucket
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket_name)

            # Delete all objects
            for page in pages:
                if "Contents" in page:
                    objects_to_delete = [
                        {"Key": obj["Key"]} for obj in page["Contents"]
                    ]
                    if objects_to_delete:
                        self.s3_client.delete_objects(
                            Bucket=self.bucket_name,
                            Delete={"Objects": objects_to_delete},
                        )

            logging.info(f"Successfully emptied bucket {self.bucket_name}")
            return True
        except ClientError as e:
            logging.error(f"Error emptying bucket {self.bucket_name}: {e}")
            return False