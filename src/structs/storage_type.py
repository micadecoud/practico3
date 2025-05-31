from enum import Enum


class StorageType(Enum):
    """Enum for different storage types"""

    LOCAL = "local"
    S3 = "s3"