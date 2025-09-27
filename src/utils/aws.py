import boto3
from botocore.config import Config
from config import settings

_S3 = None
_SESSION = None
_TRANSFER_CFG = None

def get_session():
    global _SESSION
    if _SESSION is None:
        _SESSION = boto3.session.Session()
    return _SESSION

def get_s3():
    """Singleton S3 client (reused across warm invocations)."""
    global _S3
    if _S3 is None:
        cfg = Config(
            retries={"max_attempts": 10, "mode": "standard"},
            max_pool_connections=50,
            s3={"addressing_style": "virtual"},
            region_name=settings.S3_REGION,
        )
        _S3 = get_session().client("s3", config=cfg)
    return _S3
