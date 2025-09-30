import aioboto3
from botocore.config import Config
from config import settings

_S3_CLIENT = None
_SESSION = None

def get_session():
    global _SESSION
    if _SESSION is None:
        _SESSION = aioboto3.Session()
    return _SESSION

async def get_s3():
    """
    Singleton aioboto3 S3 client.
    Call with: async with await get_s3() as s3: ...
    """
    global _S3_CLIENT
    if _S3_CLIENT is None:
        cfg = Config(
            region_name=settings.S3_REGION,
            retries={"max_attempts": 10, "mode": "standard"},
            connect_timeout=5, read_timeout=60,
            max_pool_connections=50,
            s3={"addressing_style": "virtual"},
        )
        # we build a client each time but reuse the session
        session = get_session()
        _S3_CLIENT = session.client("s3", config=cfg)
    return _S3_CLIENT