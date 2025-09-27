import os
from pymongo.synchronous.database import Database
from pymongo import MongoClient

_CLIENT = None

def get_client() -> MongoClient:
    global _CLIENT
    if _CLIENT is None:
        uri = os.environ["MONGODB_URI"]
        _CLIENT = MongoClient(uri, retryWrites=True)
    return _CLIENT

def get_db() -> Database:
    return get_client()[os.environ.get("DB_NAME", "vibe_songs")]
