import os
from pymongo.synchronous.database import Database
from pymongo import AsyncMongoClient
from config import settings

_CLIENT = None

def get_client() -> AsyncMongoClient:
    global _CLIENT
    if _CLIENT is None:
        uri = settings.MONGODB_URI
        _CLIENT = AsyncMongoClient(uri, retryWrites=True)
    return _CLIENT

def get_db() -> Database:
    return get_client()[settings.DB_NAME]
