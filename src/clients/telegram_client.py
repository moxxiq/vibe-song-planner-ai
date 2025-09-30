
from telethon import TelegramClient
from telethon.sessions import StringSession
from config import settings
import sys

_client = None

async def get_telegram_client():
    global _client
    if _client and _client.is_connected():
        return _client

    api_id = settings.TELEGRAM_API_ID
    api_hash = settings.TELEGRAM_API_HASH
    session = settings.TELEGRAM_SESSION

    # Initialize client
    client = TelegramClient(StringSession(session), api_id, api_hash)
    await client.connect()
    
    if not client.is_connected():
        print("Failed to connect to Telegram.", file=sys.stderr)
        return None
    
    # Handle authorization
    if not await client.is_user_authorized():
        print(f"Session is not authorized.")
        await client.disconnect()
        return None

    _client = client
    return _client
