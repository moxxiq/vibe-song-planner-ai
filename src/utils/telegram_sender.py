import configparser
import os
import sys
import datetime
from datetime import datetime, timedelta
from typing import Tuple, Optional, List, Dict, Any

from telethon import TelegramClient
from telethon.utils import get_display_name
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, FloodWaitError, ChatWriteForbiddenError
from telethon.tl.types import MessageEntityCustomEmoji, MessageEntityTextUrl
from telethon.tl.custom.message import Message as CustomMessage
from telethon.tl.functions.messages import SendMessageRequest
import pytz


# Define Kyiv timezone (UTC+3)
KYIV_TZ = pytz.timezone('Europe/Kyiv')

# Premium emoji document IDs
PREMIUM_EMOJI_SPOTIFY_DOCUMENT_ID = 5467890660872822948
PREMIUM_EMOJI_YM_DOCUMENT_ID = 5467547888122864018

# Emoji placeholders - different for each service
SPOTIFY_EMOJI_PLACEHOLDER = "🎵"  # Emoji placeholder for Spotify
YM_EMOJI_PLACEHOLDER = "📹"  # Emoji placeholder for YouTube Music


class MessageFormatter:
    """Helper class to format song messages with premium emoji entities and text URL links."""
    
    @staticmethod
    def format_song_message(song_data: Dict[str, str]) -> Tuple[str, List[Any]]:
        """
        Format song message with premium emoji entities and text URL entities.
        
        Args:
            song_data: Dictionary containing song information
            
        Returns:
            tuple: (message_text, entities)
        """
        # Start with the song title
        message_text = f"{song_data['Artist - Song']}\n"
        
        # Add Spotify text and emoji placeholder
        spotify_text_offset = len(message_text)
        message_text += "Spotify "
        spotify_emoji_offset = len(message_text)
        message_text += f"{SPOTIFY_EMOJI_PLACEHOLDER}\n"
        
        # Add YouTube Music text and emoji placeholder
        ym_text_offset = len(message_text)
        message_text += "YouTube Music "
        ym_emoji_offset = len(message_text)
        message_text += f"{YM_EMOJI_PLACEHOLDER}"
        
        # Create entities for text URLs and premium emojis
        entities = [
            MessageEntityTextUrl(
                offset=spotify_text_offset,
                length=len("Spotify"),
                url=song_data['Spotify Link']
            ),
            MessageEntityCustomEmoji(
                offset=spotify_emoji_offset,
                length=2,  # Length 2 for emoji entity
                document_id=PREMIUM_EMOJI_SPOTIFY_DOCUMENT_ID
            ),
            MessageEntityTextUrl(
                offset=ym_text_offset + 1, # emoji length
                length=len("YouTube Music"),
                url=song_data['YouTube Music Link']
            ),
            MessageEntityCustomEmoji(
                offset=ym_emoji_offset + 1, # emoji length
                length=2,  # Length 2 for emoji entity
                document_id=PREMIUM_EMOJI_YM_DOCUMENT_ID
            )
        ]
        
        return message_text, entities


async def initialize_telegram_client(config: configparser.ConfigParser) -> Optional[TelegramClient]:
    """Initializes and returns a Telegram client using Telethon."""
    try:
        # Get API credentials from environment or config
        api_id = os.getenv('TELEGRAM_API_ID') or int(config['TELEGRAM']['api_id'])
        api_hash = os.getenv('TELEGRAM_API_HASH') or config['TELEGRAM']['api_hash']
        session_name = os.getenv('TELEGRAM_SESSION_NAME') or config['TELEGRAM'].get('session_name', 'music_scheduler_session')
        
        # Initialize client
        client = TelegramClient(session_name, api_id, api_hash)
        await client.connect()
        
        if not client.is_connected():
            print("Failed to connect to Telegram.", file=sys.stderr)
            return None
        
        # Handle authorization
        if not await client.is_user_authorized():
            print(f"Session is not authorized.")
            await client.disconnect()
            return None
        
        print("Telegram client successfully authorized.")
        return client
    except KeyError as e:
        print(f"Error: Missing Telegram configuration in config.ini or environment variables. Error: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error initializing Telegram client: {e}", file=sys.stderr)
        return None


async def send_scheduled_message(
    client: TelegramClient, 
    group_id: str, 
    song_data: Dict[str, str]
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Sends a scheduled message to a Telegram group with song information and premium emoji.
    Then schedules file sending 1 minute after the message.
    
    Returns:
        Tuple containing scheduled message time and file time (if applicable)
    """
    # Parse scheduled time in Kyiv timezone
    scheduled_time_str = song_data['date scheduled']
    date_format = "%d.%m.%Y %H:%M:%S"
    
    try:
        # Parse naive datetime then localize to Kyiv timezone
        naive_scheduled_time = datetime.strptime(scheduled_time_str, date_format)
        scheduled_time = KYIV_TZ.localize(naive_scheduled_time)
    except ValueError:
        print(f"Error: Invalid date format in CSV for song '{song_data['Artist - Song']}'. Expected format: {date_format}", file=sys.stderr)
        return None, None
    
    # Compare with current time in Kyiv timezone
    now = datetime.now(KYIV_TZ)
    if scheduled_time <= now:
        print(f"Warning: Scheduled time for '{song_data['Artist - Song']}' is in the past. Skipping.", file=sys.stderr)
        return None, None
    
    print(f"Scheduling message for '{song_data['Artist - Song']}' to send at {scheduled_time} (Kyiv time).")
    scheduled_file_time = scheduled_time + timedelta(minutes=1)
    
    # Format the message with premium emoji entities and text URL entities
    message_text, entities = MessageFormatter.format_song_message(song_data)
    
    try:
        # Get input entity
        input_entity = await client.get_input_entity(group_id)
        
        # Send message with entities using the raw API method
        result = await client(SendMessageRequest(
            peer=input_entity,
            message=message_text,
            entities=entities,
            schedule_date=scheduled_time,
            no_webpage=False  # Enable link preview
        ))
        
        print(f"Message scheduled for '{song_data['Artist - Song']}' to Telegram group for {scheduled_time}.")
    except Exception as e:
        print(f"Error scheduling Telegram message for '{song_data['Artist - Song']}'. Error: {e}", file=sys.stderr)
        
        # Try alternative approach if the first one fails
        try:
            # Fallback to sending without custom emoji if needed
            plain_message = (
                f"{song_data['Artist - Song']}\n"
                f"[Spotify]({song_data['Spotify Link']})\n"
                f"[YouTube Music]({song_data['YouTube Music Link']})"
            )
            
            await client.send_message(
                entity=group_id,
                message=plain_message,
                schedule=scheduled_time,
                link_preview=True,
                parse_mode='markdown'
            )
            print(f"Message scheduled (without premium emoji) for '{song_data['Artist - Song']}'.")
        except Exception as e2:
            print(f"Failed to schedule message with fallback method: {e2}", file=sys.stderr)
            return None, None
    
    # Handle file attachment using advanced file matcher
    file_to_send_path = AudioFileMatcher.find_matching_file(song_data)
    if file_to_send_path:
        try:
            await client.send_file(
                entity=group_id,
                file=file_to_send_path,
                schedule=scheduled_file_time
            )
            print(f"File '{os.path.basename(file_to_send_path)}' scheduled for {scheduled_file_time}.")
        except Exception as e:
            print(f"Error scheduling file for '{song_data['Artist - Song']}'. Error: {e}", file=sys.stderr)
            return scheduled_time, None
    else:
        print(f"No suitable file found for '{song_data['Artist - Song']}'.")
        return scheduled_time, None
    
    return scheduled_time, scheduled_file_time
