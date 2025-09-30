import sys
import datetime
from datetime import datetime, timedelta
from typing import Tuple, Optional, List, Dict, Any

from telethon import TelegramClient
from telethon.utils import get_display_name
from telethon.tl.types import MessageEntityCustomEmoji, MessageEntityTextUrl
from telethon.tl.custom.message import Message as CustomMessage
from telethon.tl.functions.messages import SendMessageRequest
import pytz
from mutagen.mp3 import MP3

from config import settings
from clients.telegram_client import get_telegram_client


# Define Kyiv timezone (UTC+3)
KYIV_TZ = pytz.timezone('Europe/Kyiv')

# Premium emoji document IDs
PREMIUM_EMOJI_SPOTIFY_DOCUMENT_ID = 5467890660872822948
PREMIUM_EMOJI_YM_DOCUMENT_ID = 5467547888122864018

# Emoji placeholders - different for each service
SPOTIFY_EMOJI_PLACEHOLDER = "🎵"  # Emoji placeholder for Spotify
YM_EMOJI_PLACEHOLDER = "📹"  # Emoji placeholder for YouTube Music

def probe_mp3_duration(buf: BytesIO) -> int | None:
    """
    Optional: get duration (seconds) from the MP3 in memory for nicer Telegram UI.
    Mutagen is sync, but it's fast and works on BytesIO.
    """
    try:
        buf.seek(0)
        audio = MP3(buf)
        dur = int(audio.info.length) if audio and audio.info else None
        buf.seek(0)
        return dur
    except Exception:
        buf.seek(0)
        return None

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
        message_text = f"{song_data['artist']} - {song_data['title']}\n"
        
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
                url=song_data['spotify_link']
            ),
            MessageEntityCustomEmoji(
                offset=spotify_emoji_offset,
                length=2,  # Length 2 for emoji entity
                document_id=PREMIUM_EMOJI_SPOTIFY_DOCUMENT_ID
            ),
            MessageEntityTextUrl(
                offset=ym_text_offset + 1, # emoji length
                length=len("YouTube Music"),
                url=song_data['youtube_music_link']
            ),
            MessageEntityCustomEmoji(
                offset=ym_emoji_offset + 1, # emoji length
                length=2,  # Length 2 for emoji entity
                document_id=PREMIUM_EMOJI_YM_DOCUMENT_ID
            )
        ]
        
        return message_text, entities


async def send_scheduled_message(
    client: TelegramClient, 
    group_id: str, 
    song_data: Dict[str, str],
    audio_file: BytesIO
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Sends a scheduled message to a Telegram group with song information and premium emoji.
    Then schedules file sending 1 minute after the message.
    
    Returns:
        Tuple containing scheduled message time and file time (if applicable)
    """
    # Parse scheduled time in Kyiv timezone
    scheduled_time_str = song_data['scheduled_at']
    date_format = "%d.%m.%Y %H:%M:%S"
    duration = probe_mp3_duration(audio_file)

    try:
        # Parse naive datetime then localize to Kyiv timezone
        naive_scheduled_time = datetime.strptime(scheduled_time_str, date_format)
        scheduled_time = KYIV_TZ.localize(naive_scheduled_time)
    except ValueError:
        print(f"Error: Invalid date format in CSV for song '{song_data['artist']} - {song_data['title']}'. Expected format: {date_format}", file=sys.stderr)
        return None, None
    
    # Compare with current time in Kyiv timezone
    now = datetime.now(KYIV_TZ)
    if scheduled_time <= now:
        print(f"Warning: Scheduled time for '{song_data['artist']} - {song_data['title']}' is in the past. Skipping.", file=sys.stderr)
        return None, None
    
    print(f"Scheduling message for '{song_data['artist']} - {song_data['title']}' to send at {scheduled_time} (Kyiv time).")
    scheduled_file_time = scheduled_time + timedelta(minutes=1)
    
    # Format the message with premium emoji entities and text URL entities
    message_text, entities = MessageFormatter.format_song_message(song_data)

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
    
    print(f"Message scheduled for '{song_data['artist']} - {song_data['title']}' to Telegram group for {scheduled_time}.")

    attrs = [DocumentAttributeAudio(
        duration=duration or 0,
        voice=False,
        title=title,
        performer=performer,
    )]

    await client.send_file(
        entity=chat,
        file=audio_file, # in-memory file
        caption=caption,
        attributes=attrs,
        mime_type="audio/mpeg",
        file_name=f"{song_data['artist']} - {song_data['title']}.mp3",
        force_document=False,         # let Telegram render as audio with player
        allow_cache=False,            # avoid Telethon caching to disk
        schedule=scheduled_file_time,
    )
    print(f"File scheduled for {scheduled_file_time}.")
    
    return scheduled_time, scheduled_file_time


async def schedule_one_message(song_data: Dict[str, str], audio_file: BytesIO):
    telegram_client = await get_telegram_client()

    if not telegram_client:
        print("Failed to initialize Telegram client. Exiting.", file=sys.stderr)
        return

    # Get user and group information
    me = await telegram_client.get_me()
    print(f"Logged in as {get_display_name(me)}")
    
    # Get group ID from config
    group_id = settings.TELEGRAM_GROUP_ID

    print(f"\nProcessing song : {song_data['artist']} - {song_data['title']}")
    message_sent_time, file_sent_time = await send_scheduled_message(
        telegram_client, group_id, song_data, audio_file
    )
