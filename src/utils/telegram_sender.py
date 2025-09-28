import configparser
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

from config import settings


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


async def initialize_telegram_client() -> Optional[TelegramClient]:
    """Initializes and returns a Telegram client using Telethon."""
    try:
        # Get API credentials from environment or config
        api_id = settings.TELEGRAM_API_ID
        api_hash = settings.TELEGRAM_API_HASH
        session_name = settings.TELEGRAM_SESSION_NAME or 'music_scheduler_session'
        
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
    scheduled_time_str = song_data['scheduled_at']
    date_format = "%d.%m.%Y %H:%M:%S"
    
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
        
        print(f"Message scheduled for '{song_data['artist']} - {song_data['title']}' to Telegram group for {scheduled_time}.")
    except Exception as e:
        print(f"Error scheduling Telegram message for '{song_data['artist']} - {song_data['title']}'. Error: {e}", file=sys.stderr)
        
        # Try alternative approach if the first one fails
        try:
            # Fallback to sending without custom emoji if needed
            plain_message = (
                f"{song_data['artist']} - {song_data['title']}\n"
                f"[Spotify]({song_data['spotify_link']})\n"
                f"[YouTube Music]({song_data['youtube_music_link']})"
            )
            
            await client.send_message(
                entity=group_id,
                message=plain_message,
                schedule=scheduled_time,
                link_preview=True,
                parse_mode='markdown'
            )
            print(f"Message scheduled (without premium emoji) for '{song_data['artist']} - {song_data['title']}'.")
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
            print(f"Error scheduling file for '{song_data['artist']} - {song_data['title']}'. Error: {e}", file=sys.stderr)
            return scheduled_time, None
    else:
        print(f"No suitable file found for '{song_data['artist']} - {song_data['title']}'.")
        return scheduled_time, None
    
    return scheduled_time, scheduled_file_time


async def main() -> None:
    """Main function to run the scheduled Telegram message sending application."""
    try:
        print(f"Starting Telegram Song Scheduler (Kyiv timezone: UTC+3)")
        print(f"Current time in Kyiv: {datetime.now(KYIV_TZ).strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Load configuration and initialize client
        config = load_configuration() # TODO: use settings instead of config, rewrite this in the next function
        telegram_client = await initialize_telegram_client(config)

        if not telegram_client:
            print("Failed to initialize Telegram client. Exiting.", file=sys.stderr)
            return

        # Get user and group information
        me = await telegram_client.get_me()
        print(f"Logged in as {get_display_name(me)}")
        
        # Get group ID from config
        group_id = config.getint('TELEGRAM', 'group_id')
        
        # Test premium emoji functionality
        # premium_emoji_works = await test_premium_emoji(telegram_client)
 
        # Read songs to schedule
        songs_to_schedule = read_songs_from_csv()
        if not songs_to_schedule:
            print("No songs to schedule found in CSV or CSV file missing. Exiting.", file=sys.stderr)
            await telegram_client.disconnect()
            return
            
        print(f"Found {len(songs_to_schedule)} songs to schedule.")
        

        for index, song_data in enumerate(songs_to_schedule, 1):
            print(f"\nProcessing song {index}/{len(songs_to_schedule)}: {song_data['artist']} - {song_data['title']}")
            message_sent_time, file_sent_time = await send_scheduled_message(
                telegram_client, group_id, song_data
            )
    except Exception as e:
        print(f"Unexpected error in main function: {e}", file=sys.stderr)
    finally:
        # Ensure client is disconnected
        if 'telegram_client' in locals() and telegram_client:
            await telegram_client.disconnect()
            print("Telegram client disconnected.")
        print("Scheduler completed.")


def schedule_one_message(song_data: Dict[str, str]):
    # TODO: implement sending data that are ready to be sent
    pass
