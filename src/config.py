from pydantic import BaseSettings, Field

class Settings(BaseSettings):
    DB_URL: str = "sqlite+sqlite:///./vibe.db"
    TELEGRAM_API_ID: int
    TELEGRAM_API_HASH: str
    TELEGRAM_CHAT_ID: int 
    TELEGRAM_SESSION: str
    DEFAULT_CRON: str = "0 10,20 * * *"
    SPOTIFY_CLIENT_ID: str | None = None
    SPOTIFY_CLIENT_SECRET: str | None = None
    YTM_COOKIE_FILE: str | None = None
    S3_BUCKET: str = "vibe-songs"
    S3_REGION: str = "us-east-1"
    AUDIO_ROOT: str = "/tmp/audio"

settings = Settings()