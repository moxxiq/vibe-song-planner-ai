from pydantic import BaseSettings, Field

class Settings(BaseSettings):
    DB_URL: str = "sqlite+sqlite:///./vibe.db"
    TELEGRAM_API_ID: int
    TELEGRAM_API_HASH: str
    TELEGRAM_BOT_TOKEN: str | None = None      # if using a bot
    TELEGRAM_CHAT_ID: int                      # target chat
    DEFAULT_CRON: str = "* */12 * * *"          # check 
    SPOTIFY_CLIENT_ID: str | None = None
    SPOTIFY_CLIENT_SECRET: str | None = None
    YTM_COOKIE_FILE: str | None = None
    S3_BUCKET: str = "vibe-songs"
    S3_REGION: str = "us-east-1"
    class Config: env_file = ".env"

settings = Settings()