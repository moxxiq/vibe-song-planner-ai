import os, time
import boto3
import tempfile
from datetime import datetime, timezone, timedelta
from db import get_db
from matcher.audio_matcher import normalize_string
from config import settings

AUDIO_ROOT = os.environ.get("AUDIO_ROOT", "/tmp/audio")
THRESHOLD = 85

# S3 configuration moved to config.py


def download_track(db, track):
    """Download track using some legal way and upload to S3"""
    
    track_id = str(track["_id"])
    artist = track.get("artist", "Unknown")
    title = track.get("title", "Unknown")
    
    # Create a safe filename
    safe_filename = f"{artist} - {title}".replace("/", "_").replace("\\", "_")
    safe_filename = "".join(c for c in safe_filename if c.isalnum() or c in " -_.")[:100]
    
    # Generate unique S3 key
    s3_key = f"tracks/{track_id}/{safe_filename}.mp3"
    
    try:
        # Create temporary directory for download
        with tempfile.TemporaryDirectory() as temp_dir:
            # TODO Download using some legal way
            
            # Find the downloaded file
            downloaded_files = [f for f in os.listdir(temp_dir) if f.endswith('.mp3')]
            if not downloaded_files:
                raise RuntimeError("No MP3 file was downloaded")
            
            local_file_path = os.path.join(temp_dir, downloaded_files[0])
            
            # Upload to S3
            s3_client = boto3.client('s3', region_name=settings.S3_REGION)
            s3_client.upload_file(local_file_path, settings.S3_BUCKET, s3_key)
            
            # Update track with S3 path and mark as downloaded
            s3_url = f"s3://{settings.S3_BUCKET}/{s3_key}"
            db.tracks.update_one({"_id": track["_id"]}, {
                "$set": {
                    "status": "downloaded",
                    "download_state": "completed",
                    "s3_path": s3_url,
                    "s3_bucket": settings.S3_BUCKET,
                    "s3_key": s3_key,
                    "downloaded_at": datetime.now(timezone.utc)
                }
            })
            
    except Exception as e:
        # Mark as failed
        db.tracks.update_one({"_id": track["_id"]}, {
            "$set": {
                "status": "failed",
                "download_state": "failed"
            },
            "$push": {"errors": str(e)}
        })
        raise


def handler(event, context):
    db = get_db()
    now = datetime.now(timezone.utc)
    next_24h = now + timedelta(hours=24)
    # track that have status 'new' [new|queued|sent|failed]
    # select tracks to process
    new_tracks = list(
        db.tracks.find({
        "status": {"$in": ["new", ]},
        "scheduled_at": {"$gte": now, "$lt": next_24h}
    })
    .sort([("scheduled_at", 1), ("_id", 1)])
    .limit(10)
    )

    for t in new_tracks:
        try:
            if not t.get("download_state"):
                download_track(db, t)
            send_and_mark(db, t)
        except Exception as e:
            db.tracks.update_one({"_id": t["_id"]}, {
                "$set": {"status": "failed"},
                "$push": {"errors": str(e)}
            })

    return {"ok": True, "processed": len(new_tracks)}



def send_and_mark(db, t):
    # Use S3 path if available, otherwise fall back to match file_path
    path = t.get("s3_path") or t.get("match", {}).get("file_path")
    
    if not path:
        raise RuntimeError("No file path available for track")
    
    # TODO: реалізуйте ваш транспорт (HTTP/бот) у sender/backend_sender.py
    # send_file(path, meta={"artist": t["artist"], "title": t["title"]})

    db.dispatches.insert_one({
        "track_id": t["_id"],
        "file_path": path, # or s3 key
        "created_at": datetime.now(timezone.utc)
    })
    db.tracks.update_one({"_id": t["_id"]}, {"$set": {"status": "queued"}})