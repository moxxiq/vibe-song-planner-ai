import boto3
import tempfile
import os
from datetime import datetime, timezone

from config import settings

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
