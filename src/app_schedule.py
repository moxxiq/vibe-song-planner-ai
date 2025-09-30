from datetime import datetime, timezone, timedelta

from db import get_db
from config import settings
from utils.telegram_sender import schedule_one_message
from utils.aws import get_s3
from io import BytesIO
from os.path import basename

# read https://www.reddit.com/r/aws/comments/e4cvta/repopipeline_design_best_practise_for_lambda_cicd/
# todo: setup python typing checker

async def handler(event, context):
    db = get_db()
    left_bound = datetime.now(timezone.utc) # now
    right_bound = left_bound + timedelta(hours=24)
    # track that have status 'new' [new|queued|sent|failed]

    # select tracks to process
    tracks = list(
        db.tracks.find({
        "status": {"$in": ["new", ]},
        "scheduled_at": {"$gte": left_bound, "$lt": right_bound},
        "download_state": True,
        })
        .sort([("scheduled_at", 1), ("_id", 1)])
        .limit(10)
    )

    for t in tracks:
        try:
            await send_and_mark(db, t)
        except Exception as e:
            db.tracks.update_one({"_id": t["_id"]}, {
                "$set": {"status": "failed"},
                "$push": {"errors": str(e)}
            })

    return {"ok": True, "processed": len(tracks)}

async def s3_mp3_to_bytesio(bucket: str, key: str) -> BytesIO:
    """
    Download an S3 object asynchronously into memory (BytesIO),
    no filesystem writes.
    """
    async with await get_s3() as s3:
        resp = await s3.get_object(Bucket=bucket, Key=key)
        # resp["Body"] is an async stream
        body = resp["Body"]
        buf = BytesIO()
        # Read in chunks to keep peak memory reasonable for large files
        chunk = await body.read(1024 * 1024)
        while chunk:
            buf.write(chunk)
            chunk = await body.read(1024 * 1024)
        await body.close()
        # Telethon expects a seekable file-like object
        buf.seek(0)
        # Give it a name so Telethon can infer filename/extension
        buf.name = basename(key) or "audio.mp3"
        return buf

async def send_and_mark(db, t):
    path = t.get("file_path")

    if not path:
        raise RuntimeError("No file path available for track")
    
    # prefetch audio file from s3 storage
    audio_file = await s3_mp3_to_bytesio(settings.S3_BUCKET, path)
    await schedule_one_message(t, audio_file)

    db.tracks.update_one({"_id": t["_id"]}, {"$set": {"status": "queued"}})