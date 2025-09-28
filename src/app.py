from datetime import datetime, timezone, timedelta
from db import get_db
from config import settings
from utils.telegram_sender import schedule_one_message

# read https://www.reddit.com/r/aws/comments/e4cvta/repopipeline_design_best_practise_for_lambda_cicd/

def handler(event, context):
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
            send_and_mark(db, t)
        except Exception as e:
            db.tracks.update_one({"_id": t["_id"]}, {
                "$set": {"status": "failed"},
                "$push": {"errors": str(e)}
            })

    return {"ok": True, "processed": len(tracks)}



def send_and_mark(db, t):
    path = t.get("file_path")

    if not path:
        raise RuntimeError("No file path available for track")
    
    # TODO: fetch track from S3 and send via telegram_sender
    schedule_one_message(t)

    db.tracks.update_one({"_id": t["_id"]}, {"$set": {"status": "queued"}})