import os, boto3
from datetime import datetime, timezone
from ..db import get_db

_inited = False

def ensure_prefetched(bucket: str, prefix: str, local_root: str = "/tmp/audio"):
    global _inited
    if _inited:
        return
    s3 = boto3.client("s3")
    os.makedirs(local_root, exist_ok=True)

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.lower().endswith(".mp3"):
                continue
            local_path = os.path.join(local_root, os.path.basename(key))
            if not os.path.exists(local_path):
                s3.download_file(bucket, key, local_path)

            # update files catalog
            db = get_db()
            db.files.update_one(
                {"path": local_path},
                {"$set": {
                    "path": local_path,
                    "normalized_artist": "",  # can be filled by audio_index later
                    "normalized_title": "",
                    "downloaded_at": datetime.now(timezone.utc),
                }},
                upsert=True,
            )

            # optionally, if you have a way to link track↔file, set download_state on tracks

    _inited = True
