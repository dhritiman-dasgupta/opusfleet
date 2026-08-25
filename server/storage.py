"""Object storage for finished segments.

Talks to MinIO locally and on the VPS; the same code reaches real S3 by pointing
S3_ENDPOINT at s3.<region>.amazonaws.com with S3_SECURE=1, so the migration path
off MinIO stays open.

Key layout is deliberately identical to the original server's, so the CMS's
existing /api/recordings and /api/dates parsing keeps working:

    <imei>/<YYYY-MM-DD>/<HHMMSS>_<duration>s.<ext>
"""

import io
import threading
import time
from datetime import timedelta

from minio import Minio
from minio.error import S3Error

from . import config


class SegmentStore:
    def __init__(self):
        self._client = Minio(
            config.S3_ENDPOINT,
            access_key=config.S3_ACCESS_KEY,
            secret_key=config.S3_SECRET_KEY,
            secure=config.S3_SECURE,
            region=config.S3_REGION,
        )
        self._bucket = config.S3_BUCKET
        self._lock = threading.Lock()
        self._ensured = False

    def ensure_bucket(self):
        with self._lock:
            if self._ensured:
                return
            if not self._client.bucket_exists(self._bucket):
                self._client.make_bucket(self._bucket)
            self._ensured = True

    @staticmethod
    def key_for(imei, started_at, duration_s, ext):
        gm = time.gmtime(started_at)
        return "%s/%s/%s_%ds.%s" % (
            imei,
            time.strftime("%Y-%m-%d", gm),
            time.strftime("%H%M%S", gm),
            int(duration_s),
            ext,
        )

    def put_segment(self, imei, data, started_at, duration_s, ext, content_type):
        self.ensure_bucket()
        key = self.key_for(imei, started_at, duration_s, ext)
        self._client.put_object(
            self._bucket, key, io.BytesIO(data), length=len(data), content_type=content_type
        )
        return key

    def list_dates(self, imei):
        self.ensure_bucket()
        prefix = f"{imei}/"
        out = set()
        for obj in self._client.list_objects(self._bucket, prefix=prefix, recursive=False):
            part = obj.object_name[len(prefix):].strip("/")
            if part:
                out.add(part)
        return sorted(out)

    def list_segments(self, imei, date=None, limit=300):
        self.ensure_bucket()
        prefix = f"{imei}/" + (f"{date}/" if date else "")
        objs = list(self._client.list_objects(self._bucket, prefix=prefix, recursive=True))
        objs.sort(key=lambda o: o.object_name)
        objs = objs[-limit:]
        items = []
        for o in reversed(objs):
            name = o.object_name.rsplit("/", 1)[-1]
            duration = name.rsplit("_", 1)[-1].rsplit(".", 1)[0] if "_" in name else ""
            items.append({
                "key": o.object_name,
                "size": o.size,
                "duration": duration,
                "url": self.presign(o.object_name),
            })
        return items

    def presign(self, key, expiry=None):
        return self._client.presigned_get_object(
            self._bucket, key, expires=timedelta(seconds=expiry or config.PRESIGN_EXPIRY)
        )

    def health(self):
        try:
            self.ensure_bucket()
            return True, ""
        except (S3Error, OSError) as exc:
            return False, str(exc)
