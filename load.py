import os
import csv
import io

import boto3
import pymysql

TRANSFORMED_BUCKET = os.environ.get("TRANSFORMED_BUCKET")

DB_HOST = os.environ["DB_HOST"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_NAME = os.environ["DB_NAME"]

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS tracks (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    track_name          VARCHAR(512) NOT NULL,
    artists             VARCHAR(512) NOT NULL,
    album_name          VARCHAR(512),
    track_genre         VARCHAR(128),
    duration_minutes    DOUBLE,
    popularity          DOUBLE,
    popularity_tier     VARCHAR(32),
    danceability        DOUBLE,
    danceability_label  VARCHAR(32),
    energy              DOUBLE,
    energy_label        VARCHAR(32),
    explicit_label      VARCHAR(16)
);
"""

INSERT_SQL = """
INSERT INTO tracks (
    track_name,
    artists,
    album_name,
    track_genre,
    duration_minutes,
    popularity,
    popularity_tier,
    danceability,
    danceability_label,
    energy,
    energy_label,
    explicit_label
) VALUES (
    %(track_name)s,
    %(artists)s,
    %(album_name)s,
    %(track_genre)s,
    %(duration_minutes)s,
    %(popularity)s,
    %(popularity_tier)s,
    %(danceability)s,
    %(danceability_label)s,
    %(energy)s,
    %(energy_label)s,
    %(explicit_label)s
);
"""

def _to_float(val):
    """
    Safely convert a CSV field to float or None.
    """
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None

def lambda_handler(event, context):
    """
    Load step (full file, batched):
    - Read transformed CSV from S3
    - Create 'tracks' table if needed
    - Insert ALL rows into RDS in BATCH_SIZE chunks
    """
    print("EVENT:", event)

    bucket = event.get("bucket") or TRANSFORMED_BUCKET
    key = event.get("key")

    if not bucket:
        raise ValueError("TRANSFORMED_BUCKET env var must be set or 'bucket' provided in event")
    if not key:
        raise ValueError("Event must contain 'key' for the transformed CSV object")

    print(f"Reading from s3://{bucket}/{key}")

    s3 = boto3.client("s3")

    obj = s3.get_object(Bucket=bucket, Key=key)
    csv_bytes = obj["Body"].read()
    print(f"Downloaded {len(csv_bytes)} bytes from S3")

    text_stream = io.StringIO(csv_bytes.decode("utf-8"))
    reader = csv.DictReader(text_stream)

    print(f"Connecting to DB {DB_HOST} / {DB_NAME} as {DB_USER}")
    conn = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        autocommit=False,
    )
    print("Connected to DB")

    inserted = 0
    batch = []

    try:
        with conn.cursor() as cur:
            print("Ensuring tracks table exists...")
            cur.execute(CREATE_TABLE_SQL)

            print(f"Starting inserts with BATCH_SIZE={BATCH_SIZE}...")
            for row in reader:
                data = {
                    "track_name": row.get("track_name_clean", ""),
                    "artists": row.get("artists_clean", ""),
                    "album_name": row.get("Album"),
                    "track_genre": row.get("Genre"),
                    "duration_minutes": _to_float(row.get("duration_minutes")),
                    "popularity": _to_float(row.get("Popularity")),
                    "popularity_tier": row.get("popularity_tier"),
                    "danceability": _to_float(row.get("Danceability")),
                    "danceability_label": row.get("danceability_label"),
                    "energy": _to_float(row.get("Energy")),
                    "energy_label": row.get("energy_label"),
                    "explicit_label": row.get("explicit_label"),
                }

                batch.append(data)

                if len(batch) >= BATCH_SIZE:
                    cur.executemany(INSERT_SQL, batch)
                    inserted += len(batch)
                    print(f"Inserted batch of {len(batch)} rows (total so far: {inserted})")
                    batch.clear()

            if batch:
                cur.executemany(INSERT_SQL, batch)
                inserted += len(batch)
                print(f"Inserted final batch of {len(batch)} rows (grand total: {inserted})")
                batch.clear()

        conn.commit()
        print(f"Committed {inserted} rows.")
    finally:
        conn.close()
        print("Closed DB connection")

    return {
        "status": "ok",
        "inserted": inserted,
        "bucket": bucket,
        "key": key,
    }
