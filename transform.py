import os
import io
import csv
import codecs
import tempfile

import boto3

s3 = boto3.client("s3")

RAW_BUCKET = os.environ.get("RAW_BUCKET")
TRANSFORMED_BUCKET = os.environ.get("TRANSFORMED_BUCKET", RAW_BUCKET)

OUTPUT_FIELDS = [
    "track_name_clean",
    "artists_clean",
    "Album",
    "Genre",
    "duration_minutes",
    "Popularity",
    "popularity_tier",
    "Danceability",
    "danceability_label",
    "Energy",
    "energy_label",
    "explicit_label",
]

def _safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def _parse_duration(val):
    """
    Assisted by ChatGPT.
    Handles:
    - "3:15"
    - "00:03:30"
    - "4.5" (minutes)
    - milliseconds as big numbers
    Returns duration in minutes (float) or None.
    """
    if val is None:
        return None

    s = str(val).strip()
    if not s:
        return None

    if ":" in s:
        parts = [p.strip() for p in s.split(":")]
        try:
            if len(parts) == 3:
                h, m, sec = parts
                return float(h) * 60.0 + float(m) + float(sec) / 60.0
            elif len(parts) == 2:
                m, sec = parts
                return float(m) + float(sec) / 60.0
        except Exception:
            pass

    try:
        return float(s)
    except Exception:
        pass

    try:
        ms = float(s)
        if ms > 1000:
            return ms / 1000.0 / 60.0
    except Exception:
        pass

    return None

def _popularity_tier(p):
    if p is None:
        return "Unknown"
    if p >= 70:
        return "High"
    if p >= 40:
        return "Medium"
    return "Low"

def _danceability_label(d):
    if d is None:
        return "Unknown"
    if d >= 75:
        return "Very Danceable"
    if d >= 50:
        return "Danceable"
    return "Low"

def _energy_label(e):
    if e is None:
        return "Unknown"
    if e >= 70:
        return "High"
    if e >= 40:
        return "Medium"
    return "Low"

def _explicit_label(val):
    if val is None:
        return "Clean"

    if isinstance(val, (int, float)):
        return "Explicit" if val != 0 else "Clean"

    s = str(val).strip().lower()
    if s in ("true", "t", "yes", "y", "1", "explicit"):
        return "Explicit"
    if s in ("false", "f", "no", "n", "0", "clean"):
        return "Clean"
    return "Clean"

def _pick(row, *names, default=""):
    """
    Return the first non-empty value for the given column names from row.
    Also handles BOM prefix in header names.
    """
    for name in names:
        if name in row and row[name] not in (None, ""):
            return row[name]

        bom_name = "\ufeff" + str(name)
        if bom_name in row and row[bom_name] not in (None, ""):
            return row[bom_name]

    return default

def _transform_row(row):
    """
    Transform a single original CSV row (dict) into the normalized schema
    that your Load Lambda expects.
    """

    track_name = _pick(row, "song", "track_name", "name")
    artist = _pick(row, "Artist(s)", "artists", "artist_name", "artist")
    album = _pick(row, "Album", "album", "album_name")
    genre = _pick(row, "Genre", "genre", "track_genre")

    length_val = _pick(row, "Length", "duration", "duration_ms")
    duration_minutes = _parse_duration(length_val)

    popularity_raw = _pick(row, "Popularity", "popularity")
    popularity = _safe_float(popularity_raw)
    pop_tier = _popularity_tier(popularity)

    dance_raw = _pick(row, "Danceability", "danceability")
    dance = _safe_float(dance_raw)
    dance_label = _danceability_label(dance)

    energy_raw = _pick(row, "Energy", "energy")
    energy = _safe_float(energy_raw)
    energy_lbl = _energy_label(energy)

    explicit_raw = _pick(row, "Explicit", "explicit")
    explicit_lbl = _explicit_label(explicit_raw)

    return {
        "track_name_clean": str(track_name).strip(),
        "artists_clean": str(artist).strip(),
        "Album": str(album).strip(),
        "Genre": str(genre).strip().lower(),

        "duration_minutes": duration_minutes,
        "Popularity": popularity,
        "popularity_tier": pop_tier,

        "Danceability": dance,
        "danceability_label": dance_label,

        "Energy": energy,
        "energy_label": energy_lbl,

        "explicit_label": explicit_lbl,
    }

def lambda_handler(event, context):
    """
    Assisted by ChatGPT.
    Transform step (streaming, no big in-memory buffers):
    - Stream RAW CSV from S3
    - Transform each row into normalized schema
    - Write *_transformed.csv to /tmp, then upload to TRANSFORMED_BUCKET
    """
    if "Records" in event:
        rec = event["Records"][0]
        bucket = rec["s3"]["bucket"]["name"]
        key = rec["s3"]["object"]["key"]
    else:
        bucket = event.get("bucket", RAW_BUCKET)
        key = event["key"]

    if not bucket or not key:
        raise ValueError("Bucket and key must be provided")

    print(f"Reading RAW from s3://{bucket}/{key}")

    obj = s3.get_object(Bucket=bucket, Key=key)
    body_stream = obj["Body"]

    text_stream = codecs.getreader("utf-8")(body_stream)
    reader = csv.DictReader(text_stream)

    base_name = os.path.basename(key).replace(".csv", "_transformed.csv")
    tmp_path = os.path.join("/tmp", base_name)

    print(f"Writing transformed data to temporary file: {tmp_path}")

    row_count = 0

    with open(tmp_path, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()

        for row in reader:
            out_row = _transform_row(row)
            writer.writerow(out_row)
            row_count += 1

            if row_count % 50000 == 0:
                print(f"Transformed {row_count} rows...")

    print(f"Total transformed rows: {row_count}")

    out_key = key.replace(".csv", "_transformed.csv")
    print(f"Uploading transformed file to s3://{TRANSFORMED_BUCKET}/{out_key}")

    s3.upload_file(tmp_path, TRANSFORMED_BUCKET, out_key)

    print("Upload complete.")

    try:
        os.remove(tmp_path)
        print(f"Removed temporary file {tmp_path}")
    except OSError:
        pass

    return {
        "status": "ok",
        "input_bucket": bucket,
        "input_key": key,
        "output_bucket": TRANSFORMED_BUCKET,
        "output_key": out_key,
        "rows": row_count,
    }
