import os
import json
from typing import Any, Dict, List

import pymysql

DB_HOST = os.environ["DB_HOST"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_NAME = os.environ["DB_NAME"]

def get_conn():
    """
    Create and return a new MySQL connection.
    """
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
    )

def top_artists_by_metric(
    conn,
    metric: str = "popularity",
    group_by: str = "artists",
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """
    Return top artists (or other group) by a given metric.
    """
    sql = f"""
        SELECT {group_by} AS group_key,
               COUNT(*) AS track_count,
               AVG({metric}) AS avg_{metric}
        FROM tracks
        GROUP BY {group_by}
        ORDER BY avg_{metric} DESC
        LIMIT %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        return cur.fetchall()

def avg_metrics_by_category(
    conn,
    category_col: str = "popularity_tier",
) -> List[Dict[str, Any]]:
    """
    Return average metrics (duration, danceability, energy)
    aggregated by a category column.
    """
    sql = f"""
        SELECT {category_col} AS category,
               COUNT(*) AS track_count,
               AVG(duration_minutes) AS avg_duration_minutes,
               AVG(danceability) AS avg_danceability,
               AVG(energy) AS avg_energy
        FROM tracks
        GROUP BY {category_col}
        ORDER BY track_count DESC;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()

def songs_filtered(
    conn,
    min_popularity: float = 0,
    min_danceability: float = 0,
    min_energy: float = 0,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    Return songs filtered by minimum thresholds on
    popularity, danceability, and energy.
    """
    where_clauses = []
    params: List[Any] = []

    if min_popularity is not None:
        where_clauses.append("popularity >= %s")
        params.append(min_popularity)
    if min_danceability is not None:
        where_clauses.append("danceability >= %s")
        params.append(min_danceability)
    if min_energy is not None:
        where_clauses.append("energy >= %s")
        params.append(min_energy)

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    sql = f"""
        SELECT track_name,
               artists,
               popularity,
               danceability,
               energy,
               explicit_label
        FROM tracks
        {where_sql}
        ORDER BY popularity DESC
        LIMIT %s;
    """
    params.append(limit)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()

def explicit_vs_clean_stats(conn) -> List[Dict[str, Any]]:
    """
    Compare stats between explicit and clean tracks.
    """
    sql = """
        SELECT explicit_label,
               COUNT(*) AS track_count,
               AVG(popularity) AS avg_popularity,
               MAX(popularity) AS max_popularity
        FROM tracks
        GROUP BY explicit_label;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()

def lambda_handler(event, context):
    """
    This code was assisted by ChatGPT.
    Main Lambda handler for SAAF + CLI usage.

    - Accepts either:
        * direct dict payload: {"action": "...", ...}
        * API Gateway-style: {"body": "{\"action\": \"...\", ...}"}

    - Returns a dict with:
        * "version": int (required by SAAF)
        * "action": the action that was executed
        * "query_result": list of rows or error info
    """

    # Assisted by ChatGPT - Normalize event into a spec dict
    if "body" in event and isinstance(event["body"], str):
        try:
            spec = json.loads(event["body"])
        except json.JSONDecodeError:
            spec = {}
    else:
        spec = event if isinstance(event, dict) else {}

    action = spec.get("action")

    conn = get_conn()
    try:
        if action == "top_artists":
            result = top_artists_by_metric(
                conn,
                metric=spec.get("metric", "popularity"),
                group_by=spec.get("group_by", "artists"),
                limit=int(spec.get("limit", 10)),
            )

        elif action == "avg_metrics_by_category":
            result = avg_metrics_by_category(
                conn,
                category_col=spec.get("category_col", "popularity_tier"),
            )

        elif action == "songs_filtered":
            result = songs_filtered(
                conn,
                min_popularity=spec.get("min_popularity", 0),
                min_danceability=spec.get("min_danceability", 0),
                min_energy=spec.get("min_energy", 0),
                limit=int(spec.get("limit", 50)),
            )

        elif action == "explicit_vs_clean_stats":
            result = explicit_vs_clean_stats(conn)

        else:
            result = {
                "error": f"Unknown or missing action: {action}",
                "received_spec": spec,
            }

    finally:
        conn.close()

    return {
        "version": 1,
        "action": action,
        "query_result": result,
    }
