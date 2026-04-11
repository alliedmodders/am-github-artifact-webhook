from contextlib import contextmanager

import pymysql
import pymysql.cursors


@contextmanager
def get_connection(config):
    """Context manager yielding a committed-on-exit PyMySQL connection."""
    conn = pymysql.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        database=config.name,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_known_builds(conn) -> dict[str, dict[int, dict]]:
    """
    Return {branch: {build_num: {windows_url, linux_url}}} for all rows in sm_commit_log.
    URLs may be None if not yet populated.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT branch, build, windows_url, linux_url FROM sm_commit_log")
        known: dict[str, dict[int, dict]] = {}
        for row in cur.fetchall():
            known.setdefault(row["branch"], {})[row["build"]] = {
                "windows_url": row["windows_url"],
                "linux_url": row["linux_url"],
            }
    return known


def update_build_urls(conn, *, branch, build_num, windows_url, linux_url):
    """Fill in NULL URL columns for an existing build. Non-NULL values are never overwritten."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sm_commit_log
            SET windows_url = COALESCE(%s, windows_url),
                linux_url   = COALESCE(%s, linux_url)
            WHERE branch = %s AND build = %s
            """,
            (windows_url, linux_url, branch, build_num),
        )


def upsert_build(
    conn, *, branch, sha, build_num, timestamp, message, windows_url, linux_url
):
    """
    Insert a build record, or on duplicate key fill in any NULL URL columns.
    Existing non-NULL URLs are never overwritten.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sm_commit_log
                (branch, hash, build, date, message, windows_url, linux_url)
            VALUES (%s, %s, %s, FROM_UNIXTIME(%s), %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                windows_url = COALESCE(VALUES(windows_url), windows_url),
                linux_url   = COALESCE(VALUES(linux_url),   linux_url)
            """,
            (branch, sha, build_num, timestamp, message, windows_url, linux_url),
        )
