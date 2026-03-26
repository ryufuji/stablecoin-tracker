"""F3: Data Storage -- SQLite backend for stablecoin tracker."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS articles (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    url          TEXT UNIQUE NOT NULL,
    title        TEXT NOT NULL,
    source       TEXT NOT NULL,
    published_at TEXT,
    summary_ja   TEXT,
    category     TEXT,
    projects     TEXT,        -- JSON array as string
    importance   INTEGER,
    raw_text     TEXT,
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


class Storage:
    """Thin wrapper around an SQLite database for article persistence."""

    def __init__(self, db_path: str = "data/articles.db") -> None:
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.init_db()

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    def init_db(self) -> None:
        """Create the articles table if it doesn't already exist."""
        try:
            with self._connect() as conn:
                conn.executescript(_SCHEMA)
        except sqlite3.Error:
            logger.exception("Failed to initialise database at %s", self.db_path)
            raise

    # ------------------------------------------------------------------
    # Duplicate check
    # ------------------------------------------------------------------

    def is_duplicate(self, url: str) -> bool:
        """Return True when *url* is already stored."""
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT 1 FROM articles WHERE url = ?", (url,)
                ).fetchone()
                return row is not None
        except sqlite3.Error:
            logger.exception("Duplicate check failed for %s", url)
            return False

    # ------------------------------------------------------------------
    # Insert / Update
    # ------------------------------------------------------------------

    def save_article(self, article: dict[str, Any]) -> None:
        """Insert a single article.  Silently skips duplicates."""
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO articles
                        (url, title, source, published_at, raw_text)
                    VALUES (:url, :title, :source, :published_at, :raw_text)
                    """,
                    article,
                )
        except sqlite3.Error:
            logger.exception("Failed to save article: %s", article.get("url"))

    def update_ai_fields(
        self,
        url: str,
        summary_ja: str,
        category: str,
        projects: list[str],
        importance: int,
    ) -> None:
        """Patch the AI-generated columns for the article identified by *url*."""
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE articles
                       SET summary_ja  = ?,
                           category    = ?,
                           projects    = ?,
                           importance  = ?
                     WHERE url = ?
                    """,
                    (summary_ja, category, json.dumps(projects, ensure_ascii=False), importance, url),
                )
        except sqlite3.Error:
            logger.exception("Failed to update AI fields for %s", url)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_articles(
        self,
        days: int = 1,
        category: str | None = None,
        project: str | None = None,
        keyword: str | None = None,
        min_importance: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return articles matching the given filters, newest first."""
        since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        clauses: list[str] = ["created_at >= ?"]
        params: list[Any] = [since]

        if category is not None:
            clauses.append("category = ?")
            params.append(category)
        if project is not None:
            clauses.append("projects LIKE ?")
            params.append(f"%{project}%")
        if keyword is not None:
            clauses.append("(title LIKE ? OR raw_text LIKE ?)")
            params.extend([f"%{keyword}%", f"%{keyword}%"])
        if min_importance is not None:
            clauses.append("importance >= ?")
            params.append(min_importance)

        where = " AND ".join(clauses)
        sql = f"SELECT * FROM articles WHERE {where} ORDER BY created_at DESC"

        try:
            with self._connect() as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(sql, params).fetchall()
                return [dict(row) for row in rows]
        except sqlite3.Error:
            logger.exception("get_articles query failed")
            return []

    def get_article_count_by_source(self, days: int = 1) -> dict[str, int]:
        """Return ``{source: count}`` for articles within *days*."""
        since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT source, COUNT(*) AS cnt
                      FROM articles
                     WHERE created_at >= ?
                     GROUP BY source
                    """,
                    (since,),
                ).fetchall()
                return {row[0]: row[1] for row in rows}
        except sqlite3.Error:
            logger.exception("get_article_count_by_source query failed")
            return {}

    def get_category_counts(self, days: int = 1) -> dict[str, int]:
        """Return ``{category: count}`` for articles within *days*."""
        since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT category, COUNT(*) AS cnt
                      FROM articles
                     WHERE created_at >= ?
                       AND category IS NOT NULL
                     GROUP BY category
                    """,
                    (since,),
                ).fetchall()
                return {row[0]: row[1] for row in rows}
        except sqlite3.Error:
            logger.exception("get_category_counts query failed")
            return {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)
