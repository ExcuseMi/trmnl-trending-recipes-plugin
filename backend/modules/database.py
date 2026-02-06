"""
Database module for TRMNL Trending Recipes
Handles SQLite operations and schema management
"""

import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class Database:
    """SQLite database handler for recipe tracking"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None

    def get_connection(self) -> sqlite3.Connection:
        """Get or create database connection"""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            # Enable foreign keys and other pragmas
            self.conn.execute("PRAGMA foreign_keys = ON")
            self.conn.execute("PRAGMA journal_mode = WAL")
        return self.conn

    def initialize(self):
        """Initialize database schema"""
        logger.info(f"📊 Initializing database: {self.db_path}")

        conn = self.get_connection()
        cursor = conn.cursor()

        # Create recipes table (current state)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS recipes (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                installs INTEGER DEFAULT 0,
                forks INTEGER DEFAULT 0,
                popularity_score INTEGER DEFAULT 0,
                url TEXT,
                icon_url TEXT,
                thumbnail_url TEXT,
                created_at TEXT,
                updated_at TEXT NOT NULL,
                last_fetched TEXT NOT NULL
            )
        """)

        # Create recipe_history table (daily snapshots)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS recipe_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recipe_id TEXT NOT NULL,
                installs INTEGER NOT NULL,
                forks INTEGER NOT NULL,
                popularity_score INTEGER NOT NULL,
                snapshot_date TEXT NOT NULL,  -- UTC date string in ISO format
                snapshot_timestamp TEXT NOT NULL,  -- UTC datetime string in ISO format
                FOREIGN KEY (recipe_id) REFERENCES recipes(id),
                UNIQUE(recipe_id, snapshot_date)
            )
        """)

        # Create indices for performance
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_recipe_history_date 
            ON recipe_history(snapshot_date)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_recipe_history_recipe 
            ON recipe_history(recipe_id, snapshot_date)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_recipe_history_timestamp 
            ON recipe_history(snapshot_timestamp)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_recipes_popularity 
            ON recipes(popularity_score DESC)
        """)

        conn.commit()
        logger.info("✓ Database schema initialized")

    def upsert_recipe(self, recipe_data: Dict):
        """Insert or update a recipe"""
        conn = self.get_connection()
        cursor = conn.cursor()

        now = datetime.utcnow().isoformat()
        popularity_score = recipe_data.get('installs', 0) + recipe_data.get('forks', 0)

        cursor.execute("""
            INSERT INTO recipes (
                id, name, description, installs, forks, 
                popularity_score, url, icon_url, thumbnail_url, created_at, 
                updated_at, last_fetched
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                description = excluded.description,
                installs = excluded.installs,
                forks = excluded.forks,
                popularity_score = excluded.popularity_score,
                url = excluded.url,
                icon_url = excluded.icon_url,
                thumbnail_url = excluded.thumbnail_url,
                updated_at = excluded.updated_at,
                last_fetched = excluded.last_fetched
        """, (
            recipe_data['id'],
            recipe_data.get('name'),
            recipe_data.get('description'),
            recipe_data.get('installs', 0),
            recipe_data.get('forks', 0),
            popularity_score,
            recipe_data.get('url'),
            recipe_data.get('icon_url'),
            recipe_data.get('thumbnail_url'),
            recipe_data.get('created_at'),
            recipe_data.get('updated_at', now),
            now
        ))

        conn.commit()

    def save_snapshot(self, recipe_id: str, installs: int, forks: int):
        """Save a daily snapshot of recipe stats using UTC"""
        conn = self.get_connection()
        cursor = conn.cursor()

        # Use UTC for all timestamps
        now_utc = datetime.utcnow()
        snapshot_date = now_utc.date().isoformat()  # UTC date
        snapshot_timestamp = now_utc.isoformat()  # UTC datetime
        popularity_score = installs + forks

        cursor.execute("""
            INSERT INTO recipe_history (
                recipe_id, installs, forks, popularity_score, snapshot_date, snapshot_timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(recipe_id, snapshot_date) DO UPDATE SET
                installs = excluded.installs,
                forks = excluded.forks,
                popularity_score = excluded.popularity_score,
                snapshot_timestamp = excluded.snapshot_timestamp
        """, (recipe_id, installs, forks, popularity_score, snapshot_date, snapshot_timestamp))

        conn.commit()

    def get_recipe_current(self, recipe_id: str) -> Optional[Dict]:
        """Get current state of a recipe"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM recipes WHERE id = ?
        """, (recipe_id,))

        row = cursor.fetchone()
        return dict(row) if row else None

    def get_all_recipes_current(self) -> List[Dict]:
        """Get current state of all recipes"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM recipes ORDER BY popularity_score DESC
        """)

        return [dict(row) for row in cursor.fetchall()]

    def get_recipe_history(self, recipe_id: str, days: int = 30) -> List[Dict]:
        """Get historical snapshots for a recipe"""
        conn = self.get_connection()
        cursor = conn.cursor()

        # Calculate cutoff in UTC
        cutoff = datetime.utcnow() - timedelta(days=days)
        cutoff_date = cutoff.date().isoformat()

        cursor.execute("""
            SELECT * FROM recipe_history 
            WHERE recipe_id = ?
            AND snapshot_date >= ?
            ORDER BY snapshot_date DESC
        """, (recipe_id, cutoff_date))

        return [dict(row) for row in cursor.fetchall()]

    def get_recipe_delta(self, recipe_id: str, days_ago: int) -> Optional[Dict]:
        """Get stats from N days ago for delta calculation"""
        return self.get_recipe_delta_with_offset(recipe_id, days_ago, 0)

    def get_recipe_delta_with_offset(self, recipe_id: str, days_ago: int, utc_offset_seconds: int = 0) -> Optional[Dict]:
        """
        Get stats from N days ago with UTC offset consideration

        Args:
            recipe_id: Recipe identifier
            days_ago: Number of days to look back
            utc_offset_seconds: UTC offset in seconds for day boundary adjustment

        Returns:
            Dict with historical stats or None if not found
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        # Calculate the target date with UTC offset adjustment
        # We want the snapshot from the local midnight N days ago
        now_utc = datetime.utcnow()

        # Adjust to local time
        local_now = now_utc + timedelta(seconds=utc_offset_seconds)
        # Get local midnight N days ago
        target_local_midnight = (local_now - timedelta(days=days_ago)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        # Convert back to UTC
        target_utc_midnight = target_local_midnight - timedelta(seconds=utc_offset_seconds)
        target_date = target_utc_midnight.date().isoformat()

        logger.debug(f"Looking for snapshot for recipe {recipe_id} on date {target_date} (UTC offset: {utc_offset_seconds}s)")

        cursor.execute("""
            SELECT installs, forks, popularity_score, snapshot_date, snapshot_timestamp
            FROM recipe_history
            WHERE recipe_id = ?
            AND snapshot_date = ?
            ORDER BY snapshot_timestamp DESC
            LIMIT 1
        """, (recipe_id, target_date))

        row = cursor.fetchone()

        # If we don't have exact date match, find the closest snapshot before target date
        if not row:
            cursor.execute("""
                SELECT installs, forks, popularity_score, snapshot_date, snapshot_timestamp
                FROM recipe_history
                WHERE recipe_id = ?
                AND snapshot_date < ?
                ORDER BY snapshot_date DESC
                LIMIT 1
            """, (recipe_id, target_date))

            row = cursor.fetchone()

        return dict(row) if row else None

    def get_all_recipes_with_delta(self, days_ago: int, utc_offset_seconds: int = 0) -> List[Dict]:
        """
        Get all recipes with their stats from N days ago with UTC offset

        Args:
            days_ago: Number of days to look back
            utc_offset_seconds: UTC offset in seconds for day boundary adjustment

        Returns:
            List of recipes with current and past stats
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        # Calculate the target date with UTC offset adjustment
        now_utc = datetime.utcnow()

        # Adjust to local time
        local_now = now_utc + timedelta(seconds=utc_offset_seconds)
        # Get local midnight N days ago
        target_local_midnight = (local_now - timedelta(days=days_ago)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        # Convert back to UTC
        target_utc_midnight = target_local_midnight - timedelta(seconds=utc_offset_seconds)
        target_date = target_utc_midnight.date().isoformat()

        cursor.execute("""
            SELECT 
                r.id,
                r.name,
                r.description,
                r.installs as current_installs,
                r.forks as current_forks,
                r.popularity_score as current_popularity,
                r.url,
                r.icon_url,
                r.thumbnail_url,
                h.installs as past_installs,
                h.forks as past_forks,
                h.popularity_score as past_popularity,
                h.snapshot_date as past_snapshot_date,
                h.snapshot_timestamp as past_snapshot_timestamp
            FROM recipes r
            LEFT JOIN (
                -- Get the most recent snapshot on or before target date for each recipe
                SELECT DISTINCT
                    recipe_id,
                    installs,
                    forks,
                    popularity_score,
                    snapshot_date,
                    snapshot_timestamp,
                    ROW_NUMBER() OVER (
                        PARTITION BY recipe_id 
                        ORDER BY snapshot_date DESC
                    ) as rn
                FROM recipe_history
                WHERE snapshot_date <= ?
            ) h ON r.id = h.recipe_id AND h.rn = 1
        """, (target_date,))

        return [dict(row) for row in cursor.fetchall()]

    def get_recipe_count(self) -> int:
        """Get total number of recipes"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) as count FROM recipes")
        return cursor.fetchone()['count']

    def get_statistics(self) -> Dict:
        """Get overall database statistics"""
        conn = self.get_connection()
        cursor = conn.cursor()

        # Total recipes
        cursor.execute("SELECT COUNT(*) as count FROM recipes")
        total_recipes = cursor.fetchone()['count']

        # Total snapshots
        cursor.execute("SELECT COUNT(*) as count FROM recipe_history")
        total_snapshots = cursor.fetchone()['count']

        # Latest snapshot date
        cursor.execute("SELECT MAX(snapshot_date) as latest_date, MAX(snapshot_timestamp) as latest_timestamp FROM recipe_history")
        latest = cursor.fetchone()

        # Total installs and forks
        cursor.execute("SELECT SUM(installs) as installs, SUM(forks) as forks FROM recipes")
        totals = cursor.fetchone()

        # Database file info
        import os
        db_size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0

        return {
            'total_recipes': total_recipes,
            'total_snapshots': total_snapshots,
            'latest_snapshot_date': latest['latest_date'],
            'latest_snapshot_timestamp': latest['latest_timestamp'],
            'total_installs': totals['installs'] or 0,
            'total_forks': totals['forks'] or 0,
            'database_size_bytes': db_size
        }

    def get_recent_snapshots(self, hours: int = 24) -> List[Dict]:
        """Get snapshots from the last N hours"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cutoff = datetime.utcnow() - timedelta(hours=hours)
        cutoff_iso = cutoff.isoformat()

        cursor.execute("""
            SELECT * FROM recipe_history
            WHERE snapshot_timestamp >= ?
            ORDER BY snapshot_timestamp DESC
        """, (cutoff_iso,))

        return [dict(row) for row in cursor.fetchall()]

    def cleanup_old_snapshots(self, days_to_keep: int = 180):
        """Remove old snapshots beyond retention period"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cutoff = datetime.utcnow() - timedelta(days=days_to_keep)
        cutoff_date = cutoff.date().isoformat()

        cursor.execute("""
            DELETE FROM recipe_history
            WHERE snapshot_date < ?
        """, (cutoff_date,))

        deleted = cursor.rowcount
        conn.commit()

        if deleted > 0:
            logger.info(f"🗑️  Cleaned up {deleted} old snapshots (older than {cutoff_date})")

        return deleted

    def vacuum(self):
        """Optimize database"""
        conn = self.get_connection()
        conn.execute("VACUUM")
        logger.info("✓ Database vacuum completed")

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None