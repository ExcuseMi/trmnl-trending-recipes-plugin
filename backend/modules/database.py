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
        self.schema_version = 3  # Current schema version

    def get_connection(self) -> sqlite3.Connection:
        """Get or create database connection"""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            # Enable foreign keys and other pragmas
            self.conn.execute("PRAGMA foreign_keys = ON")
            self.conn.execute("PRAGMA journal_mode = WAL")
        return self.conn

    def _get_schema_version(self) -> int:
        """Get current schema version from database"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # Try to read schema version from a special table
            cursor.execute("SELECT version FROM schema_version ORDER BY id DESC LIMIT 1")
            row = cursor.fetchone()
            return row['version'] if row else 0
        except sqlite3.OperationalError:
            # Table doesn't exist, assume version 0
            return 0

    def _set_schema_version(self, version: int):
        """Store schema version in database"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                version INTEGER NOT NULL,
                upgraded_at TEXT NOT NULL
            )
        """)

        now = datetime.utcnow().isoformat()
        cursor.execute("INSERT INTO schema_version (version, upgraded_at) VALUES (?, ?)",
                      (version, now))
        conn.commit()

    def _migrate_from_v1_to_v2(self):
        """Migrate database from schema version 1 to 2"""
        logger.info("🔧 Migrating database schema from v1 to v2...")
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # Add snapshot_timestamp column if it doesn't exist
            cursor.execute("PRAGMA table_info(recipe_history)")
            columns = [col[1] for col in cursor.fetchall()]

            if 'snapshot_timestamp' not in columns:
                logger.info("  ✓ Adding snapshot_timestamp column...")
                cursor.execute("ALTER TABLE recipe_history ADD COLUMN snapshot_timestamp TEXT")

                # Populate with current UTC timestamp for existing rows
                # Use snapshot_date at midnight UTC as default
                cursor.execute("""
                    UPDATE recipe_history 
                    SET snapshot_timestamp = snapshot_date || 'T00:00:00Z'
                    WHERE snapshot_timestamp IS NULL
                """)
                logger.info(f"  ✓ Updated {cursor.rowcount} existing records")

            conn.commit()
            logger.info("✓ Schema migration to v2 completed")

        except Exception as e:
            conn.rollback()
            logger.error(f"✗ Schema migration failed: {e}")
            raise

    def _migrate_from_v2_to_v3(self):
        """Migrate database from schema version 2 to 3 - add hourly snapshots and user support"""
        logger.info("🔧 Migrating database schema from v2 to v3...")
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # 1. Add user_id to recipes table
            cursor.execute("PRAGMA table_info(recipes)")
            columns = [col[1] for col in cursor.fetchall()]

            if 'user_id' not in columns:
                logger.info("  ✓ Adding user_id column to recipes table...")
                cursor.execute("ALTER TABLE recipes ADD COLUMN user_id TEXT")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_recipes_user ON recipes(user_id)")
                logger.info("  ✓ Created user_id column and index")

            # 2. Create hourly snapshots table
            logger.info("  ✓ Creating recipe_hourly_snapshots table...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS recipe_hourly_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    recipe_id TEXT NOT NULL,
                    installs INTEGER NOT NULL,
                    forks INTEGER NOT NULL,
                    popularity_score INTEGER NOT NULL,
                    snapshot_hour TEXT NOT NULL,  -- Format: YYYY-MM-DDTHH:00:00Z
                    snapshot_timestamp TEXT NOT NULL,
                    FOREIGN KEY (recipe_id) REFERENCES recipes(id),
                    UNIQUE(recipe_id, snapshot_hour)
                )
            """)

            # Create indices for hourly snapshots
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_hourly_snapshots_hour 
                ON recipe_hourly_snapshots(snapshot_hour)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_hourly_snapshots_recipe 
                ON recipe_hourly_snapshots(recipe_id, snapshot_hour)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_hourly_snapshots_timestamp 
                ON recipe_hourly_snapshots(snapshot_timestamp)
            """)

            # 3. Migrate existing daily snapshots to also have hourly resolution
            # Use INSERT OR IGNORE instead of ON CONFLICT DO NOTHING
            cursor.execute("""
                INSERT OR IGNORE INTO recipe_hourly_snapshots 
                (recipe_id, installs, forks, popularity_score, snapshot_hour, snapshot_timestamp)
                SELECT 
                    recipe_id, 
                    installs, 
                    forks, 
                    popularity_score, 
                    substr(snapshot_date, 1, 10) || 'T00:00:00Z' as snapshot_hour,
                    snapshot_timestamp
                FROM recipe_history
                WHERE snapshot_timestamp IS NOT NULL
            """)
            logger.info(f"  ✓ Migrated {cursor.rowcount} daily snapshots to hourly format")

            conn.commit()
            logger.info("✓ Schema migration to v3 completed")

        except Exception as e:
            conn.rollback()
            logger.error(f"✗ Schema migration failed: {e}")
            raise
    def initialize(self):
        """Initialize database schema with migrations"""
        logger.info(f"📊 Initializing database: {self.db_path}")

        conn = self.get_connection()
        cursor = conn.cursor()

        # Check current schema version
        current_version = self._get_schema_version()
        logger.info(f"  Current schema version: {current_version}")

        # Create main tables if they don't exist
        logger.info("  Creating/verifying tables...")

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
                snapshot_date TEXT NOT NULL,
                FOREIGN KEY (recipe_id) REFERENCES recipes(id),
                UNIQUE(recipe_id, snapshot_date)
            )
        """)

        # Create initial indices
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_recipe_history_date 
            ON recipe_history(snapshot_date)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_recipe_history_recipe 
            ON recipe_history(recipe_id, snapshot_date)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_recipes_popularity 
            ON recipes(popularity_score DESC)
        """)

        conn.commit()

        # Run migrations if needed
        if current_version < 1:
            self._set_schema_version(1)
            current_version = 1

        if current_version < 2:
            self._migrate_from_v1_to_v2()
            # Create additional index for timestamp
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_recipe_history_timestamp 
                ON recipe_history(snapshot_timestamp)
            """)
            self._set_schema_version(2)
        if current_version < 3:
            self._migrate_from_v2_to_v3()
            self._set_schema_version(3)
        logger.info("✓ Database schema initialized and up to date")

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

        # Schema version
        schema_version = self._get_schema_version()

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
            'schema_version': schema_version,
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

    def get_recipes_with_delta_since(self, cutoff: datetime) -> List[Dict]:
        """
        Get all recipes with their stats since a specific cutoff time
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        cutoff_iso = cutoff.isoformat()

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
                COALESCE(h.installs, 0) as past_installs,
                COALESCE(h.forks, 0) as past_forks,
                COALESCE(h.popularity_score, 0) as past_popularity,
                h.snapshot_timestamp as past_snapshot_timestamp,
                CASE WHEN h.snapshot_timestamp IS NOT NULL THEN 1 ELSE 0 END as has_history
            FROM recipes r
            LEFT JOIN (
                -- Get the most recent snapshot BEFORE cutoff for each recipe
                SELECT DISTINCT
                    recipe_id,
                    installs,
                    forks,
                    popularity_score,
                    snapshot_timestamp,
                    ROW_NUMBER() OVER (
                        PARTITION BY recipe_id 
                        ORDER BY snapshot_timestamp DESC
                    ) as rn
                FROM recipe_history
                WHERE snapshot_timestamp <= ?
            ) h ON r.id = h.recipe_id AND h.rn = 1
            ORDER BY r.popularity_score DESC
        """, (cutoff_iso,))

        results = []
        for row in cursor.fetchall():
            row_dict = dict(row)
            # Ensure all numeric fields are integers
            row_dict['current_installs'] = int(row_dict['current_installs'] or 0)
            row_dict['current_forks'] = int(row_dict['current_forks'] or 0)
            row_dict['current_popularity'] = int(row_dict['current_popularity'] or 0)
            row_dict['past_installs'] = int(row_dict['past_installs'] or 0)
            row_dict['past_forks'] = int(row_dict['past_forks'] or 0)
            row_dict['past_popularity'] = int(row_dict['past_popularity'] or 0)
            row_dict['has_history'] = bool(row_dict['has_history'])
            results.append(row_dict)

        return results
    def get_recipe_delta_since(self, recipe_id: str, cutoff: datetime) -> Optional[Dict]:
        """
        Get stats for a recipe since a specific cutoff time
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        cutoff_iso = cutoff.isoformat()

        # First get current stats
        cursor.execute("""
            SELECT 
                installs as current_installs,
                forks as current_forks,
                popularity_score as current_popularity
            FROM recipes
            WHERE id = ?
        """, (recipe_id,))

        current_row = cursor.fetchone()
        if not current_row:
            return None

        current = dict(current_row)

        # Get historical stats before cutoff
        cursor.execute("""
            SELECT 
                installs as past_installs,
                forks as past_forks,
                popularity_score as past_popularity,
                snapshot_timestamp as past_snapshot_timestamp
            FROM recipe_history
            WHERE recipe_id = ?
            AND snapshot_timestamp <= ?
            ORDER BY snapshot_timestamp DESC
            LIMIT 1
        """, (recipe_id, cutoff_iso))

        past_row = cursor.fetchone()

        if past_row:
            past = dict(past_row)
            has_data = True
        else:
            # No historical data before cutoff
            past = {
                'past_installs': 0,
                'past_forks': 0,
                'past_popularity': 0,
                'past_snapshot_timestamp': None
            }
            has_data = False

        # Calculate deltas
        result = {
            'current_installs': current['current_installs'],
            'current_forks': current['current_forks'],
            'current_popularity': current['current_popularity'],
            'past_installs': past['past_installs'],
            'past_forks': past['past_forks'],
            'past_popularity': past['past_popularity'],
            'past_snapshot_timestamp': past['past_snapshot_timestamp'],
            'has_data': has_data,
            'delta_installs': current['current_installs'] - past['past_installs'],
            'delta_forks': current['current_forks'] - past['past_forks'],
            'delta_popularity': current['current_popularity'] - past['past_popularity']
        }

        return result

    def save_hourly_snapshot(self, recipe_id: str, installs: int, forks: int):
        """Save an hourly snapshot of recipe stats"""
        conn = self.get_connection()
        cursor = conn.cursor()

        # Use UTC for all timestamps
        now_utc = datetime.utcnow()
        snapshot_hour = now_utc.replace(minute=0, second=0, microsecond=0).isoformat() + 'Z'
        snapshot_timestamp = now_utc.isoformat() + 'Z'
        popularity_score = installs + forks

        # First, ensure the hourly snapshots table exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS recipe_hourly_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recipe_id TEXT NOT NULL,
                installs INTEGER NOT NULL,
                forks INTEGER NOT NULL,
                popularity_score INTEGER NOT NULL,
                snapshot_hour TEXT NOT NULL,
                snapshot_timestamp TEXT NOT NULL,
                FOREIGN KEY (recipe_id) REFERENCES recipes(id),
                UNIQUE(recipe_id, snapshot_hour)
            )
        """)

        # Use INSERT OR REPLACE for SQLite compatibility
        cursor.execute("""
            INSERT OR REPLACE INTO recipe_hourly_snapshots 
            (recipe_id, installs, forks, popularity_score, snapshot_hour, snapshot_timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (recipe_id, installs, forks, popularity_score, snapshot_hour, snapshot_timestamp))

        conn.commit()
        logger.debug(f"💾 Saved hourly snapshot for recipe {recipe_id} at {snapshot_hour}")

    def cleanup_hourly_snapshots(self, hours_to_keep: int = 24 * 30):  # Keep 30 days by default
        """Remove old hourly snapshots beyond retention period"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cutoff = datetime.utcnow() - timedelta(hours=hours_to_keep)
        cutoff_iso = cutoff.isoformat() + 'Z'

        cursor.execute("""
            DELETE FROM recipe_hourly_snapshots
            WHERE snapshot_timestamp < ?
        """, (cutoff_iso,))

        deleted = cursor.rowcount
        conn.commit()

        if deleted > 0:
            logger.info(f"🗑️  Cleaned up {deleted} old hourly snapshots (older than {cutoff_iso})")

        return deleted

    def cleanup_old_daily_snapshots(self, days_to_keep: int = 180):
        """Remove old daily snapshots beyond retention period"""
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
            logger.info(f"🗑️  Cleaned up {deleted} old daily snapshots (older than {cutoff_date})")

        return deleted

    def get_recipe_delta_since_hours(self, recipe_id: str, hours_ago: int) -> Optional[Dict]:
        """
        Get stats for a recipe since N hours ago using hourly snapshots
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        cutoff = datetime.utcnow() - timedelta(hours=hours_ago)
        cutoff_iso = cutoff.isoformat() + 'Z'

        # Get current stats
        cursor.execute("""
            SELECT 
                installs as current_installs,
                forks as current_forks,
                popularity_score as current_popularity
            FROM recipes
            WHERE id = ?
        """, (recipe_id,))

        current_row = cursor.fetchone()
        if not current_row:
            return None

        current = dict(current_row)

        # Get hourly snapshot before cutoff
        cursor.execute("""
            SELECT 
                installs as past_installs,
                forks as past_forks,
                popularity_score as past_popularity,
                snapshot_timestamp as past_snapshot_timestamp
            FROM recipe_hourly_snapshots
            WHERE recipe_id = ?
            AND snapshot_timestamp <= ?
            ORDER BY snapshot_timestamp DESC
            LIMIT 1
        """, (recipe_id, cutoff_iso))

        past_row = cursor.fetchone()

        if past_row:
            past = dict(past_row)
            has_data = True
        else:
            # Try to get from daily snapshots as fallback
            cursor.execute("""
                SELECT 
                    installs as past_installs,
                    forks as past_forks,
                    popularity_score as past_popularity,
                    snapshot_timestamp as past_snapshot_timestamp
                FROM recipe_history
                WHERE recipe_id = ?
                AND snapshot_timestamp <= ?
                ORDER BY snapshot_timestamp DESC
                LIMIT 1
            """, (recipe_id, cutoff_iso))

            past_row = cursor.fetchone()
            if past_row:
                past = dict(past_row)
                has_data = True
            else:
                past = {
                    'past_installs': 0,
                    'past_forks': 0,
                    'past_popularity': 0,
                    'past_snapshot_timestamp': None
                }
                has_data = False

        # Calculate deltas
        result = {
            'recipe_id': recipe_id,
            'current_installs': current['current_installs'],
            'current_forks': current['current_forks'],
            'current_popularity': current['current_popularity'],
            'past_installs': past['past_installs'],
            'past_forks': past['past_forks'],
            'past_popularity': past['past_popularity'],
            'past_snapshot_timestamp': past['past_snapshot_timestamp'],
            'has_data': has_data,
            'delta_installs': current['current_installs'] - past['past_installs'],
            'delta_forks': current['current_forks'] - past['past_forks'],
            'delta_popularity': current['current_popularity'] - past['past_popularity']
        }

        return result

    def get_recipes_by_user(self, user_id: str) -> List[Dict]:
        """Get all recipes for a specific user"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM recipes 
            WHERE user_id = ?
            ORDER BY popularity_score DESC
        """, (user_id,))

        return [dict(row) for row in cursor.fetchall()]

    def get_recipes_with_delta_since_hours(self, hours_ago: int, user_id: Optional[str] = None) -> List[Dict]:
        """
        Get all recipes with their stats since N hours ago using hourly snapshots
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        cutoff = datetime.utcnow() - timedelta(hours=hours_ago)
        cutoff_iso = cutoff.isoformat() + 'Z'

        # Build query with optional user filter
        query = """
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
                r.user_id,
                COALESCE(h.installs, 0) as past_installs,
                COALESCE(h.forks, 0) as past_forks,
                COALESCE(h.popularity_score, 0) as past_popularity,
                h.snapshot_timestamp as past_snapshot_timestamp,
                CASE WHEN h.snapshot_timestamp IS NOT NULL THEN 1 ELSE 0 END as has_history
            FROM recipes r
            LEFT JOIN (
                -- Get the most recent hourly snapshot BEFORE cutoff for each recipe
                SELECT DISTINCT
                    recipe_id,
                    installs,
                    forks,
                    popularity_score,
                    snapshot_timestamp,
                    ROW_NUMBER() OVER (
                        PARTITION BY recipe_id 
                        ORDER BY snapshot_timestamp DESC
                    ) as rn
                FROM recipe_hourly_snapshots
                WHERE snapshot_timestamp <= ?
            ) h ON r.id = h.recipe_id AND h.rn = 1
        """

        params = [cutoff_iso]

        if user_id:
            query += " WHERE r.user_id = ?"
            params.append(user_id)

        query += " ORDER BY r.popularity_score DESC"

        cursor.execute(query, params)

        results = []
        for row in cursor.fetchall():
            row_dict = dict(row)
            # Ensure all numeric fields are integers
            row_dict['current_installs'] = int(row_dict['current_installs'] or 0)
            row_dict['current_forks'] = int(row_dict['current_forks'] or 0)
            row_dict['current_popularity'] = int(row_dict['current_popularity'] or 0)
            row_dict['past_installs'] = int(row_dict['past_installs'] or 0)
            row_dict['past_forks'] = int(row_dict['past_forks'] or 0)
            row_dict['past_popularity'] = int(row_dict['past_popularity'] or 0)
            row_dict['has_history'] = bool(row_dict['has_history'])
            results.append(row_dict)

        return results



    def get_last_hourly_snapshot_time(self) -> Optional[datetime]:
        """Get the timestamp of the most recent hourly snapshot"""
        conn = self.get_connection()
        cursor = conn.cursor()

        # Ensure table exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS recipe_hourly_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recipe_id TEXT NOT NULL,
                installs INTEGER NOT NULL,
                forks INTEGER NOT NULL,
                popularity_score INTEGER NOT NULL,
                snapshot_hour TEXT NOT NULL,
                snapshot_timestamp TEXT NOT NULL,
                FOREIGN KEY (recipe_id) REFERENCES recipes(id),
                UNIQUE(recipe_id, snapshot_hour)
            )
        """)

        cursor.execute("""
            SELECT MAX(snapshot_hour) as last_hour
            FROM recipe_hourly_snapshots
        """)

        row = cursor.fetchone()
        if row and row['last_hour']:
            try:
                # Parse the hour string (e.g., "2026-02-06T12:00:00Z")
                hour_str = row['last_hour'].replace('Z', '+00:00')
                return datetime.fromisoformat(hour_str)
            except:
                return None
        return None


    def get_missing_hours(self, max_hours_back: int = 24) -> List[datetime]:
        """
        Get list of hours that are missing snapshots in the last N hours
        Returns: List of datetime objects (hour boundaries) that need snapshots
        """
        last_hourly = self.get_last_hourly_snapshot_time()
        now = datetime.utcnow()

        # If we have no snapshots at all, we need to create one for the current hour
        if not last_hourly:
            return [now.replace(minute=0, second=0, microsecond=0)]

        missing_hours = []
        current_hour = now.replace(minute=0, second=0, microsecond=0)

        # Start from the hour after the last snapshot
        next_hour = last_hourly.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

        # Collect all missing hours up to current hour (max 24 hours back)
        while next_hour <= current_hour and len(missing_hours) < max_hours_back:
            missing_hours.append(next_hour)
            next_hour += timedelta(hours=1)

        return missing_hours


    def create_hourly_snapshot_for_hour(self, recipe_id: str, installs: int, forks: int, target_hour: datetime):
        """
        Create a historical hourly snapshot for a specific hour
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        snapshot_hour = target_hour.isoformat().replace('+00:00', 'Z')
        snapshot_timestamp = target_hour.isoformat().replace('+00:00', 'Z')
        popularity_score = installs + forks

        cursor.execute("""
            INSERT OR REPLACE INTO recipe_hourly_snapshots 
            (recipe_id, installs, forks, popularity_score, snapshot_hour, snapshot_timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (recipe_id, installs, forks, popularity_score, snapshot_hour, snapshot_timestamp))

        conn.commit()
        logger.debug(f"💾 Created historical hourly snapshot for recipe {recipe_id} at {snapshot_hour}")

    def should_refresh_user_recipes(self, user_id: str, max_hours: int = 24) -> bool:
        """
        Check if we should refresh user recipes based on last fetch time
        Returns: True if we should refresh, False if recent data exists
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        # Check if we have any recipes for this user
        cursor.execute("SELECT COUNT(*) as count FROM recipes WHERE user_id = ?", (user_id,))
        count = cursor.fetchone()['count']

        if count == 0:
            # No recipes at all for this user, definitely refresh
            return True

        # Check when we last fetched any recipe for this user
        cursor.execute("""
            SELECT MAX(last_fetched) as last_fetch_time 
            FROM recipes 
            WHERE user_id = ?
        """, (user_id,))

        row = cursor.fetchone()
        if not row or not row['last_fetch_time']:
            # No last_fetched timestamp, refresh
            return True

        try:
            # Parse the last fetch time
            last_fetch = datetime.fromisoformat(row['last_fetch_time'].replace('Z', '+00:00'))
            hours_since = (datetime.utcnow() - last_fetch).total_seconds() / 3600

            # Refresh if older than max_hours
            return hours_since > max_hours
        except:
            # Error parsing, refresh to be safe
            return True