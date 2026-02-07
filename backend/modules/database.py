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

        # user_recipes cache table (no migration needed)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_recipes (
                user_id TEXT NOT NULL,
                recipe_id TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                UNIQUE(user_id, recipe_id)
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_recipes_user
            ON user_recipes(user_id)
        """)
        conn.commit()

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

    def get_recipes_with_delta_since(self, cutoff: datetime, recipe_ids: Optional[List[str]] = None) -> List[Dict]:
        """
        Get all recipes with their stats since a specific cutoff time.
        Optionally filter to only the given recipe IDs.
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        cutoff_iso = cutoff.isoformat()

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
                COALESCE(h.installs, 0) as past_installs,
                COALESCE(h.forks, 0) as past_forks,
                COALESCE(h.popularity_score, 0) as past_popularity,
                h.snapshot_timestamp as past_snapshot_timestamp,
                CASE WHEN h.snapshot_timestamp IS NOT NULL THEN 1 ELSE 0 END as has_history
            FROM recipes r
            LEFT JOIN (
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
        """

        params: list = [cutoff_iso]

        if recipe_ids is not None:
            if not recipe_ids:
                return []
            placeholders = ','.join('?' * len(recipe_ids))
            query += f" WHERE r.id IN ({placeholders})"
            params.extend(recipe_ids)

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

    def get_recipes_with_delta_since_hours(self, hours_ago: int, recipe_ids: Optional[List[str]] = None) -> List[Dict]:
        """
        Get all recipes with their stats since N hours ago using hourly snapshots.
        Optionally filter to only the given recipe IDs.
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        cutoff = datetime.utcnow() - timedelta(hours=hours_ago)
        cutoff_iso = cutoff.isoformat() + 'Z'

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

        params: list = [cutoff_iso]

        if recipe_ids is not None:
            if not recipe_ids:
                return []
            placeholders = ','.join('?' * len(recipe_ids))
            query += f" WHERE r.id IN ({placeholders})"
            params.extend(recipe_ids)

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

    def get_user_recipe_ids(self, user_id: str) -> List[str]:
        """Return cached recipe IDs for a user"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT recipe_id FROM user_recipes WHERE user_id = ?", (user_id,))
        return [row['recipe_id'] for row in cursor.fetchall()]

    def save_user_recipes(self, user_id: str, recipe_ids: List[str]):
        """Replace all cached recipe IDs for a user"""
        conn = self.get_connection()
        cursor = conn.cursor()
        now = datetime.utcnow().isoformat()
        cursor.execute("DELETE FROM user_recipes WHERE user_id = ?", (user_id,))
        for rid in recipe_ids:
            cursor.execute(
                "INSERT OR IGNORE INTO user_recipes (user_id, recipe_id, fetched_at) VALUES (?, ?, ?)",
                (user_id, rid, now)
            )
        conn.commit()

    def is_user_recipes_stale(self, user_id: str, max_hours: int = 24) -> bool:
        """Check if the user recipe cache needs a refresh"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT MIN(fetched_at) as oldest FROM user_recipes WHERE user_id = ?",
            (user_id,)
        )
        row = cursor.fetchone()
        if not row or not row['oldest']:
            return True
        try:
            fetched = datetime.fromisoformat(row['oldest'])
            return (datetime.utcnow() - fetched).total_seconds() / 3600 > max_hours
        except Exception:
            return True

    def get_last_recipe_fetch_time(self) -> Optional[datetime]:
        """Get when recipes were last fetched"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT MAX(last_fetched) as last_fetch FROM recipes")
        row = cursor.fetchone()

        if row and row['last_fetch']:
            try:
                return datetime.fromisoformat(row['last_fetch'].replace('Z', '+00:00'))
            except:
                return None
        return None


    def should_fetch_all_recipes(self, max_hours: int = 1) -> bool:
        """Check if we should fetch all recipes (based on last fetch time)"""
        last_fetch = self.get_last_recipe_fetch_time()

        if not last_fetch:
            # Never fetched before
            return True

        hours_since = (datetime.utcnow() - last_fetch).total_seconds() / 3600
        return hours_since > max_hours