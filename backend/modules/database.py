"""
Database module for TRMNL Trending Recipes
Handles SQLite operations and schema management
"""

import logging
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class Database:
    """SQLite database handler for recipe tracking"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.schema_version = 6  # Current schema version
        # Each cache maps key -> {'result': ..., 'timestamp': float}
        self._rank_cache: dict = {}
        self._rank_cache_ttl = 60 * 60        # 1 hour — ranks only change after hourly fetch
        self._stats_cache: dict = {}
        self._stats_cache_ttl = 5 * 60        # 5 minutes
        self._delta_cache: dict = {}
        self._delta_cache_ttl = 5 * 60        # 5 minutes

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

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_hourly_snapshots_recipe_ts
                ON recipe_hourly_snapshots(recipe_id, snapshot_timestamp DESC)
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
    def _migrate_from_v3_to_v4(self):
        """Migrate database from schema version 3 to 4 - add categories column"""
        logger.info("🔧 Migrating database schema from v3 to v4...")
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("PRAGMA table_info(recipes)")
            columns = [col[1] for col in cursor.fetchall()]

            if 'categories' not in columns:
                logger.info("  ✓ Adding categories column to recipes table...")
                cursor.execute("ALTER TABLE recipes ADD COLUMN categories TEXT")
                logger.info("  ✓ Created categories column")

            conn.commit()
            logger.info("✓ Schema migration to v4 completed")

        except Exception as e:
            conn.rollback()
            logger.error(f"✗ Schema migration failed: {e}")
            raise

    def _migrate_from_v5_to_v6(self):
        """Migrate database from schema version 5 to 6 - add is_active flag to recipes"""
        logger.info("🔧 Migrating database schema from v5 to v6...")
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("PRAGMA table_info(recipes)")
            columns = [col[1] for col in cursor.fetchall()]

            if 'is_active' not in columns:
                logger.info("  ✓ Adding is_active column to recipes table...")
                cursor.execute("ALTER TABLE recipes ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_recipes_active ON recipes(is_active)")

            conn.commit()
            logger.info("✓ Schema migration to v6 completed")

        except Exception as e:
            conn.rollback()
            logger.error(f"✗ Schema migration failed: {e}")
            raise

    def _migrate_from_v4_to_v5(self):
        """Migrate database from schema version 4 to 5 - add materialized view tables for ranks"""
        logger.info("🔧 Migrating database schema from v4 to v5...")
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS mv_recipe_ranks (
                    recipe_id TEXT PRIMARY KEY,
                    popularity_score INTEGER NOT NULL,
                    global_rank INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS mv_user_ranks (
                    user_id TEXT PRIMARY KEY,
                    total_popularity INTEGER NOT NULL,
                    recipe_count INTEGER NOT NULL,
                    global_rank INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)

            conn.commit()
            logger.info("✓ Schema migration to v5 completed")

            # Populate MVs immediately
            self.refresh_materialized_views()

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
                last_fetched TEXT NOT NULL,
                user_id TEXT,
                categories TEXT,
                is_active INTEGER NOT NULL DEFAULT 1
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
                snapshot_timestamp TEXT,
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
            CREATE INDEX IF NOT EXISTS idx_recipe_history_recipe_ts
            ON recipe_history(recipe_id, snapshot_timestamp DESC)
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

        if current_version < 4:
            self._migrate_from_v3_to_v4()
            self._set_schema_version(4)

        if current_version < 5:
            self._migrate_from_v4_to_v5()
            self._set_schema_version(5)

        if current_version < 6:
            self._migrate_from_v5_to_v6()
            self._set_schema_version(6)

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
                updated_at, last_fetched, user_id, categories, is_active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
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
                last_fetched = excluded.last_fetched,
                user_id = excluded.user_id,
                categories = excluded.categories,
                is_active = 1
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
            now,
            recipe_data.get('user_id'),
            recipe_data.get('categories')
        ))

        conn.commit()

    def mark_inactive_recipes(self, seen_ids: List[str]):
        """Mark recipes not seen in the latest poll as inactive"""
        conn = self.get_connection()
        cursor = conn.cursor()

        if not seen_ids:
            return

        placeholders = ','.join('?' * len(seen_ids))
        cursor.execute(f"""
            UPDATE recipes SET is_active = 0
            WHERE id NOT IN ({placeholders}) AND is_active = 1
        """, seen_ids)

        deactivated = cursor.rowcount
        conn.commit()

        if deactivated > 0:
            logger.info(f"🔕 Marked {deactivated} recipes as inactive (not seen in latest poll)")

    def save_snapshot(self, recipe_id: str, installs: int, forks: int):
        """Save a daily snapshot of recipe stats using UTC.
        Only inserts if no entry exists for that day (keeps earliest snapshot)."""
        conn = self.get_connection()
        cursor = conn.cursor()

        # Use UTC for all timestamps
        now_utc = datetime.utcnow()
        snapshot_date = now_utc.date().isoformat()  # UTC date
        snapshot_timestamp = now_utc.isoformat()  # UTC datetime
        popularity_score = installs + forks

        cursor.execute("""
            INSERT OR IGNORE INTO recipe_history (
                recipe_id, installs, forks, popularity_score, snapshot_date, snapshot_timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?)
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
                r.categories,
                r.installs as current_installs,
                r.forks as current_forks,
                r.popularity_score as current_popularity,
                r.url,
                r.icon_url,
                r.thumbnail_url,
                r.created_at,
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

    def cleanup_hourly_snapshots(self, hours_to_keep: int = 24 * 7):  # Keep 7 days by default
        """Promote earliest hourly snapshot per recipe per day to daily history,
        then delete all hourly snapshots beyond the retention period."""
        conn = self.get_connection()
        cursor = conn.cursor()

        cutoff = datetime.utcnow() - timedelta(hours=hours_to_keep)
        cutoff_iso = cutoff.isoformat() + 'Z'

        # Promote earliest hourly snapshot per recipe per day into recipe_history.
        # Uses MIN(snapshot_timestamp) per (recipe_id, day) via GROUP BY — avoids
        # the O(N log N) sort that ROW_NUMBER() OVER (...) requires.
        cursor.execute("""
            INSERT OR IGNORE INTO recipe_history (
                recipe_id, installs, forks, popularity_score, snapshot_date, snapshot_timestamp
            )
            SELECT h.recipe_id, h.installs, h.forks, h.popularity_score,
                   date(h.snapshot_timestamp) as snapshot_date,
                   h.snapshot_timestamp
            FROM (
                SELECT recipe_id, date(snapshot_timestamp) as day,
                       MIN(snapshot_timestamp) as min_ts
                FROM recipe_hourly_snapshots
                WHERE snapshot_timestamp < ?
                GROUP BY recipe_id, date(snapshot_timestamp)
            ) earliest
            JOIN recipe_hourly_snapshots h
                ON h.recipe_id = earliest.recipe_id
               AND h.snapshot_timestamp = earliest.min_ts
        """, (cutoff_iso,))

        promoted = cursor.rowcount
        if promoted > 0:
            logger.info(f"📦 Promoted {promoted} earliest hourly snapshots to daily history")

        # Delete expired hourly snapshots
        cursor.execute("""
            DELETE FROM recipe_hourly_snapshots
            WHERE snapshot_timestamp < ?
        """, (cutoff_iso,))

        deleted = cursor.rowcount

        # Downsample recipe_history: for rows older than 90 days, keep only the earliest
        # snapshot per recipe per month and delete the rest (daily → monthly resolution).
        monthly_cutoff = (datetime.utcnow() - timedelta(days=90)).isoformat()
        cursor.execute("""
            DELETE FROM recipe_history
            WHERE snapshot_timestamp < ?
              AND snapshot_timestamp NOT IN (
                  SELECT MIN(snapshot_timestamp)
                  FROM recipe_history
                  WHERE snapshot_timestamp < ?
                  GROUP BY recipe_id, strftime('%Y-%m', snapshot_timestamp)
              )
        """, (monthly_cutoff, monthly_cutoff))
        downsampled = cursor.rowcount

        conn.commit()

        if deleted > 0:
            logger.info(f"🗑️  Cleaned up {deleted} hourly snapshots (older than {cutoff_iso})")
        if downsampled > 0:
            logger.info(f"🗑️  Downsampled {downsampled} daily history rows to monthly (older than 90d)")

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
        start_time = time.time()

        ids_key = tuple(sorted(recipe_ids)) if recipe_ids is not None else None
        cache_key = f"{hours_ago}_{ids_key}"
        current_time = datetime.utcnow().timestamp()
        entry = self._delta_cache.get(cache_key)
        if entry and current_time - entry['timestamp'] < self._delta_cache_ttl:
            logger.debug(f"⚡ get_recipes_with_delta_since_hours({hours_ago}h) cache hit ({len(entry['result'])} recipes)")
            return entry['result']

        conn = self.get_connection()
        cursor = conn.cursor()

        cutoff = datetime.utcnow() - timedelta(hours=hours_ago)
        cutoff_iso = cutoff.isoformat() + 'Z'

        # Find the latest snapshot timestamp per recipe before the cutoff using GROUP BY +
        # MAX (hash aggregation — O(N)), then point-join back to fetch the actual row data
        # using the (recipe_id, snapshot_timestamp) index. Avoids the O(N log N) sort
        # that ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) requires.
        query = """
            WITH latest_ts AS (
                SELECT recipe_id, MAX(snapshot_timestamp) AS max_ts
                FROM (
                    SELECT recipe_id, snapshot_timestamp
                    FROM recipe_hourly_snapshots
                    WHERE snapshot_timestamp <= ?
                    UNION ALL
                    SELECT recipe_id, snapshot_timestamp
                    FROM recipe_history
                    WHERE snapshot_timestamp <= ?
                )
                GROUP BY recipe_id
            )
            SELECT
                r.id,
                r.name,
                r.description,
                r.categories,
                r.installs AS current_installs,
                r.forks AS current_forks,
                r.popularity_score AS current_popularity,
                r.url,
                r.icon_url,
                r.thumbnail_url,
                r.created_at,
                r.user_id,
                COALESCE(h.installs, rh.installs, 0) AS past_installs,
                COALESCE(h.forks, rh.forks, 0) AS past_forks,
                COALESCE(h.popularity_score, rh.popularity_score, 0) AS past_popularity,
                lt.max_ts AS past_snapshot_timestamp,
                CASE WHEN lt.max_ts IS NOT NULL THEN 1 ELSE 0 END AS has_history
            FROM recipes r
            LEFT JOIN latest_ts lt ON lt.recipe_id = r.id
            LEFT JOIN recipe_hourly_snapshots h
                ON h.recipe_id = lt.recipe_id AND h.snapshot_timestamp = lt.max_ts
            LEFT JOIN recipe_history rh
                ON rh.recipe_id = lt.recipe_id AND rh.snapshot_timestamp = lt.max_ts
            WHERE r.is_active = 1
        """

        params: list = [cutoff_iso, cutoff_iso]

        if recipe_ids is not None:
            if not recipe_ids:
                return []
            placeholders = ','.join('?' * len(recipe_ids))
            query += f" AND r.id IN ({placeholders})"
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

        duration = time.time() - start_time
        logger.info(f"⏱️ get_recipes_with_delta_since_hours({hours_ago}h): {len(results)} recipes in {duration:.3f}s")

        self._delta_cache[cache_key] = {'result': results, 'timestamp': current_time}
        return results

    def get_recipe_ids_for_user(self, user_id: str) -> List[str]:
        """Get recipe IDs for a user from the recipes table"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM recipes WHERE user_id = ?", (user_id,))
        return [row['id'] for row in cursor.fetchall()]

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


    def log_table_stats(self):
        """Log row counts and snapshot age ranges for key tables"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) as cnt FROM recipes WHERE is_active = 1")
        recipes_count = cursor.fetchone()['cnt']

        cursor.execute("""
            SELECT COUNT(*) as cnt,
                   MIN(snapshot_timestamp) as oldest,
                   MAX(snapshot_timestamp) as newest
            FROM recipe_hourly_snapshots
        """)
        row = cursor.fetchone()
        hourly_count = row['cnt']
        hourly_oldest = row['oldest']
        hourly_newest = row['newest']

        cursor.execute("""
            SELECT COUNT(*) as cnt,
                   MIN(snapshot_timestamp) as oldest,
                   MAX(snapshot_timestamp) as newest
            FROM recipe_history
        """)
        row = cursor.fetchone()
        history_count = row['cnt']
        history_oldest = row['oldest']
        history_newest = row['newest']

        logger.info(
            f"📊 Table stats: recipes={recipes_count} | "
            f"hourly_snapshots={hourly_count:,} ({hourly_oldest} → {hourly_newest}) | "
            f"recipe_history={history_count:,} ({history_oldest} → {history_newest})"
        )

    def refresh_materialized_views(self):
        """Refresh the materialized view tables for recipe and user ranks"""
        conn = self.get_connection()
        cursor = conn.cursor()
        now = datetime.utcnow().isoformat()

        try:
            # Refresh mv_recipe_ranks
            cursor.execute("DELETE FROM mv_recipe_ranks")
            cursor.execute("""
                INSERT INTO mv_recipe_ranks (recipe_id, popularity_score, global_rank, updated_at)
                SELECT id, popularity_score,
                       ROW_NUMBER() OVER (ORDER BY popularity_score DESC) as global_rank,
                       ?
                FROM recipes
                WHERE popularity_score > 0 AND is_active = 1
            """, (now,))
            recipe_count = cursor.rowcount

            # Refresh mv_user_ranks
            cursor.execute("DELETE FROM mv_user_ranks")
            cursor.execute("""
                INSERT INTO mv_user_ranks (user_id, total_popularity, recipe_count, global_rank, updated_at)
                SELECT user_id, SUM(popularity_score) as total_popularity,
                       COUNT(*) as recipe_count,
                       ROW_NUMBER() OVER (ORDER BY SUM(popularity_score) DESC) as global_rank,
                       ?
                FROM recipes
                WHERE user_id IS NOT NULL AND popularity_score > 0 AND is_active = 1
                GROUP BY user_id
            """, (now,))
            user_count = cursor.rowcount

            conn.commit()

            # Invalidate all query caches
            self._rank_cache = {}
            self._stats_cache = {}
            self._delta_cache = {}

            logger.info(f"✓ Materialized views refreshed: {recipe_count} recipes, {user_count} developers")
            self.log_table_stats()

        except Exception as e:
            conn.rollback()
            logger.error(f"✗ Failed to refresh materialized views: {e}")
            raise

    def get_user_rank(self, user_id: str) -> Optional[Dict]:
        """Get a developer's rank from the materialized view"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT global_rank, total_popularity, recipe_count
            FROM mv_user_ranks
            WHERE user_id = ?
        """, (user_id,))

        row = cursor.fetchone()
        if row:
            return {
                'global_rank': row['global_rank'],
                'total_popularity': row['total_popularity'],
                'recipe_count': row['recipe_count'],
            }
        return None

    def get_total_developers(self) -> int:
        """Get total number of developers with ranked recipes"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) as total FROM mv_user_ranks")
        return cursor.fetchone()['total']

    def should_fetch_all_recipes(self, max_hours: int = 1) -> bool:
        """Check if we should fetch all recipes (based on last fetch time)"""
        last_fetch = self.get_last_recipe_fetch_time()

        if not last_fetch:
            # Never fetched before
            return True

        hours_since = (datetime.utcnow() - last_fetch).total_seconds() / 3600
        return hours_since > max_hours

    def compute_global_ranks(self, timeframe: str, cutoff: datetime) -> Dict[str, Dict[str, int]]:
        """Compute current global rank and rank improvement with caching"""
        start_time = time.time()

        # Round cutoff to the hour so the cache key is stable across requests
        cutoff_rounded = cutoff.replace(minute=0, second=0, microsecond=0)
        cache_key = f"{timeframe}_{cutoff_rounded.isoformat()}"
        current_time = datetime.utcnow().timestamp()
        cutoff_iso = cutoff.isoformat()
        entry = self._rank_cache.get(cache_key)
        if entry and current_time - entry['timestamp'] < self._rank_cache_ttl:
            logger.debug(f"⚡ compute_global_ranks({timeframe}) cache hit")
            return entry['result']

        # Read current ranks from materialized view, compute past ranks for rank_difference
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            WITH current_ranked AS (
                SELECT id AS recipe_id,
                       ROW_NUMBER() OVER (ORDER BY popularity_score DESC) AS global_rank
                FROM recipes
                WHERE popularity_score > 0
            ),
            latest_ts AS (
                -- GROUP BY + MAX: hash aggregation O(N), avoids O(N log N) sort of ROW_NUMBER
                SELECT recipe_id, MAX(snapshot_timestamp) AS max_ts
                FROM (
                    SELECT recipe_id, snapshot_timestamp FROM recipe_history WHERE snapshot_timestamp <= ?
                    UNION ALL
                    SELECT recipe_id, snapshot_timestamp FROM recipe_hourly_snapshots WHERE snapshot_timestamp <= ?
                )
                GROUP BY recipe_id
            ),
            latest_before_cutoff AS (
                -- Point-join back to fetch popularity using the (recipe_id, snapshot_timestamp) index
                SELECT lt.recipe_id AS id,
                       COALESCE(h.popularity_score, rh.popularity_score) AS popularity_score
                FROM latest_ts lt
                LEFT JOIN recipe_hourly_snapshots h
                    ON h.recipe_id = lt.recipe_id AND h.snapshot_timestamp = lt.max_ts
                LEFT JOIN recipe_history rh
                    ON rh.recipe_id = lt.recipe_id AND rh.snapshot_timestamp = lt.max_ts
            ),
            past_ranked AS (
                -- ROW_NUMBER here ranks only ~800 recipes, not millions of rows
                SELECT id,
                       ROW_NUMBER() OVER (ORDER BY popularity_score DESC) as past_rank
                FROM latest_before_cutoff
                WHERE popularity_score > 0
            ),
            total_active AS (
                SELECT COUNT(*) as cnt FROM recipes WHERE popularity_score > 0
            )
            SELECT
                cr.recipe_id as id,
                cr.global_rank as current_rank,
                COALESCE(pr.past_rank, (SELECT cnt FROM total_active) + 1) as past_rank
            FROM current_ranked cr
            LEFT JOIN past_ranked pr ON cr.recipe_id = pr.id
        """, (cutoff_iso, cutoff_iso))

        # Build result
        result = {}
        for row in cursor.fetchall():
            result[row['id']] = {
                'global_rank': row['current_rank'],
                'rank_difference': row['past_rank'] - row['current_rank']
            }

        # Cache the result
        self._rank_cache[cache_key] = {'result': result, 'timestamp': current_time}

        duration = time.time() - start_time
        logger.info(f"⏱️ compute_global_ranks({timeframe}): {len(result)} recipes in {duration:.3f}s")

        return result
    def get_global_stats(self, cutoff: datetime, use_hourly: bool = False) -> Dict:
        """Get total connections (sum of popularity) now and at cutoff"""
        start_time = time.time()

        # Cache keyed on rounded-hour cutoff + table choice
        cutoff_rounded = cutoff.replace(minute=0, second=0, microsecond=0)
        cache_key = f"{'h' if use_hourly else 'd'}_{cutoff_rounded.isoformat()}"
        current_time = datetime.utcnow().timestamp()
        entry = self._stats_cache.get(cache_key)
        if entry and current_time - entry['timestamp'] < self._stats_cache_ttl:
            logger.debug(f"⚡ get_global_stats cache hit")
            return entry['result']

        conn = self.get_connection()
        cursor = conn.cursor()

        # Current total
        cursor.execute("SELECT COALESCE(SUM(popularity_score), 0) as total FROM recipes")
        total_now = cursor.fetchone()['total']

        # Past total from closest snapshot before cutoff for each recipe
        cutoff_iso = cutoff.isoformat() + ('Z' if use_hourly else '')
        table = 'recipe_hourly_snapshots' if use_hourly else 'recipe_history'

        cursor.execute(f"""
            SELECT COALESCE(SUM(s.popularity_score), 0) as total
            FROM (
                SELECT recipe_id, MAX(snapshot_timestamp) AS max_ts
                FROM {table}
                WHERE snapshot_timestamp <= ?
                GROUP BY recipe_id
            ) latest
            JOIN {table} s ON s.recipe_id = latest.recipe_id AND s.snapshot_timestamp = latest.max_ts
        """, (cutoff_iso,))
        total_past = cursor.fetchone()['total']

        duration = time.time() - start_time
        logger.info(f"⏱️ get_global_stats(use_hourly={use_hourly}): {duration:.3f}s")

        result = {
            'total_connections': total_now,
            'total_connections_delta': total_now - total_past,
        }
        self._stats_cache[cache_key] = {'result': result, 'timestamp': current_time}
        return result