"""
Tests for trending calculation logic.
Covers: delta calculation, snapshot fallbacks, inactive recipes,
        calendar timeframes, dual-list mode, and ranking.
"""
import pytest
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from modules.database import Database
from modules.trending_calculator import TrendingCalculator


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    d = Database(":memory:")
    d.initialize()
    return d


@pytest.fixture
def calc(db):
    return TrendingCalculator(db)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(hours_ago=0, days_ago=0):
    return (datetime.utcnow() - timedelta(hours=hours_ago, days=days_ago)).isoformat() + 'Z'


def insert_recipe(db, id, name, popularity, age_days=365, is_active=1, user_id=None):
    conn = db.get_connection()
    now = datetime.utcnow().isoformat()
    conn.execute("""
        INSERT OR REPLACE INTO recipes
            (id, name, installs, forks, popularity_score,
             updated_at, last_fetched, created_at, is_active, user_id)
        VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?)
    """, (id, name, popularity, popularity, now, now, _ts(days_ago=age_days), is_active, user_id))
    conn.commit()


def insert_hourly_snapshot(db, recipe_id, popularity, hours_ago):
    conn = db.get_connection()
    t = datetime.utcnow() - timedelta(hours=hours_ago)
    snapshot_hour = t.replace(minute=0, second=0, microsecond=0).isoformat() + 'Z'
    conn.execute("""
        INSERT OR REPLACE INTO recipe_hourly_snapshots
            (recipe_id, installs, forks, popularity_score, snapshot_hour, snapshot_timestamp)
        VALUES (?, ?, 0, ?, ?, ?)
    """, (recipe_id, popularity, popularity, snapshot_hour, t.isoformat() + 'Z'))
    conn.commit()


def insert_daily_snapshot(db, recipe_id, popularity, days_ago):
    conn = db.get_connection()
    t = datetime.utcnow() - timedelta(days=days_ago)
    conn.execute("""
        INSERT OR IGNORE INTO recipe_history
            (recipe_id, installs, forks, popularity_score, snapshot_date, snapshot_timestamp)
        VALUES (?, ?, 0, ?, ?, ?)
    """, (recipe_id, popularity, popularity, t.date().isoformat(), t.isoformat() + 'Z'))
    conn.commit()


def recipe_ids(result):
    return [r['id'] for r in result['recipes']]


# ---------------------------------------------------------------------------
# Bug regression: deleted plugins appeared as top gainers
# ---------------------------------------------------------------------------

class TestDeletedPluginBug:
    def test_recipe_with_no_snapshots_excluded(self, db, calc):
        """Deleted plugin with no snapshots must not appear in trending."""
        insert_recipe(db, 'deleted', 'Old Plugin', popularity=200)

        result = calc.calculate_trending('24h')

        assert 'deleted' not in recipe_ids(result)

    def test_recipe_with_no_snapshots_has_zero_delta_in_include_all(self, db, calc):
        """In include_all mode, deleted plugin shows delta=0, not full popularity."""
        insert_recipe(db, 'deleted', 'Old Plugin', popularity=200)

        result = calc.calculate_trending('24h', include_all=True)

        match = next((r for r in result['recipes'] if r['id'] == 'deleted'), None)
        assert match is not None
        assert match['popularity_delta'] == 0


# ---------------------------------------------------------------------------
# Delta calculation
# ---------------------------------------------------------------------------

class TestDeltaCalculation:
    def test_hourly_snapshot_gives_correct_delta(self, db, calc):
        insert_recipe(db, 'r1', 'Recipe', popularity=100)
        insert_hourly_snapshot(db, 'r1', popularity=60, hours_ago=25)

        result = calc.calculate_trending('24h')

        match = next(r for r in result['recipes'] if r['id'] == 'r1')
        assert match['popularity_delta'] == 40
        assert match['popularity'] == 100

    def test_daily_history_fallback_when_hourly_pruned(self, db, calc):
        """When hourly snapshots are pruned, daily history is used as baseline."""
        insert_recipe(db, 'r1', 'Established Recipe', popularity=150)
        # No hourly snapshot — simulate pruned hourly data
        insert_daily_snapshot(db, 'r1', popularity=100, days_ago=8)

        result = calc.calculate_trending('7d')

        match = next(r for r in result['recipes'] if r['id'] == 'r1')
        assert match['popularity_delta'] == 50

    def test_new_recipe_uses_full_popularity_as_delta_for_long_window(self, db, calc):
        """Recipe published within the window started from 0 — delta equals full popularity."""
        insert_recipe(db, 'new', 'New Recipe', popularity=100, age_days=10)
        insert_hourly_snapshot(db, 'new', popularity=5, hours_ago=5 * 24)

        result = calc.calculate_trending('30d')

        match = next(r for r in result['recipes'] if r['id'] == 'new')
        assert match['popularity_delta'] == 100

    def test_new_recipe_visible_in_short_window(self, db, calc):
        """Same new recipe should surface correctly in a shorter timeframe."""
        insert_recipe(db, 'new', 'New Recipe', popularity=100, age_days=10)
        insert_hourly_snapshot(db, 'new', popularity=50, hours_ago=25)

        result = calc.calculate_trending('24h')

        match = next((r for r in result['recipes'] if r['id'] == 'r1'), None)
        # Verify the new recipe appears with the correct 24h delta
        match = next(r for r in result['recipes'] if r['id'] == 'new')
        assert match['popularity_delta'] == 50

    def test_zero_growth_recipe_excluded(self, db, calc):
        """Recipe with no popularity change is not trending."""
        insert_recipe(db, 'flat', 'Flat Recipe', popularity=80)
        insert_hourly_snapshot(db, 'flat', popularity=80, hours_ago=25)

        result = calc.calculate_trending('24h')

        assert 'flat' not in recipe_ids(result)

    def test_multiple_recipes_sorted_by_trending_score(self, db, calc):
        """Recipes are returned highest trending score first."""
        insert_recipe(db, 'slow', 'Slow Grower', popularity=110)
        insert_hourly_snapshot(db, 'slow', popularity=100, hours_ago=25)  # delta=10

        insert_recipe(db, 'fast', 'Fast Grower', popularity=160)
        insert_hourly_snapshot(db, 'fast', popularity=100, hours_ago=25)  # delta=60

        result = calc.calculate_trending('24h')
        ids = recipe_ids(result)

        assert ids.index('fast') < ids.index('slow')


# ---------------------------------------------------------------------------
# Inactive recipes
# ---------------------------------------------------------------------------

class TestInactiveRecipes:
    def test_inactive_recipe_excluded_from_trending(self, db, calc):
        insert_recipe(db, 'gone', 'Removed Recipe', popularity=200, is_active=0)
        insert_hourly_snapshot(db, 'gone', popularity=100, hours_ago=25)

        result = calc.calculate_trending('24h')

        assert 'gone' not in recipe_ids(result)

    def test_inactive_recipe_excluded_from_include_all(self, db, calc):
        insert_recipe(db, 'gone', 'Removed Recipe', popularity=200, is_active=0)
        insert_hourly_snapshot(db, 'gone', popularity=100, hours_ago=25)

        result = calc.calculate_trending('24h', include_all=True)

        assert 'gone' not in recipe_ids(result)

    def test_mark_inactive_recipes(self, db):
        insert_recipe(db, 'r1', 'Active', popularity=50)
        insert_recipe(db, 'r2', 'Gone', popularity=30)

        db.mark_inactive_recipes(['r1'])

        conn = db.get_connection()
        row = conn.execute("SELECT is_active FROM recipes WHERE id = 'r2'").fetchone()
        assert row['is_active'] == 0

    def test_mark_inactive_does_not_affect_seen_recipes(self, db):
        insert_recipe(db, 'r1', 'Active', popularity=50)

        db.mark_inactive_recipes(['r1'])

        conn = db.get_connection()
        row = conn.execute("SELECT is_active FROM recipes WHERE id = 'r1'").fetchone()
        assert row['is_active'] == 1

    def test_reactivated_recipe_comes_back(self, db, calc):
        """A recipe that reappears in the API (upsert) becomes active again."""
        insert_recipe(db, 'r1', 'Returning Recipe', popularity=50, is_active=0)

        db.upsert_recipe({
            'id': 'r1', 'name': 'Returning Recipe', 'installs': 50, 'forks': 0,
            'categories': '', 'url': '', 'icon_url': '', 'thumbnail_url': '',
            'created_at': _ts(days_ago=365), 'updated_at': _ts(),
        })

        conn = db.get_connection()
        row = conn.execute("SELECT is_active FROM recipes WHERE id = 'r1'").fetchone()
        assert row['is_active'] == 1


# ---------------------------------------------------------------------------
# Calendar timeframes
# ---------------------------------------------------------------------------

class TestCalendarTimeframes:
    def test_month_timeframe_does_not_crash(self, db, calc):
        """Month timeframe must not raise TypeError from None / int."""
        insert_recipe(db, 'r1', 'Old Recipe', popularity=100, age_days=60)
        insert_daily_snapshot(db, 'r1', popularity=80, days_ago=40)

        result = calc.calculate_trending('month')

        assert 'recipes' in result
        assert result['timeframe']['key'] == 'month'

    def test_month_shows_new_recipe_with_full_delta(self, db, calc):
        """Recipe published this month shows full popularity as delta (started from 0)."""
        insert_recipe(db, 'new', 'New This Month', popularity=131, age_days=17)
        insert_hourly_snapshot(db, 'new', popularity=2, hours_ago=17 * 24)

        result = calc.calculate_trending('month')

        match = next((r for r in result['recipes'] if r['id'] == 'new'), None)
        assert match is not None
        assert match['popularity_delta'] == 131

    def test_today_timeframe_returns_results(self, db, calc):
        insert_recipe(db, 'r1', 'Recipe', popularity=50, age_days=60)
        # 25h ago is always before midnight, giving a valid baseline for 'today'
        insert_hourly_snapshot(db, 'r1', popularity=30, hours_ago=25)

        result = calc.calculate_trending('today')

        assert result['timeframe']['key'] == 'today'
        assert any(r['id'] == 'r1' for r in result['recipes'])

    def test_week_timeframe_returns_results(self, db, calc):
        insert_recipe(db, 'r1', 'Recipe', popularity=50, age_days=60)
        insert_hourly_snapshot(db, 'r1', popularity=20, hours_ago=5 * 24)

        result = calc.calculate_trending('week')

        assert result['timeframe']['key'] == 'week'


# ---------------------------------------------------------------------------
# Dual-list mode (user_id provided)
# ---------------------------------------------------------------------------

class TestDualListMode:
    def test_user_recipes_partitioned_correctly(self, db, calc):
        insert_recipe(db, 'mine', 'My Recipe', popularity=80, user_id='u1')
        insert_recipe(db, 'public', 'Public Recipe', popularity=120)
        insert_hourly_snapshot(db, 'mine', popularity=50, hours_ago=25)
        insert_hourly_snapshot(db, 'public', popularity=80, hours_ago=25)

        result = calc.calculate_trending('24h', user_id='u1',
                                         user_recipe_ids=['mine'])

        assert any(r['id'] == 'mine' for r in result['user_recipes'])
        assert any(r['id'] == 'public' for r in result['recipes'])
        assert all(r['id'] != 'mine' for r in result['recipes'])

    def test_user_recipes_with_zero_delta_excluded_by_default(self, db, calc):
        insert_recipe(db, 'mine', 'My Recipe', popularity=80, user_id='u1')
        insert_hourly_snapshot(db, 'mine', popularity=80, hours_ago=25)  # flat

        result = calc.calculate_trending('24h', user_id='u1',
                                         user_recipe_ids=['mine'])

        assert len(result['user_recipes']) == 0

    def test_user_recipes_with_zero_delta_shown_when_include_unchanged(self, db, calc):
        insert_recipe(db, 'mine', 'My Recipe', popularity=80, user_id='u1')
        insert_hourly_snapshot(db, 'mine', popularity=80, hours_ago=25)

        result = calc.calculate_trending('24h', user_id='u1',
                                         user_recipe_ids=['mine'],
                                         include_unchanged=True)

        assert any(r['id'] == 'mine' for r in result['user_recipes'])

    def test_limit_respected_in_dual_list(self, db, calc):
        for i in range(20):
            insert_recipe(db, f'r{i}', f'Recipe {i}', popularity=100 + i)
            insert_hourly_snapshot(db, f'r{i}', popularity=50, hours_ago=25)

        result = calc.calculate_trending('24h', limit=5, user_recipe_ids=[])

        assert len(result['recipes']) <= 5


# ---------------------------------------------------------------------------
# Ranking
# ---------------------------------------------------------------------------

class TestRanking:
    def test_top_gainer_flagged(self, db, calc):
        insert_recipe(db, 'r1', 'Top', popularity=200)
        insert_recipe(db, 'r2', 'Second', popularity=120)
        insert_hourly_snapshot(db, 'r1', popularity=100, hours_ago=25)  # delta=100
        insert_hourly_snapshot(db, 'r2', popularity=100, hours_ago=25)  # delta=20

        result = calc.calculate_trending('24h')

        top = next(r for r in result['recipes'] if r['id'] == 'r1')
        second = next(r for r in result['recipes'] if r['id'] == 'r2')
        assert top.get('is_top_gainer') is True
        assert 'is_top_gainer' not in second

    def test_trending_rank_starts_at_one(self, db, calc):
        insert_recipe(db, 'r1', 'Recipe', popularity=100)
        insert_hourly_snapshot(db, 'r1', popularity=50, hours_ago=25)

        result = calc.calculate_trending('24h')

        assert result['recipes'][0]['trending_rank'] == 1

    def test_tied_recipes_share_rank(self, db, calc):
        for rid in ('r1', 'r2'):
            insert_recipe(db, rid, rid, popularity=100)
            insert_hourly_snapshot(db, rid, popularity=50, hours_ago=25)  # both delta=50

        result = calc.calculate_trending('24h')

        ranks = [r['trending_rank'] for r in result['recipes']]
        assert ranks[0] == ranks[1] == 1

    def test_id_included_in_response(self, db, calc):
        insert_recipe(db, 'r1', 'Recipe', popularity=100)
        insert_hourly_snapshot(db, 'r1', popularity=50, hours_ago=25)

        result = calc.calculate_trending('24h')

        assert result['recipes'][0]['id'] == 'r1'

    def test_new_recipe_rank_difference_uses_first_snapshot(self, db, calc):
        """New recipes use their oldest snapshot as the past rank baseline."""
        # Several established recipes with high popularity before the window
        for i, pop in enumerate([500, 400, 300, 200, 180, 160, 150]):
            insert_recipe(db, f'old{i}', f'Established {i}', popularity=pop)
            insert_daily_snapshot(db, f'old{i}', popularity=pop, days_ago=35)

        # new: published within window — popularity=2 at first poll, now 131
        # current rank: somewhere in the middle; past rank: near the bottom (popularity=2)
        insert_recipe(db, 'new', 'New Recipe', popularity=131, age_days=17)
        insert_hourly_snapshot(db, 'new', popularity=2, hours_ago=17 * 24)

        db.refresh_materialized_views()
        result = calc.calculate_trending('30d')

        match = next(r for r in result['recipes'] if r['id'] == 'new')
        # 'new' moved up from near-last (popularity=2) to a mid-table rank (popularity=131)
        assert match.get('rank_difference', 0) > 0

    def test_deleted_plugin_rank_difference_is_zero(self, db, calc):
        """Plugins with no snapshots at all show rank_difference=0."""
        insert_recipe(db, 'gone', 'Deleted', popularity=100, is_active=0)

        result = calc.calculate_trending('30d', include_all=True)

        match = next((r for r in result['recipes'] if r['id'] == 'gone'), None)
        if match:
            assert match.get('rank_difference', 0) == 0
