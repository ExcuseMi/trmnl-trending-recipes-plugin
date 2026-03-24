import pytest
import time
from datetime import datetime, timedelta
from modules.database import Database
from modules.trending_calculator import TrendingCalculator

def test_trending_calculation_performance(tmp_path):
    """Verify that trending calculation is fast even with thousands of recipes."""
    db_path = str(tmp_path / "perf_test.db")
    db = Database(db_path)
    db.initialize()
    conn = db.get_connection()
    
    num_recipes = 1000
    days_of_history = 31
    now = datetime.utcnow()
    
    # Insert recipes
    recipes = []
    for i in range(num_recipes):
        recipe_id = f"recipe_{i}"
        name = f"Recipe {i}"
        popularity = 100 + i
        created_at = (now - timedelta(days=60)).isoformat() + 'Z'
        updated_at = now.isoformat() + 'Z'
        recipes.append((recipe_id, name, popularity, popularity, updated_at, updated_at, created_at, 1))
    
    conn.executemany("""
        INSERT INTO recipes (id, name, installs, forks, popularity_score, updated_at, last_fetched, created_at, is_active)
        VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?)
    """, recipes)
    
    # Insert daily snapshots
    history = []
    for d in range(1, days_of_history + 1):
        snapshot_date = (now - timedelta(days=d)).date().isoformat()
        snapshot_timestamp = (now - timedelta(days=d)).isoformat() + 'Z'
        for i in range(num_recipes):
            recipe_id = f"recipe_{i}"
            popularity = 50 + i
            history.append((recipe_id, popularity, popularity, snapshot_date, snapshot_timestamp))
    
    conn.executemany("""
        INSERT INTO recipe_history (recipe_id, installs, forks, popularity_score, snapshot_date, snapshot_timestamp)
        VALUES (?, ?, 0, ?, ?, ?)
    """, history)
    
    conn.commit()
    db.refresh_materialized_views()
    
    calc = TrendingCalculator(db)
    
    # Measure 30d trending calculation
    start_time = time.time()
    result = calc.calculate_trending(timeframe='30d', limit=50)
    duration = time.time() - start_time
    
    # It should take less than 0.5s for 1000 recipes on a typical machine
    # (My test with 5000 took 0.2s, so 0.5s is safe)
    assert duration < 0.5, f"Trending calculation too slow: {duration:.2f}s"
    assert len(result['recipes']) > 0

def test_rank_difference_correctness_for_new_recipes(tmp_path):
    """Verify that rank_difference is still calculated correctly for new recipes after optimization."""
    db_path = str(tmp_path / "rank_test.db")
    db = Database(db_path)
    db.initialize()
    
    # 1. Established recipes
    for i in range(10):
        db.upsert_recipe({'id': f'old{i}', 'name': f'Old {i}', 'installs': 500 - i*10})
        # No snapshots, but they exist
        
    # 2. A new recipe that just appeared
    now_iso = datetime.utcnow().isoformat() + 'Z'
    db.upsert_recipe({'id': 'new', 'name': 'New', 'installs': 600, 'created_at': now_iso}) # Should be rank 1
    
    db.refresh_materialized_views()
    calc = TrendingCalculator(db)
    
    # 30d window, no snapshots exist
    result = calc.calculate_trending('30d')
    
    new_recipe = next(r for r in result['recipes'] if r['id'] == 'new')
    assert new_recipe['global_rank'] == 1
    # Since it had no snapshots, past_rank fallback should be total+1 = 12
    # rank_difference = 12 - 1 = 11
    assert new_recipe['rank_difference'] == 11
