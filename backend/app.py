#!/usr/bin/env python3
"""
TRMNL Trending Recipes Plugin
Tracks recipe statistics over time and provides trending recipes
"""

import asyncio
import logging
import os
import threading
import time
from datetime import datetime, timedelta
from functools import wraps
from typing import Optional

import httpx
from flask import Flask, jsonify, request
from flask_cors import CORS

from modules.database import Database
from modules.recipe_fetcher import RecipeFetcher
from modules.trending_calculator import TrendingCalculator

# ============================================================================
# CONFIGURATION
# ============================================================================

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Environment variables
ENABLE_IP_WHITELIST = os.getenv('ENABLE_IP_WHITELIST', 'true').lower() == 'true'
IP_REFRESH_HOURS = int(os.getenv('IP_REFRESH_HOURS', '24'))
TRMNL_IPS_API = 'https://trmnl.com/api/ips'
LOCALHOST_IPS = ['127.0.0.1', '::1', 'localhost']
DATABASE_PATH = os.getenv('DATABASE_PATH', '/data/recipes.db')
FETCH_INTERVAL_HOURS = int(os.getenv('FETCH_INTERVAL_HOURS', '24'))
WORKER_LOCK_FILE = os.getenv('WORKER_LOCK_FILE', '/tmp/trmnl-primary-worker.lock')

# Global IP whitelist state
TRMNL_IPS = set(LOCALHOST_IPS)
TRMNL_IPS_LOCK = threading.Lock()
last_ip_refresh: Optional[datetime] = None

# Worker coordination
is_primary_worker = False

# ============================================================================
# FLASK APP
# ============================================================================

app = Flask(__name__)
CORS(app)

# Initialize components
db = Database(DATABASE_PATH)
recipe_fetcher = RecipeFetcher(db)
trending_calculator = TrendingCalculator(db)


# ============================================================================
# WORKER COORDINATION
# ============================================================================

def try_acquire_primary_worker():
    """
    Try to become the primary worker (for background jobs).
    Uses a file lock to ensure only one worker runs background tasks.
    Returns True if this worker becomes primary.
    """
    import fcntl

    try:
        # Try to create and lock the file
        lock_fd = os.open(WORKER_LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)

        # If we got here, we created the file - we're the primary worker
        os.write(lock_fd, f"{os.getpid()}\n".encode())

        logger.info(f"✓ This worker (PID {os.getpid()}) is the PRIMARY worker")
        return True

    except FileExistsError:
        # Lock file already exists - another worker is primary
        logger.info(f"✓ This worker (PID {os.getpid()}) is a SECONDARY worker (background jobs disabled)")
        return False
    except Exception as e:
        # If we can't create the lock, assume we're not primary
        logger.warning(f"⚠️  Could not acquire primary worker lock: {e}")
        return False


# ============================================================================
# IP WHITELIST
# ============================================================================

async def fetch_trmnl_ips():
    """Fetch current TRMNL server IPs from their API"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(TRMNL_IPS_API)
            response.raise_for_status()
            data = response.json()

            ipv4_list = data.get('data', {}).get('ipv4', [])
            ipv6_list = data.get('data', {}).get('ipv6', [])

            ips = set(ipv4_list + ipv6_list + LOCALHOST_IPS)

            logger.info(f"✓ Loaded {len(ips)} TRMNL IPs ({len(ipv4_list)} IPv4, {len(ipv6_list)} IPv6)")
            return ips

    except Exception as e:
        logger.error(f"✗ Failed to fetch TRMNL IPs: {e}")
        return set(LOCALHOST_IPS)


def update_trmnl_ips_sync():
    """Update TRMNL IPs - sync wrapper for background thread"""
    global TRMNL_IPS, last_ip_refresh

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            ips = loop.run_until_complete(fetch_trmnl_ips())
            with TRMNL_IPS_LOCK:
                TRMNL_IPS = ips
                last_ip_refresh = datetime.now()
        finally:
            loop.close()
    except Exception as e:
        logger.error(f"✗ IP refresh error: {e}")


def ip_refresh_worker():
    """Background worker that refreshes TRMNL IPs exactly on the hour"""
    while True:
        try:
            # Calculate time until next hour
            now = datetime.now()
            minutes_to_next_hour = 60 - now.minute
            seconds_to_next_hour = (minutes_to_next_hour * 60) - now.second

            # Wait until next hour
            logger.info(f"⏰ IP refresh: Waiting {minutes_to_next_hour} minutes until next hour")
            time.sleep(seconds_to_next_hour)

            # Refresh IPs
            logger.info("🔄 Refreshing TRMNL IPs...")
            update_trmnl_ips_sync()

            # Wait until next scheduled refresh time
            logger.info(f"⏰ IP refresh: Next refresh in {IP_REFRESH_HOURS} hours")
            time.sleep(IP_REFRESH_HOURS * 3600 - 1)  # Subtract 1 second to align with hour

        except Exception as e:
            logger.error(f"✗ IP refresh worker error: {e}")
            time.sleep(3600)  # Retry in 1 hour on error


def start_ip_refresh_worker():
    """Start background thread for IP refresh"""
    if not ENABLE_IP_WHITELIST:
        return

    worker_thread = threading.Thread(
        target=ip_refresh_worker,
        daemon=True,
        name='IP-Refresh-Worker'
    )
    worker_thread.start()
    logger.info(f"✓ IP refresh worker started (every {IP_REFRESH_HOURS}h, aligned to hour)")


def get_allowed_ips():
    """Get current list of allowed IPs from TRMNL API"""
    with TRMNL_IPS_LOCK:
        return TRMNL_IPS.copy()


def get_client_ip():
    """Get the real client IP address, accounting for proxies"""
    if request.headers.get('CF-Connecting-IP'):
        return request.headers.get('CF-Connecting-IP').strip()
    if request.headers.get('X-Forwarded-For'):
        return request.headers.get('X-Forwarded-For').split(',')[0].strip()
    if request.headers.get('X-Real-IP'):
        return request.headers.get('X-Real-IP').strip()
    return request.remote_addr


def require_whitelisted_ip(f):
    """Decorator to enforce IP whitelisting on routes"""

    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not ENABLE_IP_WHITELIST:
            return f(*args, **kwargs)

        client_ip = get_client_ip()
        allowed_ips = get_allowed_ips()

        if client_ip not in allowed_ips:
            logger.warning(f"🚫 Blocked unauthorized IP: {client_ip}")
            return jsonify({
                'error': 'Access denied',
                'message': 'Your IP address is not authorized to access this service'
            }), 403

        return f(*args, **kwargs)

    return decorated_function


# ============================================================================
# BACKGROUND JOBS
# ============================================================================

def recipe_fetch_worker():
    """Background worker that fetches recipes exactly on the hour"""
    while True:
        try:
            # Calculate time until next hour
            now = datetime.now()
            minutes_to_next_hour = 60 - now.minute
            seconds_to_next_hour = (minutes_to_next_hour * 60) - now.second

            # Wait until next hour
            logger.info(f"⏰ Recipe fetch: Waiting {minutes_to_next_hour} minutes until next hour")
            time.sleep(seconds_to_next_hour)

            # Run the fetch
            logger.info("🔄 Starting recipe fetch job...")
            asyncio.run(recipe_fetcher.fetch_all_recipes())

            # Calculate next fetch time
            logger.info(f"✓ Recipe fetch complete. Next fetch in {FETCH_INTERVAL_HOURS} hours")

            # Wait until next scheduled fetch time (aligned to hour)
            time.sleep(FETCH_INTERVAL_HOURS * 3600 - 1)  # Subtract 1 second to stay aligned

        except Exception as e:
            logger.error(f"✗ Recipe fetch worker error: {e}")
            # On error, wait until next hour before retrying
            now = datetime.now()
            minutes_to_next_hour = 60 - now.minute
            seconds_to_next_hour = (minutes_to_next_hour * 60) - now.second
            logger.info(f"⏰ Recipe fetch: Retrying in {minutes_to_next_hour} minutes")
            time.sleep(seconds_to_next_hour)


def start_recipe_fetch_worker():
    """Start background thread for recipe fetching"""
    worker_thread = threading.Thread(
        target=recipe_fetch_worker,
        daemon=True,
        name='Recipe-Fetch-Worker'
    )
    worker_thread.start()
    logger.info(f"✓ Recipe fetch worker started (every {FETCH_INTERVAL_HOURS}h, aligned to hour)")


# ============================================================================
# TIME HELPER FUNCTIONS
# ============================================================================

def get_local_midnight(utc_offset_seconds: int = 0):
    """
    Get the local midnight time based on UTC offset.

    Args:
        utc_offset_seconds: Offset from UTC in seconds

    Returns:
        datetime object for the most recent midnight in local time (in UTC)
    """
    now = datetime.utcnow()
    # Adjust to local time
    local_now = now + timedelta(seconds=utc_offset_seconds)
    # Get midnight in local time
    local_midnight = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    # Convert back to UTC for consistency
    utc_midnight = local_midnight - timedelta(seconds=utc_offset_seconds)
    return utc_midnight


# ============================================================================
# API ROUTES
# ============================================================================

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    # Get UTC offset from query parameter (default to 0)
    try:
        utc_offset = int(request.args.get('utc_offset', '0'))
    except ValueError:
        utc_offset = 0

    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'local_midnight': get_local_midnight(utc_offset).isoformat(),
        'utc_offset_seconds': utc_offset,
        'ip_whitelist_enabled': ENABLE_IP_WHITELIST,
        'last_ip_refresh': last_ip_refresh.isoformat() if last_ip_refresh else None,
        'is_primary_worker': is_primary_worker
    })


# Replace the existing /trending endpoint with:

@app.route('/trending', methods=['GET'])
@app.route('/trending/', methods=['GET'])
@require_whitelisted_ip
def get_trending():
    """
    Get trending recipes with clear timeframe options

    Query parameters:
    - timeframe: today, week, 24h, 7d, 30d, 180d (default: 24h)
                 or legacy: 1d, 1w, 1m, 6m
    - limit: number of results (default: 10)
    - utc_offset: UTC offset in seconds for calendar calculations (default: 0)
    """
    timeframe = request.args.get('timeframe', request.args.get('duration', '24h'))
    limit = int(request.args.get('limit', '10'))

    # Get UTC offset
    try:
        utc_offset = int(request.args.get('utc_offset', '0'))
    except ValueError:
        return jsonify({
            'error': 'Invalid UTC offset',
            'message': 'UTC offset must be an integer number of seconds'
        }), 400

    try:
        trending_data = trending_calculator.calculate_trending(
            timeframe=timeframe,
            limit=limit,
            utc_offset_seconds=utc_offset
        )

        return jsonify(trending_data)

    except ValueError as e:
        return jsonify({
            'error': 'Invalid timeframe',
            'message': str(e),
            'valid_timeframes': list(trending_calculator.TIMEFRAMES.keys())
        }), 400
    except Exception as e:
        logger.error(f"✗ Error calculating trending: {e}")
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/trending/all', methods=['GET'])
@require_whitelisted_ip
def get_all_trending():
    """
    Get trending recipes for all timeframes at once
    """
    try:
        utc_offset = int(request.args.get('utc_offset', '0'))
    except ValueError:
        return jsonify({
            'error': 'Invalid UTC offset',
            'message': 'UTC offset must be an integer number of seconds'
        }), 400

    limit = int(request.args.get('limit', '10'))

    try:
        all_trending = trending_calculator.get_all_timeframes_trending(
            limit=limit,
            utc_offset_seconds=utc_offset
        )

        return jsonify({
            'timeframes': all_trending,
            'utc_offset_seconds': utc_offset,
            'current_time': datetime.utcnow().isoformat()
        })

    except Exception as e:
        logger.error(f"✗ Error calculating all trending: {e}")
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/trending/timeframes', methods=['GET'])
def get_timeframes():
    """
    Get information about available trending timeframes
    """
    timeframes_info = {}

    for timeframe, info in trending_calculator.TIMEFRAMES.items():
        timeframes_info[timeframe] = {
            'type': info['type'],
            'description': info['description'],
            'hours': info.get('hours'),
            'example_url': f"/trending?timeframe={timeframe}&utc_offset=3600"
        }

    return jsonify({
        'available_timeframes': timeframes_info,
        'recommended_for_new_users': ['24h', '7d', '30d'],
        'note': 'Calendar timeframes (today, week) require sufficient historical data'
    })

@app.route('/api/recipe/<recipe_id>', methods=['GET'])
@require_whitelisted_ip
def get_recipe(recipe_id):
    """Get details for a specific recipe"""
    try:
        recipe = db.get_recipe_current(recipe_id)
        if not recipe:
            return jsonify({
                'error': 'Not found',
                'message': f'Recipe {recipe_id} not found'
            }), 404

        return jsonify(recipe)
    except Exception as e:
        logger.error(f"✗ Error fetching recipe: {e}")
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/debug/snapshots-overview', methods=['GET'])
def debug_snapshots_overview():
    """Check snapshot coverage"""
    conn = db.get_connection()
    cursor = conn.cursor()

    # Get all snapshot dates
    cursor.execute("""
        SELECT 
            snapshot_date,
            COUNT(*) as recipe_count,
            MIN(snapshot_timestamp) as earliest,
            MAX(snapshot_timestamp) as latest
        FROM recipe_history
        GROUP BY snapshot_date
        ORDER BY snapshot_date DESC
    """)

    dates = [dict(row) for row in cursor.fetchall()]

    # Get total recipe count
    cursor.execute("SELECT COUNT(*) as count FROM recipes")
    total_recipes = cursor.fetchone()['count']

    # Check which dates have coverage
    coverage = {}
    for date_info in dates:
        date = date_info['snapshot_date']
        coverage_pct = (date_info['recipe_count'] / total_recipes * 100) if total_recipes > 0 else 0
        coverage[date] = {
            'recipe_count': date_info['recipe_count'],
            'coverage_pct': round(coverage_pct, 1),
            'earliest': date_info['earliest'],
            'latest': date_info['latest']
        }

    return jsonify({
        'total_recipes': total_recipes,
        'snapshot_dates': dates,
        'coverage': coverage,
        'diagnosis': 'Missing yesterday snapshots' if len(dates) < 2 else 'Has multiple days'
    })
@app.route('/debug/recipe/<recipe_id>/history', methods=['GET'])
def debug_recipe_history(recipe_id):
    """Debug why a recipe has no historical data"""
    try:
        # Get recipe info
        recipe = db.get_recipe_current(recipe_id)
        if not recipe:
            return jsonify({'error': 'Recipe not found'}), 404

        # Get all snapshots
        conn = db.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT snapshot_date, snapshot_timestamp, installs, forks
            FROM recipe_history
            WHERE recipe_id = ?
            ORDER BY snapshot_date DESC
            LIMIT 10
        """, (recipe_id,))

        snapshots = [dict(row) for row in cursor.fetchall()]

        # Check for yesterday's date
        yesterday = (datetime.utcnow() - timedelta(days=1)).date().isoformat()
        has_yesterday = any(s['snapshot_date'] == yesterday for s in snapshots)

        # Check cutoff for "today"
        cutoff = datetime.utcnow() - timedelta(hours=24)
        cutoff_iso = cutoff.isoformat()

        cursor.execute("""
            SELECT COUNT(*) as count
            FROM recipe_history
            WHERE recipe_id = ?
            AND snapshot_timestamp <= ?
        """, (recipe_id, cutoff_iso))

        has_before_cutoff = cursor.fetchone()['count'] > 0

        return jsonify({
            'recipe_id': recipe_id,
            'published_at': recipe.get('created_at'),
            'recipe_age_days': (datetime.utcnow() - datetime.fromisoformat(
                recipe['created_at'].replace('Z', ''))).total_seconds() / 86400 if recipe.get('created_at') else 0,
            'total_snapshots': len(snapshots),
            'snapshots': snapshots,
            'has_yesterday_snapshot': has_yesterday,
            'has_snapshot_before_24h_cutoff': has_before_cutoff,
            'cutoff_time': cutoff_iso,
            'yesterday_date': yesterday,
            'diagnosis': 'Recipe is old but missing recent snapshots' if not has_yesterday else 'Has yesterday snapshot'
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500
@app.route('/trending/today', methods=['GET'])
@require_whitelisted_ip
def get_trending_today():
    """Get trending since local midnight today"""
    return redirect_trending('today')


@app.route('/trending/24h', methods=['GET'])
@require_whitelisted_ip
def get_trending_24h():
    """Get trending for last 24 hours (rolling)"""
    return redirect_trending('24h')


@app.route('/trending/week', methods=['GET'])
@require_whitelisted_ip
def get_trending_week():
    """Get trending since start of week (Monday)"""
    return redirect_trending('week')


@app.route('/trending/7d', methods=['GET'])
@require_whitelisted_ip
def get_trending_7d():
    """Get trending for last 7 days (rolling)"""
    return redirect_trending('7d')


@app.route('/trending/30d', methods=['GET'])
@require_whitelisted_ip
def get_trending_30d():
    """Get trending for last 30 days (rolling)"""
    return redirect_trending('30d')


def redirect_trending(timeframe: str):
    """Helper to redirect specific timeframe endpoints to main trending"""
    args = request.args.copy()
    args['timeframe'] = timeframe

    # Build query string
    query_string = '&'.join([f"{k}={v}" for k, v in args.items()])

    # Use internal redirect to avoid external redirect
    with app.test_request_context(f'/trending?{query_string}'):
        return get_trending()

@app.route('/api/stats', methods=['GET'])
@require_whitelisted_ip
def get_stats():
    """Get overall statistics"""
    try:
        # Get UTC offset from query parameter
        try:
            utc_offset = int(request.args.get('utc_offset', '0'))
        except ValueError:
            utc_offset = 0

        stats = db.get_statistics()
        stats['utc_offset_seconds'] = utc_offset
        stats['local_midnight'] = get_local_midnight(utc_offset).isoformat()
        return jsonify(stats)
    except Exception as e:
        logger.error(f"✗ Error fetching stats: {e}")
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


# ============================================================================
# STARTUP
# ============================================================================

def initialize():
    """Initialize the application"""
    global is_primary_worker

    logger.info("🚀 Starting TRMNL Trending Recipes Plugin")

    # Initialize database (all workers need this)
    db.initialize()

    # Determine if this worker should run background jobs
    is_primary_worker = try_acquire_primary_worker()

    # Load initial TRMNL IPs (only primary worker)
    if ENABLE_IP_WHITELIST and is_primary_worker:
        logger.info("🔐 IP whitelist enabled")
        update_trmnl_ips_sync()
        start_ip_refresh_worker()
    elif ENABLE_IP_WHITELIST and not is_primary_worker:
        logger.info("🔐 IP whitelist enabled (managed by primary worker)")
        # Secondary workers still need to load the IPs once
        update_trmnl_ips_sync()
    else:
        logger.warning("⚠️  IP whitelist DISABLED - all IPs allowed!")

    # Start background workers (only primary worker)
    if is_primary_worker:
        start_recipe_fetch_worker()

    logger.info("✓ Application initialized successfully")


# Initialize on module load (for gunicorn)
initialize()

# Cleanup handler for graceful shutdown
import atexit


def cleanup_primary_worker():
    """Remove primary worker lock file on shutdown"""
    if is_primary_worker and os.path.exists(WORKER_LOCK_FILE):
        try:
            os.remove(WORKER_LOCK_FILE)
            logger.info("✓ Primary worker lock cleaned up")
        except Exception as e:
            logger.warning(f"⚠️  Could not remove lock file: {e}")


atexit.register(cleanup_primary_worker)

if __name__ == '__main__':
    # Run Flask development server (for local testing only)
    port = int(os.getenv('PORT', '5000'))
    app.run(host='0.0.0.0', port=port, debug=False)