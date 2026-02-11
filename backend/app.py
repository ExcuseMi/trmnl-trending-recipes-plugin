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
LOG_LEVEL = logging.DEBUG if os.getenv('DEBUG', 'false').lower() == 'true' else logging.INFO
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Environment variables
ENABLE_IP_WHITELIST = os.getenv('ENABLE_IP_WHITELIST', 'true').lower() == 'true'
IP_REFRESH_HOURS = int(os.getenv('IP_REFRESH_HOURS', '24'))
TRMNL_IPS_API = 'https://trmnl.com/api/ips'
LOCALHOST_IPS = ['127.0.0.1', '::1', 'localhost']
DATABASE_PATH = os.getenv('DATABASE_PATH', '/data/recipes.db')
FORCE_REFRESH = os.getenv('FORCE_REFRESH', 'false').lower() == 'true'
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

def start_recipe_fetch_worker():
    """Start background thread for recipe fetching"""
    worker_thread = threading.Thread(
        target=recipe_fetch_worker,
        daemon=True,
        name='Recipe-Fetch-Worker'
    )
    worker_thread.start()
    logger.info("✓ Recipe fetch worker started (hourly, aligned to hour)")


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


@app.route('/trending', methods=['GET'])
@app.route('/trending/', methods=['GET'])
@require_whitelisted_ip
def get_trending():
    """
    Get trending recipes with clear timeframe options

    Query parameters:
    - timeframe: today, week, 1h, 24h, 7d, 30d, 180d (default: 24h)
                 or legacy: 1d, 1w, 1m, 6m
    - limit: number of results (default: 10)
    - utc_offset: UTC offset in seconds for calendar calculations (default: 0)
    - user_id: filter to only this user's recipes (fetched + cached on first call)
    """
    timeframe = request.args.get('timeframe', request.args.get('duration', '24h'))
    limit = min(int(request.args.get('limit', '10')), 50)

    # Get UTC offset
    try:
        utc_offset = int(request.args.get('utc_offset', '0'))
    except ValueError:
        return jsonify({
            'error': 'Invalid UTC offset',
            'message': 'UTC offset must be an integer number of seconds'
        }), 400

    # Parse include_unchanged (boolean, default false)
    include_unchanged = request.args.get('include_unchanged', 'false').lower() in ('true', '1', 'yes')

    # Parse categories filter (comma-separated string)
    categories_param = request.args.get('categories', '').strip()
    categories_filter = [c.strip() for c in categories_param.split(',') if c.strip()] if categories_param else None

    # Parse max recipe age filter (0 = no filter)
    try:
        max_age_days = int(request.args.get('max_age_days', '0'))
    except ValueError:
        max_age_days = 0

    filter_user_age = request.args.get('filter_user_age', 'false').lower() == 'true'

    try:
        # Resolve user_id to recipe_ids if requested
        user_id = request.args.get('user_id')

        if user_id:
            user_recipe_ids = db.get_recipe_ids_for_user(user_id)
            logger.info(f"user_id={user_id} user_recipe_ids={user_recipe_ids}")

            trending_data = trending_calculator.calculate_trending(
                timeframe=timeframe,
                limit=limit,
                utc_offset_seconds=utc_offset,
                user_recipe_ids=user_recipe_ids,
                include_unchanged=include_unchanged,
                categories_filter=categories_filter,
                max_age_days=max_age_days,
                filter_user_age=filter_user_age,
                user_id=user_id,
            )

        else:
            trending_data = trending_calculator.calculate_trending(
                timeframe=timeframe,
                limit=limit,
                utc_offset_seconds=utc_offset,
                categories_filter=categories_filter,
                max_age_days=max_age_days,
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
            'message': str(e),
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
        start_snapshot_cleanup_worker()

    logger.info("✓ Application initialized successfully")


def recipe_fetch_worker():
    """Fetch recipes every hour, with immediate execution on startup"""

    # First, always fetch immediately on startup (catch up any missed hours)
    if FORCE_REFRESH or db.should_fetch_all_recipes(max_hours=1):
        logger.info("🚀 Starting immediate fetch on startup...")
        try:
            start_time = time.time()
            recipes_processed = asyncio.run(recipe_fetcher.fetch_all_recipes())
            duration = time.time() - start_time
            logger.info(f"✓ Startup fetch complete: {recipes_processed} recipes in {duration:.1f}s")
            if recipes_processed > 0:
                db.refresh_materialized_views()
        except Exception as e:
            logger.error(f"✗ Startup fetch failed: {e}")

    # Now run hourly
    while True:
        try:
            # Calculate time until next hour
            now = datetime.now()
            minutes_to_next_hour = 60 - now.minute
            seconds_to_next_hour = (minutes_to_next_hour * 60) - now.second

            # Wait until next hour
            next_hour = (now.hour + 1) % 24
            logger.info(f"⏰ Next fetch at {next_hour:02d}:00 ({minutes_to_next_hour} minutes)")
            time.sleep(seconds_to_next_hour)

            # Fetch at exactly the hour
            logger.info(f"🔄 Starting fetch at {datetime.now().hour:02d}:00...")
            start_time = time.time()
            recipes_processed = asyncio.run(recipe_fetcher.fetch_all_recipes())
            duration = time.time() - start_time
            logger.info(f"✓ Hourly fetch complete: {recipes_processed} recipes in {duration:.1f}s")
            if recipes_processed > 0:
                db.refresh_materialized_views()

        except Exception as e:
            logger.error(f"✗ Recipe fetch worker error: {e}")
            # Wait 5 minutes before retrying on error
            time.sleep(300)

def snapshot_cleanup_worker():
    """Clean up old snapshots daily at midnight"""

    while True:
        try:
            # Calculate time until next midnight
            now = datetime.now()
            seconds_until_midnight = ((24 - now.hour - 1) * 3600) + \
                                     ((60 - now.minute - 1) * 60) + \
                                     (60 - now.second)

            # Wait until midnight
            hours_until = seconds_until_midnight / 3600
            logger.info(f"⏰ Next cleanup at 00:00 ({hours_until:.1f} hours)")
            time.sleep(seconds_until_midnight)

            # Run cleanup at midnight
            logger.info("🗑️  Running midnight cleanup...")
            start_time = time.time()
            hourly_deleted = db.cleanup_hourly_snapshots(hours_to_keep=24 * 30)
            duration = time.time() - start_time

            if hourly_deleted > 0:
                logger.info(f"✓ Cleanup complete: {hourly_deleted} hourly snapshots in {duration:.1f}s")
            else:
                logger.info(f"✓ Cleanup complete: nothing to clean in {duration:.1f}s")

        except Exception as e:
            logger.error(f"✗ Snapshot cleanup error: {e}")
            # Wait 1 hour before retrying on error
            time.sleep(3600)

def start_snapshot_cleanup_worker():
    """Start background thread for snapshot cleanup"""
    worker_thread = threading.Thread(
        target=snapshot_cleanup_worker,
        daemon=True,
        name='Snapshot-Cleanup-Worker'
    )
    worker_thread.start()
    logger.info("✓ Snapshot cleanup worker started (runs daily at 00:00)")


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