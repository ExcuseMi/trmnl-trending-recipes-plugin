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
TRMNL_IPS_API = 'https://usetrmnl.com/api/ips'
LOCALHOST_IPS = ['127.0.0.1', '::1', 'localhost']
DATABASE_PATH = os.getenv('DATABASE_PATH', '/data/recipes.db')
FETCH_INTERVAL_HOURS = int(os.getenv('FETCH_INTERVAL_HOURS', '24'))

# Global IP whitelist state
TRMNL_IPS = set(LOCALHOST_IPS)
TRMNL_IPS_LOCK = threading.Lock()
last_ip_refresh: Optional[datetime] = None

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
    """Background worker that refreshes TRMNL IPs periodically"""
    while True:
        try:
            time.sleep(IP_REFRESH_HOURS * 3600)
            update_trmnl_ips_sync()
        except Exception as e:
            logger.error(f"✗ IP refresh worker error: {e}")
            time.sleep(3600)


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
    logger.info(f"✓ IP refresh worker started (every {IP_REFRESH_HOURS}h)")


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
    """Background worker that fetches recipes periodically"""
    while True:
        try:
            logger.info("🔄 Starting recipe fetch job...")
            asyncio.run(recipe_fetcher.fetch_all_recipes())
            logger.info(f"✓ Recipe fetch complete. Next fetch in {FETCH_INTERVAL_HOURS}h")
            time.sleep(FETCH_INTERVAL_HOURS * 3600)
        except Exception as e:
            logger.error(f"✗ Recipe fetch worker error: {e}")
            time.sleep(3600)  # Retry in 1 hour on error


def start_recipe_fetch_worker():
    """Start background thread for recipe fetching"""
    worker_thread = threading.Thread(
        target=recipe_fetch_worker,
        daemon=True,
        name='Recipe-Fetch-Worker'
    )
    worker_thread.start()
    logger.info(f"✓ Recipe fetch worker started (every {FETCH_INTERVAL_HOURS}h)")


# ============================================================================
# API ROUTES
# ============================================================================

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'ip_whitelist_enabled': ENABLE_IP_WHITELIST,
        'last_ip_refresh': last_ip_refresh.isoformat() if last_ip_refresh else None
    })


@app.route('/api/trending', methods=['GET'])
@require_whitelisted_ip
def get_trending():
    """
    Get trending recipes

    Query parameters:
    - duration: 1d, 1w, 1m, 6m (default: 1w)
    - limit: number of results (default: 10)
    """
    duration = request.args.get('duration', '1w')
    limit = int(request.args.get('limit', '10'))

    # Validate duration
    valid_durations = ['1d', '1w', '1m', '6m']
    if duration not in valid_durations:
        return jsonify({
            'error': 'Invalid duration',
            'message': f'Duration must be one of: {", ".join(valid_durations)}'
        }), 400

    try:
        trending = trending_calculator.calculate_trending(duration, limit)
        return jsonify({
            'duration': duration,
            'count': len(trending),
            'recipes': trending
        })
    except Exception as e:
        logger.error(f"✗ Error calculating trending: {e}")
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500




# ============================================================================
# STARTUP
# ============================================================================

def initialize():
    """Initialize the application"""
    logger.info("🚀 Starting TRMNL Trending Recipes Plugin")

    # Initialize database
    db.initialize()

    # Load initial TRMNL IPs
    if ENABLE_IP_WHITELIST:
        logger.info("🔐 IP whitelist enabled")
        update_trmnl_ips_sync()
        start_ip_refresh_worker()
    else:
        logger.warning("⚠️  IP whitelist DISABLED - all IPs allowed!")

    # Start background workers
    start_recipe_fetch_worker()

    # Do initial fetch if database is empty
    if db.get_recipe_count() == 0:
        logger.info("📥 No recipes in database, performing initial fetch...")
        asyncio.run(recipe_fetcher.fetch_all_recipes())

    logger.info("✓ Application initialized successfully")


if __name__ == '__main__':
    initialize()

    # Run Flask app
    port = int(os.getenv('PORT', '5000'))
    app.run(host='0.0.0.0', port=port, debug=False)