"""
Trending calculator for recipes
Calculates trending recipes based on installs+forks deltas over different periods
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class TrendingCalculator:
    """Calculate trending recipes based on popularity growth"""

    # Timeframe mapping
    TIMEFRAMES = {
        # Calendar boundaries (since midnight/Monday)
        'today': {'type': 'calendar', 'hours': None, 'description': 'Since local midnight today'},
        'week': {'type': 'calendar', 'hours': None, 'description': 'Since start of week (Monday)'},

        # New 1h timeframe
        '1h': {'type': 'rolling', 'hours': 1, 'description': 'Last hour'},

        # Rolling windows
        '24h': {'type': 'rolling', 'hours': 24, 'description': 'Last 24 hours'},
        '7d': {'type': 'rolling', 'hours': 168, 'description': 'Last 7 days'},
        '30d': {'type': 'rolling', 'hours': 720, 'description': 'Last 30 days'},
        '180d': {'type': 'rolling', 'hours': 4320, 'description': 'Last 180 days'},

        # Aliases for backward compatibility
        '1d': {'type': 'rolling', 'hours': 24, 'description': 'Last 24 hours (alias for 24h)'},
        '1w': {'type': 'rolling', 'hours': 168, 'description': 'Last 7 days (alias for 7d)'},
        '1m': {'type': 'rolling', 'hours': 720, 'description': 'Last 30 days (alias for 30d)'},
        '6m': {'type': 'rolling', 'hours': 4320, 'description': 'Last 180 days (alias for 180d)'},
    }
    def __init__(self, database):
        self.database = database

    def calculate_trending(self, timeframe: str, limit: int = 10, utc_offset_seconds: int = 0,
                           recipe_ids: Optional[List[str]] = None,
                           include_all: bool = False) -> Dict:
        """
        Calculate trending recipes for a given timeframe

        Args:
            timeframe: One of '1h', 'today', 'week', '24h', '7d', '30d', '180d'
                      or legacy '1d', '1w', '1m', '6m'
            limit: Maximum number of results to return
            utc_offset_seconds: UTC offset in seconds for calendar calculations
            recipe_ids: Optional list of recipe IDs to restrict results to
            include_all: If True, include all recipes (not just trending), sorted by popularity

        Returns:
            Dict with timeframe info and trending recipes
        """
        if timeframe not in self.TIMEFRAMES:
            raise ValueError(f"Invalid timeframe: {timeframe}. Must be one of: {list(self.TIMEFRAMES.keys())}")

        timeframe_info = self.TIMEFRAMES[timeframe]

        if timeframe_info['type'] == 'calendar':
            trending_recipes = self._calculate_calendar_trending(
                timeframe, limit, utc_offset_seconds, recipe_ids=recipe_ids,
                include_all=include_all
            )
        else:
            trending_recipes = self._calculate_rolling_trending(
                timeframe, timeframe_info['hours'], limit, utc_offset_seconds,
                recipe_ids=recipe_ids, include_all=include_all
            )

        # Get timeframe cutoff
        timeframe_info = self.TIMEFRAMES[timeframe]
        if timeframe_info['type'] == 'calendar':
            if timeframe == 'today':
                cutoff = self._get_local_midnight(utc_offset_seconds)
            else:
                cutoff = self._get_week_start(utc_offset_seconds)
        else:
            cutoff = datetime.utcnow() - timedelta(hours=timeframe_info['hours'])

        global_ranks = self.database.compute_global_ranks(timeframe, cutoff)
        for recipe in trending_recipes:
            ranks = global_ranks.get(recipe['id'])
            recipe['global_rank'] = ranks['global_rank']
            recipe['rank_difference'] = ranks['rank_difference']

        # Build comprehensive response
        response = {
            'timeframe': timeframe,
            'type': timeframe_info['type'],
            'description': timeframe_info['description'],
            'utc_offset_seconds': utc_offset_seconds,
            'count': len(trending_recipes),
            'recipes': trending_recipes,
            'calculation_info': self._get_calculation_info(timeframe, utc_offset_seconds)
        }

        return response

    def _calculate_calendar_trending(self, timeframe: str, limit: int,
                                     utc_offset_seconds: int,
                                     recipe_ids: Optional[List[str]] = None,
                                     include_all: bool = False) -> List[Dict]:
        """Calculate trending based on calendar boundaries"""
        now_utc = datetime.utcnow()

        if timeframe == 'today':
            # Calculate local midnight
            local_midnight = self._get_local_midnight(utc_offset_seconds)
            cutoff = local_midnight

            logger.info(f"📅 Calculating 'today' trending (since {local_midnight.isoformat()} UTC)")
        elif timeframe == 'week':
            # Calculate start of week (Monday) in local time
            cutoff = self._get_week_start(utc_offset_seconds)

            logger.info(f"📅 Calculating 'week' trending (since {cutoff.isoformat()} UTC)")

        else:
            raise ValueError(f"Unknown calendar timeframe: {timeframe}")

        # Get recipes with delta since cutoff
        recipes = self.database.get_recipes_with_delta_since(cutoff, recipe_ids=recipe_ids)

        return self._process_trending_recipes(recipes, timeframe, limit, utc_offset_seconds,
                                              cutoff_iso=cutoff.isoformat(),
                                              include_all=include_all)

    def _calculate_rolling_trending(self, timeframe: str, hours: int, limit: int,
                                    utc_offset_seconds: int,
                                    recipe_ids: Optional[List[str]] = None,
                                    include_all: bool = False) -> List[Dict]:
        """Calculate trending based on rolling time window"""
        logger.info(f"⏰ Calculating '{timeframe}' trending ({hours}h rolling window)" +
                    (f" filtered to {len(recipe_ids)} recipes" if recipe_ids else ""))

        # Get recipes with delta since cutoff using hourly snapshots
        recipes = self.database.get_recipes_with_delta_since_hours(hours, recipe_ids=recipe_ids)

        # Log a one-line summary with the actual snapshot age range
        snapshot_ages = []
        now = datetime.utcnow()
        for r in recipes:
            ts = r.get('past_snapshot_timestamp')
            if ts:
                try:
                    dt = datetime.fromisoformat(ts.replace('Z', ''))
                    snapshot_ages.append((now - dt).total_seconds() / 3600)
                except Exception:
                    pass
        if snapshot_ages:
            logger.debug(
                f"Rolling {timeframe} ({hours}h): {len(recipes)} recipes, "
                f"{len(snapshot_ages)}/{len(recipes)} with snapshots, "
                f"snapshot age range: {min(snapshot_ages):.1f}h - {max(snapshot_ages):.1f}h"
            )
        else:
            logger.debug(f"Rolling {timeframe} ({hours}h): {len(recipes)} recipes, no snapshot data")

        return self._process_trending_recipes(recipes, timeframe, limit, utc_offset_seconds,
                                              include_all=include_all)

    def _process_trending_recipes(self, recipes: List[Dict], timeframe: str, limit: int,
                                  utc_offset_seconds: int = 0, cutoff_iso: str = None,
                                  include_all: bool = False) -> List[Dict]:
        """Process recipes into trending format"""
        trending_recipes = []

        for recipe in recipes:
            # Get full recipe details including published_at
            recipe_details = self.database.get_recipe_current(recipe['id'])

            if not recipe_details:
                continue

            published_at_str = recipe_details.get('created_at')  # This is actually published_at
            recipe_age_days = self._calculate_recipe_age_days(published_at_str) if published_at_str else 0

            trending_score = self._calculate_trending_score_for_period(
                {
                    'id': recipe['id'],
                    'popularity': recipe['current_popularity'],
                    'popularity_delta': recipe['current_popularity'] - recipe.get('past_popularity', 0),
                    'has_historical_data': recipe.get('has_history', False),
                    'recipe_age_days': recipe_age_days,  # Pass calculated age
                    'published_at': published_at_str  # Pass raw string too
                },
                timeframe,
                utc_offset_seconds,
                cutoff_iso
            )

            if not include_all and trending_score <= 0:
                continue

            # Get delta for the requested timeframe
            if timeframe in ['today', 'week']:
                main_delta = self._get_delta_for_timeframe(
                    recipe['id'], timeframe, utc_offset_seconds
                )
            else:
                timeframe_info = self.TIMEFRAMES[timeframe]
                main_delta = self._get_delta_for_rolling(
                    recipe['id'], timeframe_info.get('hours', 24)
                )

            trending_recipes.append({
                'id': recipe['id'],
                'name': recipe['name'],
                'description': recipe['description'],
                'url': recipe['url'],
                'icon_url': recipe['icon_url'],
                'thumbnail_url': recipe['thumbnail_url'],
                'popularity': recipe['current_popularity'],
                'popularity_delta': recipe['current_popularity'] - recipe.get('past_popularity', 0),
                'trending_score': trending_score,
                'timeframe': timeframe,
                'has_historical_data': recipe.get('has_history', False),
                'popularity_growth_pct': ((recipe['current_popularity'] - recipe.get('past_popularity', 0)) /
                                          recipe['current_popularity'] * 100) if recipe[
                                                                                     'current_popularity'] > 0 else 0,
                'published_at': published_at_str,
                'recipe_age_days': recipe_age_days
            })

        # Sort by popularity when showing all, otherwise by trending score
        if include_all:
            trending_recipes.sort(key=lambda x: (x['popularity_delta'], x['popularity']), reverse=True)
        else:
            trending_recipes.sort(key=lambda x: x['trending_score'], reverse=True)

        # Apply limit
        return trending_recipes[:limit]

    def _calculate_recipe_age_days(self, published_at_str: str) -> float:
        """Calculate how many days old a recipe is"""
        if not published_at_str:
            return 0.0

        try:
            # Try multiple date formats with timezone awareness
            published_at = None

            # Format 1: ISO with microseconds and Z timezone
            try:
                # Parse as UTC (Z means UTC)
                dt_str = published_at_str.replace('Z', '+00:00')
                published_at = datetime.fromisoformat(dt_str)
            except ValueError:
                pass

            # Format 2: ISO without microseconds
            if not published_at:
                try:
                    # Use datetime.strptime and add timezone
                    dt_naive = datetime.strptime(published_at_str, '%Y-%m-%dT%H:%M:%SZ')
                    published_at = dt_naive.replace(tzinfo=timezone.utc)
                except ValueError:
                    pass

            # Format 3: ISO with milliseconds
            if not published_at:
                try:
                    dt_naive = datetime.strptime(published_at_str, '%Y-%m-%dT%H:%M:%S.%fZ')
                    published_at = dt_naive.replace(tzinfo=timezone.utc)
                except ValueError:
                    pass

            if not published_at:
                logger.warning(f"Could not parse date: {published_at_str}")
                return 0.0

            # Get current time in UTC with timezone awareness
            now_utc = datetime.now(timezone.utc)

            # Calculate difference
            age_seconds = (now_utc - published_at).total_seconds()
            return age_seconds / 86400  # Convert to days

        except Exception as e:
            logger.error(f"Error calculating age for {published_at_str}: {e}")
            return 0.0

    def _get_local_midnight(self, utc_offset_seconds: int) -> datetime:
        """Get the most recent local midnight in UTC"""
        now_utc = datetime.utcnow()
        local_now = now_utc + timedelta(seconds=utc_offset_seconds)
        local_midnight = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        return local_midnight - timedelta(seconds=utc_offset_seconds)

    def _get_week_start(self, utc_offset_seconds: int) -> datetime:
        """Get start of week (Monday) in local time, converted to UTC"""
        now_utc = datetime.utcnow()
        local_now = now_utc + timedelta(seconds=utc_offset_seconds)

        # Monday is 0, Sunday is 6
        days_since_monday = local_now.weekday()

        # Go back to Monday at 00:00 local time
        local_monday = local_now - timedelta(days=days_since_monday)
        local_monday_midnight = local_monday.replace(hour=0, minute=0, second=0, microsecond=0)

        return local_monday_midnight - timedelta(seconds=utc_offset_seconds)

    def _calculate_trending_score_for_period(self, recipe: Dict, timeframe: str,
                                             utc_offset_seconds: int = 0,
                                             cutoff_iso: str = None) -> float:
        """Calculate trending score normalized to per-day growth rate"""
        try:
            has_history = recipe.get('has_historical_data', False)
            popularity_delta = recipe.get('popularity_delta', 0) or 0

            if not has_history or popularity_delta <= 0:
                return 0.0

            # For calendar timeframes, use actual elapsed time
            if timeframe in ('today', 'week') and cutoff_iso:
                try:
                    hours_passed = (datetime.utcnow() - datetime.fromisoformat(
                        cutoff_iso.replace('Z', ''))).total_seconds() / 3600
                    days = max(0.1, hours_passed / 24)
                except Exception:
                    days = 1.0
                return float(popularity_delta) / days

            # For rolling windows, normalize by the window size
            timeframe_info = self.TIMEFRAMES.get(timeframe, {'hours': 24})
            hours = timeframe_info.get('hours', 24)
            days = max(0.1, hours / 24)
            return float(popularity_delta) / days

        except Exception as e:
            logger.error(f"Error calculating trending score: {e}")
            return 0.0
    def _get_delta_for_timeframe(self, recipe_id: str, timeframe: str, utc_offset_seconds: int = 0) -> Dict:
        """Get delta for a specific timeframe"""
        try:
            now_utc = datetime.utcnow()

            if timeframe == 'today':
                cutoff = self._get_local_midnight(utc_offset_seconds)
            elif timeframe == 'week':
                cutoff = self._get_week_start(utc_offset_seconds)
            else:
                cutoff = now_utc - timedelta(hours=24)  # Default to 24h

            delta_data = self.database.get_recipe_delta_since(recipe_id, cutoff)

            if delta_data:
                return {
                    'installs': delta_data.get('delta_installs', 0),
                    'forks': delta_data.get('delta_forks', 0),
                    'popularity': delta_data.get('delta_popularity', 0),
                    'timeframe': timeframe,
                    'has_data': delta_data.get('has_data', False),
                    'past_timestamp': delta_data.get('past_snapshot_timestamp'),
                    'period_start': cutoff.isoformat(),
                    'current_popularity': delta_data.get('current_popularity', 0),
                    'past_popularity': delta_data.get('past_popularity', 0)
                }
            else:
                return self._get_empty_delta(timeframe, cutoff)

        except Exception as e:
            logger.error(f"Error getting delta for {timeframe}: {e}")
            return self._get_empty_delta(timeframe, datetime.utcnow() - timedelta(hours=24))

    def _get_delta_for_rolling(self, recipe_id: str, hours: int) -> Dict:
        """Get delta for a rolling timeframe using hourly snapshots"""
        try:
            cutoff = datetime.utcnow() - timedelta(hours=hours)

            delta_data = self.database.get_recipe_delta_since_hours(recipe_id, hours)

            if delta_data:
                return {
                    'installs': delta_data.get('delta_installs', 0),
                    'forks': delta_data.get('delta_forks', 0),
                    'popularity': delta_data.get('delta_popularity', 0),
                    'hours': hours,
                    'has_data': delta_data.get('has_data', False),
                    'past_timestamp': delta_data.get('past_snapshot_timestamp'),
                    'period_start': cutoff.isoformat(),
                    'current_popularity': delta_data.get('current_popularity', 0),
                    'past_popularity': delta_data.get('past_popularity', 0)
                }
            else:
                return self._get_empty_delta(f"{hours}h", cutoff)

        except Exception as e:
            logger.error(f"Error getting delta for {hours}h: {e}")
            return self._get_empty_delta(f"{hours}h", datetime.utcnow() - timedelta(hours=hours))

    def _get_empty_delta(self, timeframe: str, cutoff: datetime) -> Dict:
        """Return empty delta structure"""
        if timeframe.endswith('h'):
            hours = int(timeframe[:-1]) if timeframe[:-1].isdigit() else 24
            return {
                'installs': 0,
                'forks': 0,
                'popularity': 0,
                'hours': hours,
                'has_data': False,
                'period_start': cutoff.isoformat()
            }
        else:
            return {
                'installs': 0,
                'forks': 0,
                'popularity': 0,
                'timeframe': timeframe,
                'has_data': False,
                'period_start': cutoff.isoformat()
            }
    def _get_calculation_info(self, timeframe: str, utc_offset_seconds: int) -> Dict:
        """Get information about how the trending was calculated"""
        info = {
            'utc_offset_applied': utc_offset_seconds,
            'current_utc_time': datetime.utcnow().isoformat()
        }

        timeframe_info = self.TIMEFRAMES[timeframe]

        if timeframe_info['type'] == 'calendar':
            if timeframe == 'today':
                midnight = self._get_local_midnight(utc_offset_seconds)
                info.update({
                    'period_start': midnight.isoformat(),
                    'period_type': 'calendar_day',
                    'local_midnight': midnight.isoformat()
                })
            elif timeframe == 'week':
                week_start = self._get_week_start(utc_offset_seconds)
                info.update({
                    'period_start': week_start.isoformat(),
                    'period_type': 'calendar_week',
                    'week_start': week_start.isoformat()
                })
        else:
            hours = timeframe_info.get('hours', 24)
            cutoff = datetime.utcnow() - timedelta(hours=hours)
            info.update({
                'period_start': cutoff.isoformat(),
                'period_type': 'rolling_window',
                'window_hours': hours
            })

        return info

