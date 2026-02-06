"""
Trending calculator for recipes
Calculates trending recipes based on installs+forks deltas over different periods
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List

logger = logging.getLogger(__name__)


class TrendingCalculator:
    """Calculate trending recipes based on popularity growth"""

    # Timeframe mapping
    TIMEFRAMES = {
        # Calendar boundaries (since midnight/Monday)
        'today': {'type': 'calendar', 'hours': None, 'description': 'Since local midnight today'},
        'week': {'type': 'calendar', 'hours': None, 'description': 'Since start of week (Monday)'},

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

    def calculate_trending(self, timeframe: str, limit: int = 10, utc_offset_seconds: int = 0) -> Dict:
        """
        Calculate trending recipes for a given timeframe

        Args:
            timeframe: One of 'today', 'week', '24h', '7d', '30d', '180d'
                      or legacy '1d', '1w', '1m', '6m'
            limit: Maximum number of results to return
            utc_offset_seconds: UTC offset in seconds for calendar calculations

        Returns:
            Dict with timeframe info and trending recipes
        """
        if timeframe not in self.TIMEFRAMES:
            raise ValueError(f"Invalid timeframe: {timeframe}. Must be one of: {list(self.TIMEFRAMES.keys())}")

        timeframe_info = self.TIMEFRAMES[timeframe]

        if timeframe_info['type'] == 'calendar':
            trending_recipes = self._calculate_calendar_trending(
                timeframe, limit, utc_offset_seconds
            )
        else:
            trending_recipes = self._calculate_rolling_trending(
                timeframe, timeframe_info['hours'], limit, utc_offset_seconds
            )

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

    def _calculate_calendar_trending(self, timeframe: str, limit: int, utc_offset_seconds: int) -> List[Dict]:
        """Calculate trending based on calendar boundaries"""
        now_utc = datetime.utcnow()

        if timeframe == 'today':
            # Calculate local midnight
            local_midnight = self._get_local_midnight(utc_offset_seconds)
            cutoff = local_midnight
            days_ago = None  # Not using days_ago for calendar calculations

            logger.info(f"📅 Calculating 'today' trending (since {local_midnight.isoformat()} UTC)")

        elif timeframe == 'week':
            # Calculate start of week (Monday) in local time
            cutoff = self._get_week_start(utc_offset_seconds)
            days_ago = None

            logger.info(f"📅 Calculating 'week' trending (since {cutoff.isoformat()} UTC)")

        else:
            raise ValueError(f"Unknown calendar timeframe: {timeframe}")

        # Get recipes with delta since cutoff
        recipes = self.database.get_recipes_with_delta_since(cutoff)

        return self._process_trending_recipes(recipes, timeframe, limit, cutoff_iso=cutoff.isoformat())

    def _calculate_rolling_trending(self, timeframe: str, hours: int, limit: int, utc_offset_seconds: int) -> List[Dict]:
        """Calculate trending based on rolling time window"""
        logger.info(f"⏰ Calculating '{timeframe}' trending ({hours}h rolling window)")

        # Calculate cutoff time (rolling window)
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        cutoff_iso = cutoff.isoformat()

        # Get recipes with delta since cutoff
        recipes = self.database.get_recipes_with_delta_since(cutoff)

        return self._process_trending_recipes(recipes, timeframe, limit, cutoff_iso=cutoff_iso)

    def _process_trending_recipes(self, recipes: List[Dict], timeframe: str, limit: int,
                                  utc_offset_seconds: int = 0, cutoff_iso: str = None) -> List[Dict]:
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

            if trending_score > 0:
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
                    'recipe_age_days': recipe_age_days  # Include in response
                })

        # Sort by trending score (descending)
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
        """
        Calculate trending score with improved "today" handling
        """
        try:
            has_history = recipe.get('has_historical_data', False)
            popularity_delta = recipe.get('popularity_delta', 0) or 0

            # For "today" trending with hourly updates:
            if timeframe == 'today':
                # Get the most recent snapshot timestamp
                conn = self.database.get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT MAX(snapshot_timestamp) as latest_snapshot
                    FROM recipe_history 
                    WHERE recipe_id = ?
                """, (recipe['id'],))

                row = cursor.fetchone()
                latest_snapshot = row['latest_snapshot'] if row else None

                if latest_snapshot:
                    # Calculate how many hours since last snapshot
                    try:
                        latest_dt = datetime.fromisoformat(latest_snapshot.replace('Z', ''))
                        hours_since_snapshot = (datetime.utcnow() - latest_dt).total_seconds() / 3600

                        # If snapshot is very recent (< 2 hours), use it
                        if hours_since_snapshot < 2 and popularity_delta > 0:
                            # Normalize to hourly rate
                            return float(popularity_delta) / max(0.1, hours_since_snapshot / 24)
                    except Exception:
                        pass

                # Fallback to normal calculation
                if has_history and popularity_delta > 0:
                    if cutoff_iso:
                        try:
                            hours_passed = (datetime.utcnow() - datetime.fromisoformat(
                                cutoff_iso.replace('Z', ''))).total_seconds() / 3600
                            days = max(0.1, hours_passed / 24)
                        except Exception:
                            days = 1.0
                    else:
                        days = 1.0
                    return float(popularity_delta) / days

            # Normal calculation for other timeframes
            if has_history and popularity_delta > 0:
                if timeframe == 'today':
                    if cutoff_iso:
                        try:
                            hours_passed = (datetime.utcnow() - datetime.fromisoformat(
                                cutoff_iso.replace('Z', ''))).total_seconds() / 3600
                            days = max(0.1, hours_passed / 24)
                        except Exception:
                            days = 1.0
                    else:
                        days = 1.0
                    return float(popularity_delta) / days
                else:
                    timeframe_info = self.TIMEFRAMES.get(timeframe, {'hours': 24})
                    hours = timeframe_info.get('hours', 24)
                    days = max(0.1, hours / 24)
                    return float(popularity_delta) / days

            return 0.0

        except Exception as e:
            logger.error(f"Error: {e}")
            return 0.0
    def _calculate_conservative_score(self, has_history: bool, popularity_delta: int,
                                      timeframe: str, cutoff_iso: str = None) -> float:
        """
        Conservative score calculation when we have limited information
        """
        if popularity_delta <= 0:
            return 0.0

        if has_history:
            # We have historical data
            if timeframe == 'today':
                if cutoff_iso:
                    try:
                        hours_passed = (datetime.utcnow() - datetime.fromisoformat(
                            cutoff_iso.replace('Z', ''))).total_seconds() / 3600
                        days = max(0.1, hours_passed / 24)
                    except Exception:
                        days = 1.0
                else:
                    days = 1.0
                return float(popularity_delta) / days
            else:
                timeframe_info = self.TIMEFRAMES.get(timeframe, {'hours': 24})
                hours = timeframe_info.get('hours', 24)
                days = max(0.1, hours / 24)
                return float(popularity_delta) / days
        else:
            # No historical data - heavily penalize
            if timeframe == 'today':
                # Assume growth happened over at least 12 hours
                return float(popularity_delta) / 0.5
            elif timeframe == 'week':
                # Assume growth happened over at least 3.5 days
                return float(popularity_delta) / 3.5
            else:
                # For rolling windows, assume half the period
                timeframe_info = self.TIMEFRAMES.get(timeframe, {'hours': 24})
                hours = timeframe_info.get('hours', 24)
                days = max(1.0, hours / 48)  # Half the period
                return float(popularity_delta) / days

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
        """Get delta for a rolling timeframe"""
        try:
            cutoff = datetime.utcnow() - timedelta(hours=hours)

            delta_data = self.database.get_recipe_delta_since(recipe_id, cutoff)

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

    def get_all_timeframes_trending(self, limit: int = 10, utc_offset_seconds: int = 0) -> Dict:
        """Get trending recipes for all timeframes"""
        results = {}

        for timeframe in ['today', 'week', '24h', '7d', '30d']:
            try:
                results[timeframe] = self.calculate_trending(timeframe, limit, utc_offset_seconds)
            except Exception as e:
                logger.error(f"✗ Error calculating trending for {timeframe}: {e}")
                results[timeframe] = {
                    'error': str(e),
                    'timeframe': timeframe
                }

        return results

    # Backward compatibility aliases
    def calculate_trending_with_offset(self, duration: str, limit: int = 10, utc_offset_seconds: int = 0) -> Dict:
        """Alias for backward compatibility"""
        # Map old duration names to new ones
        duration_map = {
            '1d': '24h',
            '1w': '7d',
            '1m': '30d',
            '6m': '180d'
        }

        timeframe = duration_map.get(duration, duration)
        return self.calculate_trending(timeframe, limit, utc_offset_seconds)