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
        '3d': {'type': 'rolling', 'hours': 72, 'description': 'Last 3 days'},
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
                           include_all: bool = False,
                           user_recipe_ids: Optional[List[str]] = None,
                           include_unchanged: bool = False,
                           categories_filter: Optional[List[str]] = None,
                           max_age_days: int = 0,
                           filter_user_age: bool = False,
                           user_id: Optional[str] = None) -> Dict:
        """
        Calculate trending recipes for a given timeframe

        Args:
            timeframe: One of '1h', 'today', 'week', '24h', '3d', '7d', '30d', '180d'
                      or legacy '1d', '1w', '1m', '6m'
            limit: Maximum number of results to return
            utc_offset_seconds: UTC offset in seconds for calendar calculations
            recipe_ids: Optional list of recipe IDs to restrict results to (single-list mode)
            include_all: If True, include all recipes (not just trending), sorted by popularity
            user_recipe_ids: Optional list of user's recipe IDs (enables dual-list mode)
            include_unchanged: If True, include user recipes with 0 popularity_delta
            categories_filter: Optional list of categories to filter by

        Returns:
            Dict with timeframe info and trending recipes
        """
        if timeframe not in self.TIMEFRAMES:
            raise ValueError(f"Invalid timeframe: {timeframe}. Must be one of: {list(self.TIMEFRAMES.keys())}")

        timeframe_info = self.TIMEFRAMES[timeframe]

        # Get timeframe cutoff
        if timeframe_info['type'] == 'calendar':
            if timeframe == 'today':
                cutoff = self._get_local_midnight(utc_offset_seconds)
            else:
                cutoff = self._get_week_start(utc_offset_seconds)
        else:
            cutoff = datetime.utcnow() - timedelta(hours=timeframe_info['hours'])

        # Dual-list mode: user_recipe_ids provided
        if user_recipe_ids is not None:
            return self._calculate_dual_list(
                timeframe, timeframe_info, limit, utc_offset_seconds, cutoff,
                user_recipe_ids, include_unchanged, categories_filter, max_age_days,
                filter_user_age=filter_user_age, user_id=user_id
            )

        # Single-list mode (original behavior)
        if timeframe_info['type'] == 'calendar':
            trending_recipes = self._calculate_calendar_trending(
                timeframe, None, utc_offset_seconds, recipe_ids=recipe_ids,
                include_all=include_all
            )
        else:
            trending_recipes = self._calculate_rolling_trending(
                timeframe, timeframe_info['hours'], None, utc_offset_seconds,
                recipe_ids=recipe_ids, include_all=include_all
            )

        global_ranks = self.database.compute_global_ranks(timeframe, cutoff)
        for recipe in trending_recipes:
            ranks = global_ranks.get(recipe['id'])
            if ranks:
                recipe['global_rank'] = ranks['global_rank']
                recipe['rank_difference'] = ranks['rank_difference']

        # Apply category filter
        if categories_filter:
            trending_recipes = [
                r for r in trending_recipes
                if any(c in categories_filter for c in r.get('categories', []))
            ]

        # Apply age filter (after ranks so rankings stay global)
        if max_age_days > 0:
            trending_recipes = [
                r for r in trending_recipes
                if r.get('recipe_age_days', 0) <= max_age_days
            ]

        # Apply limit
        trending_recipes = trending_recipes[:limit]

        # Flag top gainer(s)
        if trending_recipes and trending_recipes[0]['popularity_delta'] > 0:
            top_delta = trending_recipes[0]['popularity_delta']
            for r in trending_recipes:
                if r['popularity_delta'] == top_delta:
                    r['is_top_gainer'] = True
                else:
                    break

        # Global stats
        use_hourly = timeframe_info['type'] == 'rolling'
        global_stats = self.database.get_global_stats(cutoff, use_hourly=use_hourly)
        global_stats['total_developers'] = self.database.get_total_developers()

        # Strip internal-only fields
        trending_recipes = [self._strip_recipe(r) for r in trending_recipes]

        return {
            'recipes': trending_recipes,
            'global_stats': global_stats,
        }

    def _calculate_dual_list(self, timeframe: str, timeframe_info: Dict, limit: int,
                             utc_offset_seconds: int, cutoff: datetime,
                             user_recipe_ids: List[str],
                             include_unchanged: bool,
                             categories_filter: Optional[List[str]],
                             max_age_days: int = 0,
                             filter_user_age: bool = False,
                             user_id: Optional[str] = None) -> Dict:
        """Calculate trending with dual-list mode: user_recipes + global recipes"""

        # Fetch ALL recipes (no filter)
        if timeframe_info['type'] == 'calendar':
            all_recipes = self._calculate_calendar_trending(
                timeframe, None, utc_offset_seconds, recipe_ids=None, include_all=True
            )
        else:
            all_recipes = self._calculate_rolling_trending(
                timeframe, timeframe_info['hours'], None, utc_offset_seconds,
                recipe_ids=None, include_all=True
            )

        # Compute global ranks for all
        global_ranks = self.database.compute_global_ranks(timeframe, cutoff)
        for recipe in all_recipes:
            ranks = global_ranks.get(recipe['id'])
            if ranks:
                recipe['global_rank'] = ranks['global_rank']
                recipe['rank_difference'] = ranks['rank_difference']

        # Apply category filter to full set
        if categories_filter:
            all_recipes = [
                r for r in all_recipes
                if any(c in categories_filter for c in r.get('categories', []))
            ]
        # Flag top gainer(s)
        if all_recipes and all_recipes[0]['popularity_delta'] > 0:
            top_delta = all_recipes[0]['popularity_delta']
            for r in all_recipes:
                if r['popularity_delta'] == top_delta:
                    r['is_top_gainer'] = True
                else:
                    break
        # Partition into user_recipes and global_recipes
        user_id_set = set(user_recipe_ids)
        user_recipes_all = [r for r in all_recipes if r['id'] in user_id_set]
        global_recipes = [r for r in all_recipes if r['id'] not in user_id_set]

        # Compute user_stats from ALL user recipes (before filtering)
        total_developers = self.database.get_total_developers()
        user_stats = {
            'total_popularity': sum(r['popularity'] for r in user_recipes_all),
            'popularity_delta': sum(r['popularity_delta'] for r in user_recipes_all),
            'total_recipes': len(user_recipe_ids),
        }

        # Add developer rank if user_id is available
        if user_id:
            dev_rank = self.database.get_user_rank(user_id)
            if dev_rank:
                user_stats['developer_rank'] = dev_rank['global_rank']

        # Filter user_recipes: only trending up unless include_unchanged (show all)
        if not include_unchanged:
            user_recipes_filtered = [r for r in user_recipes_all if r['popularity_delta'] > 0]
        else:
            user_recipes_filtered = list(user_recipes_all)

        # Apply age filter
        if max_age_days > 0:
            global_recipes = [
                r for r in global_recipes
                if r.get('recipe_age_days', 0) <= max_age_days
            ]
            if filter_user_age:
                user_recipes_filtered = [
                    r for r in user_recipes_filtered
                    if r.get('recipe_age_days', 0) <= max_age_days
                ]

        # Sort: user_recipes by popularity_delta DESC, global by trending_score DESC
        user_recipes_filtered.sort(key=lambda x: x['popularity_delta'], reverse=True)
        global_recipes = [r for r in global_recipes if r['trending_score'] > 0]
        global_recipes.sort(key=lambda x: x['trending_score'], reverse=True)

        # Apply limit: user_recipes first, fill remaining with global_recipes
        final_user = user_recipes_filtered[:limit]
        remaining = max(0, limit - len(final_user))
        final_global = global_recipes[:remaining]

        # Global stats
        use_hourly = timeframe_info['type'] == 'rolling'
        global_stats = self.database.get_global_stats(cutoff, use_hourly=use_hourly)
        global_stats['total_developers'] = total_developers

        # Strip internal-only fields
        final_user = [self._strip_recipe(r) for r in final_user]
        final_global = [self._strip_recipe(r) for r in final_global]

        return {
            'user_recipes': final_user,
            'recipes': final_global,
            'user_stats': user_stats,
            'global_stats': global_stats,
        }

    def _calculate_calendar_trending(self, timeframe: str, limit: Optional[int],
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

    def _calculate_rolling_trending(self, timeframe: str, hours: int, limit: Optional[int],
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

    def _process_trending_recipes(self, recipes: List[Dict], timeframe: str, limit: Optional[int],
                                  utc_offset_seconds: int = 0, cutoff_iso: str = None,
                                  include_all: bool = False) -> List[Dict]:
        """Process recipes into trending format"""
        trending_recipes = []

        for recipe in recipes:
            published_at_str = recipe.get('created_at')
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

            # Parse categories into array
            categories_raw = recipe.get('categories') or ''
            categories_list = [c.strip() for c in categories_raw.split(',') if c.strip()]

            trending_recipes.append({
                'id': recipe['id'],
                'name': recipe['name'],
                'categories': categories_list,
                'icon_url': recipe['icon_url'],
                'popularity': recipe['current_popularity'],
                'popularity_delta': recipe['current_popularity'] - recipe.get('past_popularity', 0),
                'trending_score': trending_score,
                'recipe_age_days': int(recipe_age_days)
            })

        # Sort by popularity when showing all, otherwise by trending score
        if include_all:
            trending_recipes.sort(key=lambda x: (x['popularity_delta'], x['popularity']), reverse=True)
        else:
            trending_recipes.sort(key=lambda x: x['trending_score'], reverse=True)

        # Assign trending_rank (dense ranking — tied deltas share the same rank)
        rank = 0
        prev_delta = None
        for r in trending_recipes:
            if r['popularity_delta'] <= 0:
                continue
            if r['popularity_delta'] != prev_delta:
                rank += 1
                prev_delta = r['popularity_delta']
            r['trending_rank'] = rank

        # Apply limit (None means no limit, for dual-list code path)
        if limit is not None:
            return trending_recipes[:limit]
        return trending_recipes

    _DISPLAY_FIELDS = {'name', 'icon_url', 'popularity', 'popularity_delta',
                       'recipe_age_days', 'global_rank', 'rank_difference',
                       'is_top_gainer', 'trending_rank'}

    def _strip_recipe(self, recipe: Dict) -> Dict:
        """Keep only fields used by the liquid templates"""
        return {k: v for k, v in recipe.items() if k in self._DISPLAY_FIELDS}

    def _calculate_recipe_age_days(self, published_at_str: str) -> int:
        """Calculate how many days old a recipe is"""
        if not published_at_str:
            return 0

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
                return 0

            # Get current time in UTC with timezone awareness
            now_utc = datetime.now(timezone.utc)

            # Calculate difference
            age_seconds = (now_utc - published_at).total_seconds()
            return int(age_seconds / 86400)  # Convert to days

        except Exception as e:
            logger.error(f"Error calculating age for {published_at_str}: {e}")
            return 0

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
            popularity_delta = recipe.get('popularity_delta', 0) or 0

            if popularity_delta <= 0:
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

