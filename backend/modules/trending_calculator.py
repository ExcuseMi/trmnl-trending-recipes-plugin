"""
Trending calculator for recipes
Calculates trending recipes based on installs+forks deltas over different periods
"""

import logging
from typing import Dict, List

logger = logging.getLogger(__name__)


class TrendingCalculator:
    """Calculate trending recipes based on popularity growth"""

    # Duration mapping: duration code -> days ago
    DURATIONS = {
        '1d': 1,
        '1w': 7,
        '1m': 30,
        '6m': 180
    }

    def __init__(self, database):
        self.database = database

    def calculate_trending(self, duration: str, limit: int = 10) -> List[Dict]:
        """
        Calculate trending recipes for a given duration

        Args:
            duration: One of '1d', '1w', '1m', '6m'
            limit: Maximum number of results to return

        Returns:
            List of recipes sorted by trending score (highest first)
        """
        if duration not in self.DURATIONS:
            raise ValueError(f"Invalid duration: {duration}. Must be one of: {list(self.DURATIONS.keys())}")

        days_ago = self.DURATIONS[duration]

        logger.info(f"📈 Calculating trending recipes for {duration} ({days_ago} days)")

        # Get all recipes with their historical data
        recipes = self.database.get_all_recipes_with_delta(days_ago)

        # Calculate trending scores
        trending_recipes = []
        for recipe in recipes:
            trending_score = self._calculate_trending_score(recipe, days_ago)

            if trending_score is not None:
                trending_recipes.append({
                    'id': recipe['id'],
                    'name': recipe['name'],
                    'description': recipe['description'],
                    'url': recipe['url'],
                    'thumbnail_url': recipe['thumbnail_url'],
                    'current_stats': {
                        'installs': recipe['current_installs'],
                        'forks': recipe['current_forks'],
                        'popularity': recipe['current_popularity']
                    },
                    'past_stats': {
                        'installs': recipe.get('past_installs'),
                        'forks': recipe.get('past_forks'),
                        'popularity': recipe.get('past_popularity'),
                        'snapshot_date': recipe.get('past_snapshot_date')
                    },
                    'delta': {
                        'installs': recipe['current_installs'] - (recipe.get('past_installs') or 0),
                        'forks': recipe['current_forks'] - (recipe.get('past_forks') or 0),
                        'popularity': recipe['current_popularity'] - (recipe.get('past_popularity') or 0)
                    },
                    'trending_score': trending_score,
                    'duration': duration
                })

        # Sort by trending score (descending)
        trending_recipes.sort(key=lambda x: x['trending_score'], reverse=True)

        # Apply limit
        result = trending_recipes[:limit]

        logger.info(f"  ✓ Found {len(result)} trending recipes (from {len(recipes)} total)")

        return result

    def _calculate_trending_score(self, recipe: Dict, days_ago: int) -> float:
        """
        Calculate trending score for a recipe

        Trending score = (current_popularity - past_popularity) / days
        This gives us the average daily growth rate

        For new recipes without history, we use current popularity / days_since_creation
        """
        current_popularity = recipe['current_popularity']
        past_popularity = recipe.get('past_popularity')

        # If we have historical data, calculate delta
        if past_popularity is not None:
            delta = current_popularity - past_popularity

            # Avoid division by zero and negative scores
            if days_ago > 0:
                return max(0, delta / days_ago)
            else:
                return 0

        # For new recipes without history, penalize them slightly
        # to avoid favoring brand new recipes with few installs
        if current_popularity > 0:
            # Use half the score to avoid favoring too heavily
            return (current_popularity / (days_ago * 2)) if days_ago > 0 else 0

        return 0

    def get_trending_all_durations(self, limit: int = 10) -> Dict[str, List[Dict]]:
        """Get trending recipes for all duration periods"""
        results = {}

        for duration in self.DURATIONS.keys():
            try:
                results[duration] = self.calculate_trending(duration, limit)
            except Exception as e:
                logger.error(f"✗ Error calculating trending for {duration}: {e}")
                results[duration] = []

        return results

    def get_recipe_momentum(self, recipe_id: str) -> Dict:
        """
        Get momentum metrics for a specific recipe across all durations

        Returns:
            Dict with trending scores for each duration
        """
        momentum = {}

        for duration, days_ago in self.DURATIONS.items():
            try:
                # Get recipe with delta
                recipes = self.database.get_all_recipes_with_delta(days_ago)
                recipe = next((r for r in recipes if r['id'] == recipe_id), None)

                if recipe:
                    score = self._calculate_trending_score(recipe, days_ago)
                    delta_popularity = recipe['current_popularity'] - (recipe.get('past_popularity') or 0)

                    momentum[duration] = {
                        'trending_score': score,
                        'delta_popularity': delta_popularity,
                        'days_ago': days_ago
                    }
                else:
                    momentum[duration] = None

            except Exception as e:
                logger.error(f"✗ Error calculating momentum for {duration}: {e}")
                momentum[duration] = None

        return momentum