"""
Recipe fetcher for TRMNL API
Handles paged API calls and data persistence
"""

import asyncio
import logging
from typing import Dict
from datetime import datetime

import httpx

logger = logging.getLogger(__name__)


class RecipeFetchError(Exception):
    """Raised when a fetch could not retrieve every page"""


class RecipeFetcher:
    """Fetches recipes from TRMNL API"""

    def __init__(self, database):
        self.database = database
        self.base_url = "https://trmnl.com/recipes.json"
        self.timeout = 30.0
        self.max_attempts = 3
        self.retry_delay = 10  # seconds, doubled after each failed attempt

    async def fetch_page(self, client: httpx.AsyncClient, page: int) -> Dict:
        """Fetch a single page of recipes, retrying on failure"""
        delay = self.retry_delay
        for attempt in range(1, self.max_attempts + 1):
            try:
                response = await client.get(
                    self.base_url,
                    params={'page': page, 'per_page': 100},
                    timeout=self.timeout
                )
                response.raise_for_status()
                return response.json()
            except Exception as e:
                # httpx timeouts have an empty str(), so include the type
                logger.error(f"✗ Error fetching page {page} (attempt {attempt}/{self.max_attempts}): {type(e).__name__}: {e}")
                if attempt == self.max_attempts:
                    raise
                await asyncio.sleep(delay)
                delay *= 2

    def parse_recipe(self, recipe_data: Dict) -> Dict:
        """Parse and normalize recipe data from API"""
        # Extract stats (installs and forks are in a 'stats' object)
        stats = recipe_data.get('stats', {})
        installs = int(stats.get('installs', 0))
        forks = int(stats.get('forks', 0))

        # Extract description and categories from author_bio
        description = ''
        categories = ''
        author_bio = recipe_data.get('author_bio', {})
        if isinstance(author_bio, dict):
            description = author_bio.get('description', '')
            categories = author_bio.get('category', '')

        # user_id is now included directly in recipes.json
        recipe_user_id = recipe_data.get('user_id') or recipe_data.get('author_id')

        result = {
            'id': str(recipe_data.get('id', '')),
            'name': recipe_data.get('name', 'Untitled'),
            'description': description,
            'categories': categories,
            'installs': installs,
            'forks': forks,
            'url': f"https://trmnl.com/recipes/{recipe_data.get('id', '')}",
            'thumbnail_url': recipe_data.get('screenshot_url', ''),
            'icon_url': recipe_data.get('icon_url', ''),
            'created_at': recipe_data.get('published_at'),
            'updated_at': recipe_data.get('published_at'),
        }

        if recipe_user_id:
            result['user_id'] = str(recipe_user_id)

        return result

    async def fetch_all_recipes(self) -> int:
        """
        Fetch all recipes from TRMNL API (all pages)
        Returns: number of recipes processed
        Raises: RecipeFetchError if any page could not be fetched
        """
        start_time = datetime.now()
        recipes_processed = 0
        page = 1
        total_recipes = None
        seen_ids = []
        failed_page = None

        logger.info("📥 Starting recipe fetch from TRMNL API...")

        async with httpx.AsyncClient() as client:
            while True:
                try:
                    logger.info(f"  → Fetching page {page}...")
                    data = await self.fetch_page(client, page)

                    # Log total on first page
                    if page == 1 and 'total' in data:
                        total_recipes = data['total']
                        logger.info(f"  📊 Total recipes available: {total_recipes}")

                    # Recipes are in the 'data' field
                    recipes = data.get('data', [])
                    if not recipes:
                        logger.info(f"  ✓ No more recipes on page {page}, stopping")
                        break

                    # Process recipes
                    for recipe_data in recipes:
                        try:
                            recipe = self.parse_recipe(recipe_data)

                            # Update current state
                            self.database.upsert_recipe(recipe)
                            seen_ids.append(recipe['id'])

                            # Save hourly snapshot
                            self.database.save_hourly_snapshot(
                                recipe['id'],
                                recipe['installs'],
                                recipe['forks']
                            )

                            # Save daily snapshot (no-op if today's already exists, so the
                            # first successful fetch of the UTC day wins, even if midnight failed)
                            self.database.save_snapshot(
                                recipe['id'],
                                recipe['installs'],
                                recipe['forks']
                            )

                            recipes_processed += 1

                        except Exception as e:
                            logger.error(f"✗ Error processing recipe {recipe_data.get('id', 'unknown')}: {e}")
                            continue

                    # Show progress
                    if total_recipes:
                        progress = (recipes_processed / total_recipes) * 100
                        logger.info(f"  ✓ Page {page}: processed {len(recipes)} recipes ({recipes_processed}/{total_recipes} = {progress:.1f}%)")
                    else:
                        logger.info(f"  ✓ Page {page}: processed {len(recipes)} recipes (total: {recipes_processed})")

                    # Check if there are more pages using next_page_url
                    next_page_url = data.get('next_page_url')
                    if not next_page_url:
                        logger.info(f"  ✓ Reached last page (no next_page_url)")
                        break

                    page += 1

                    # Small delay between pages to be nice to the API
                    await asyncio.sleep(2)

                except Exception as e:
                    logger.error(f"✗ Error on page {page}: {type(e).__name__}: {e}")
                    failed_page = page
                    break

        duration = (datetime.now() - start_time).total_seconds()

        if failed_page is not None:
            # Don't mark unseen recipes inactive: the missing pages would deactivate them wrongly
            raise RecipeFetchError(
                f"fetch incomplete, failed on page {failed_page} "
                f"({recipes_processed} recipes processed in {duration:.1f}s)"
            )

        # Mark any recipe not returned by the API this poll as inactive
        if seen_ids:
            self.database.mark_inactive_recipes(seen_ids)

        logger.info(f"✓ Recipe fetch complete: {recipes_processed} recipes in {duration:.1f}s")

        return recipes_processed


