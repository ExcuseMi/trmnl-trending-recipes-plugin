"""
Recipe fetcher for TRMNL API
Handles paged API calls and data persistence
"""

import asyncio
import logging
from typing import Dict, List, Optional  # ADD THIS IMPORT
from datetime import datetime

import httpx

logger = logging.getLogger(__name__)


class RecipeFetcher:
    """Fetches recipes from TRMNL API"""

    def __init__(self, database):
        self.database = database
        self.base_url = "https://trmnl.com/recipes.json"
        self.timeout = 30.0

    async def fetch_page(self, client: httpx.AsyncClient, page: int) -> Dict:
        """Fetch a single page of recipes"""
        try:
            response = await client.get(
                self.base_url,
                params={'page': page},
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"✗ Error fetching page {page}: {e}")
            raise

    def parse_recipe(self, recipe_data: Dict, user_id: Optional[str] = None) -> Dict:
        """Parse and normalize recipe data from API"""
        # Extract stats (installs and forks are in a 'stats' object)
        stats = recipe_data.get('stats', {})
        installs = int(stats.get('installs', 0))
        forks = int(stats.get('forks', 0))

        # Extract description from author_bio
        description = ''
        author_bio = recipe_data.get('author_bio', {})
        if isinstance(author_bio, dict):
            description = author_bio.get('description', '')

        # Try to get user_id from various sources
        recipe_user_id = user_id or recipe_data.get('user_id') or recipe_data.get('author_id')

        result = {
            'id': str(recipe_data.get('id', '')),
            'name': recipe_data.get('name', 'Untitled'),
            'description': description,
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
        """
        start_time = datetime.now()
        recipes_processed = 0
        page = 1
        total_recipes = None

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

                            # Save hourly snapshot
                            self.database.save_hourly_snapshot(
                                recipe['id'],
                                recipe['installs'],
                                recipe['forks']
                            )

                            # Save daily snapshot at midnight (if it's the first snapshot of the day)
                            now_utc = datetime.utcnow()
                            if now_utc.hour == 0 and now_utc.minute < 10:  # Only in first 10 minutes of midnight
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
                    logger.error(f"✗ Error on page {page}: {e}")
                    break

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"✓ Recipe fetch complete: {recipes_processed} recipes in {duration:.1f}s")

        return recipes_processed

    async def fetch_recipe_by_id(self, recipe_id: str) -> Dict:
        """Fetch a specific recipe by ID"""
        # Note: This assumes the API supports filtering by ID
        # Adjust based on actual API capabilities
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    f"{self.base_url}/{recipe_id}",
                    timeout=self.timeout
                )
                response.raise_for_status()
                recipe_data = response.json()
                return self.parse_recipe(recipe_data)
            except Exception as e:
                logger.error(f"✗ Error fetching recipe {recipe_id}: {e}")
                raise

