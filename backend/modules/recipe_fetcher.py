"""
Recipe fetcher for TRMNL API
Handles paged API calls and data persistence
"""

import asyncio
import logging
from typing import Dict, List
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

    def parse_recipe(self, recipe_data: Dict) -> Dict:
        """Parse and normalize recipe data from API"""
        return {
            'id': str(recipe_data.get('id', '')),
            'name': recipe_data.get('name', 'Untitled'),
            'author': recipe_data.get('author', ''),
            'description': recipe_data.get('description', ''),
            'installs': int(recipe_data.get('installs', 0)),
            'forks': int(recipe_data.get('forks', 0)),
            'url': recipe_data.get('url', ''),
            'thumbnail_url': recipe_data.get('thumbnail_url', ''),
            'created_at': recipe_data.get('created_at'),
            'updated_at': recipe_data.get('updated_at'),
        }

    async def fetch_all_recipes(self) -> int:
        """
        Fetch all recipes from TRMNL API (all pages)
        Returns: number of recipes processed
        """
        start_time = datetime.now()
        recipes_processed = 0
        page = 1

        logger.info("📥 Starting recipe fetch from TRMNL API...")

        async with httpx.AsyncClient() as client:
            while True:
                try:
                    logger.info(f"  → Fetching page {page}...")
                    data = await self.fetch_page(client, page)

                    recipes = data.get('recipes', [])
                    if not recipes:
                        logger.info(f"  ✓ No more recipes on page {page}, stopping")
                        break

                    # Process recipes
                    for recipe_data in recipes:
                        try:
                            recipe = self.parse_recipe(recipe_data)

                            # Update current state
                            self.database.upsert_recipe(recipe)

                            # Save daily snapshot
                            self.database.save_snapshot(
                                recipe['id'],
                                recipe['installs'],
                                recipe['forks']
                            )

                            recipes_processed += 1

                        except Exception as e:
                            logger.error(f"✗ Error processing recipe: {e}")
                            continue

                    logger.info(f"  ✓ Page {page}: processed {len(recipes)} recipes")

                    # Check if there are more pages
                    pagination = data.get('pagination', {})
                    total_pages = pagination.get('total_pages', page)

                    if page >= total_pages:
                        logger.info(f"  ✓ Reached last page ({total_pages})")
                        break

                    page += 1

                    # Small delay between pages to be nice to the API
                    await asyncio.sleep(0.5)

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