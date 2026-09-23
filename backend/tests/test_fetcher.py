"""
Tests for recipe fetching: retries, incomplete fetches, daily snapshots.
"""
import asyncio
import pytest
import httpx

from modules.database import Database
from modules.recipe_fetcher import RecipeFetcher, RecipeFetchError


@pytest.fixture
def db():
    d = Database(":memory:")
    d.initialize()
    return d


@pytest.fixture
def fetcher(db):
    f = RecipeFetcher(db)
    f.retry_delay = 0
    return f


def api_recipe(id, installs=10):
    return {'id': id, 'name': f'Recipe {id}', 'published_at': '2026-01-01T00:00:00Z',
            'stats': {'installs': installs, 'forks': 0}}


def fake_pages(pages):
    """fetch_page stand-in: pages is a list of page payloads or exceptions"""
    async def fetch_page(client, page):
        result = pages[page - 1]
        if isinstance(result, Exception):
            raise result
        return result
    return fetch_page


def is_active(db, id):
    row = db.get_connection().execute("SELECT is_active FROM recipes WHERE id = ?", (id,)).fetchone()
    return row['is_active']


class TestFetchPageRetry:
    def test_retries_until_success(self, fetcher):
        calls = []

        def handler(request):
            calls.append(request)
            if len(calls) < 3:
                raise httpx.ReadTimeout("timed out")
            return httpx.Response(200, json={'data': []})

        async def run():
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                return await fetcher.fetch_page(client, 1)

        assert asyncio.run(run()) == {'data': []}
        assert len(calls) == 3

    def test_raises_after_max_attempts(self, fetcher):
        calls = []

        def handler(request):
            calls.append(request)
            return httpx.Response(503)

        async def run():
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                return await fetcher.fetch_page(client, 1)

        with pytest.raises(httpx.HTTPStatusError):
            asyncio.run(run())
        assert len(calls) == fetcher.max_attempts


class TestIncompleteFetch:
    def test_failed_first_page_raises(self, fetcher):
        fetcher.fetch_page = fake_pages([httpx.ReadTimeout("")])
        with pytest.raises(RecipeFetchError):
            asyncio.run(fetcher.fetch_all_recipes())

    def test_partial_fetch_does_not_mark_unseen_inactive(self, db, fetcher, monkeypatch):
        monkeypatch.setattr(asyncio, 'sleep', _no_sleep)
        fetcher.fetch_page = fake_pages([
            {'data': [api_recipe('a'), api_recipe('b')], 'next_page_url': 'p2'},
            {'data': [api_recipe('c')]},
        ])
        asyncio.run(fetcher.fetch_all_recipes())

        # Second poll fails on page 2, so 'c' is never seen
        fetcher.fetch_page = fake_pages([
            {'data': [api_recipe('a'), api_recipe('b')], 'next_page_url': 'p2'},
            httpx.ReadTimeout(""),
        ])
        with pytest.raises(RecipeFetchError):
            asyncio.run(fetcher.fetch_all_recipes())

        assert is_active(db, 'c') == 1

    def test_complete_fetch_marks_unseen_inactive(self, db, fetcher):
        fetcher.fetch_page = fake_pages([{'data': [api_recipe('a'), api_recipe('b')]}])
        asyncio.run(fetcher.fetch_all_recipes())
        fetcher.fetch_page = fake_pages([{'data': [api_recipe('a')]}])
        asyncio.run(fetcher.fetch_all_recipes())

        assert is_active(db, 'b') == 0


class TestDailySnapshot:
    def test_first_fetch_of_day_writes_history(self, db, fetcher):
        fetcher.fetch_page = fake_pages([{'data': [api_recipe('a', installs=10)]}])
        asyncio.run(fetcher.fetch_all_recipes())
        fetcher.fetch_page = fake_pages([{'data': [api_recipe('a', installs=20)]}])
        asyncio.run(fetcher.fetch_all_recipes())

        rows = db.get_connection().execute(
            "SELECT installs FROM recipe_history WHERE recipe_id = 'a'"
        ).fetchall()
        assert [r['installs'] for r in rows] == [10]


class TestMigration:
    def test_user_recipes_table_dropped(self, db):
        tables = {r[0] for r in db.get_connection().execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )}
        assert 'user_recipes' not in tables


async def _no_sleep(_):
    pass
