import os
import logging
import asyncio
import certifi
import ssl

from asyncio import CancelledError
from typing import Final, AsyncIterator, TypeVar
from aiohttp import ClientSession
from aiochclient import ChClient

from scraper import GithubReposScrapper, Repository, RepositoryAuthorCommitsNum
from clickhouse import clickhouse_batch_insert

GITHUB_ACCESS_TOKEN: Final[str] = os.environ["GITHUB_ACCESS_TOKEN"]
MAX_REQUEST_PER_SECOND: Final[float] = float(os.getenv("MAX_REQUEST_PER_SECOND", "inf"))
MAX_CONCURRENT_REQUESTS: Final[int] = int(os.getenv("MAX_CONCURRENT_REQUESTS", 0))

CLICKHOUSE_BATCH_SIZE: Final[int] = int(os.getenv("CLICKHOUSE_BATCH_SIZE", 10))
CLICKHOUSE_DATABASE: Final[str] = os.getenv("CLICKHOUSE_DATABASE") or "test"
CLICKHOUSE_URL: Final[str] = os.getenv("CLICKHOUSE_URL") or "http://localhost:8123"
CLICKHOUSE_USER: Final[str] = os.getenv("CLICKHOUSE_USER") or "default"
CLICKHOUSE_PASSWORD: Final[str] = os.getenv("CLICKHOUSE_PASSWORD", "")


async def main():
    logging.basicConfig(level=logging.DEBUG)

    scrapper = GithubReposScrapper(
        GITHUB_ACCESS_TOKEN,
        max_request_per_second=MAX_REQUEST_PER_SECOND,
        max_concurrent_requests=MAX_CONCURRENT_REQUESTS,
    )

    clickhouse_session = ClientSession()
    clickhouse_client = ChClient(
        clickhouse_session,
        database=CLICKHOUSE_DATABASE,
        url=CLICKHOUSE_URL,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )

    try:
        async for repo_batch in batched(
            scrapper.get_repositories_iter(), CLICKHOUSE_BATCH_SIZE
        ):
            await clickhouse_batch_insert(clickhouse_client, repo_batch)
    except asyncio.CancelledError:
        logging.info("Task canceled by user, exiting")
    finally:
        await clickhouse_session.close()
        await scrapper.close()


T = TypeVar("T")


async def batched(
    async_gen: AsyncIterator[T], batch_size: int
) -> AsyncIterator[list[T]]:
    batch = []
    async for item in async_gen:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


if __name__ == "__main__":
    asyncio.run(main())
