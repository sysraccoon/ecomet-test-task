import os
import asyncio
import logging
import ssl
import certifi

from typing import Final, Any, Optional, AsyncIterator, Iterator, NamedTuple, Self
from collections import Counter
from datetime import datetime, timedelta, timezone
from pprint import pprint
from aiohttp import ClientSession, TCPConnector
from pydantic.dataclasses import dataclass
from aiolimiter import AsyncLimiter
from aiochclient import ChClient
from tenacity import retry, stop_after_attempt, wait_exponential

GITHUB_API_BASE_URL: Final[str] = "https://api.github.com"


@dataclass
class RepositoryAuthorCommitsNum:
    author: str
    commits_num: int


@dataclass
class Repository:
    name: str
    owner: str
    position: int
    stars: int
    watchers: int
    forks: int
    language: str
    authors_commits_num_today: list[RepositoryAuthorCommitsNum]


class GithubReposScrapper:
    def __init__(
        self,
        access_token: str,
        *,
        max_request_per_second: float,
        max_concurrent_requests: int,
    ):
        self._rate_limiter = AsyncLimiter(
            max_rate=max_request_per_second, time_period=1
        )
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        self._session = ClientSession(
            connector=TCPConnector(
                limit=max_concurrent_requests,
                ssl=ssl_context,
            ),
            headers={
                "Accept": "application/vnd.github.v3+json",
                "Authorization": f"Bearer {access_token}",
            },
        )

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=30))
    async def _make_request(
        self,
        endpoint: str,
        method: str = "GET",
        params: Optional[dict[str, Any]] = None,
    ) -> Any:
        async with self._rate_limiter:
            target_url = f"{GITHUB_API_BASE_URL}/{endpoint}"
            logging.debug(f"{method}: {target_url}, {repr(params)}")

            async with self._session.request(
                method, target_url, params=params
            ) as response:
                return await response.json()

    async def _get_top_repositories(self, limit: int = 100) -> list[dict[str, Any]]:
        """
        GitHub REST API:
        https://docs.github.com/en/rest/search/search?apiVersion=2022-11-28#search-repositories
        """
        data = await self._make_request(
            endpoint="search/repositories",
            params={
                "q": "stars:>1",
                "sort": "stars",
                "order": "desc",
                "per_page": limit,
            },
        )
        return data["items"]

    async def _get_repository_commits(
        self, owner: str, repo: str, *, since: Optional[datetime] = None
    ) -> list[dict[str, Any]]:
        """
        GitHub REST API:
        https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28#list-commits
        """
        PER_PAGE = 30

        params = {}
        if since:
            params["since"] = since.isoformat()

        page = 1
        result_commits = []

        while True:
            page_commits = await self._make_request(
                endpoint=f"repos/{owner}/{repo}/commits",
                params={
                    "per_page": PER_PAGE,
                    "page": page,
                    **params,
                },
            )
            result_commits.extend(page_commits)

            if len(page_commits) < PER_PAGE:
                break

            page += 1

        return result_commits

    async def _handle_raw_repository(
        self, position: int, raw_repo: dict[str, Any]
    ) -> Repository:
        now = datetime.now(timezone.utc)
        yesterday = now - timedelta(days=1)

        owner = raw_repo["owner"]["login"]
        name = raw_repo["name"]

        raw_commits = await self._get_repository_commits(owner, name, since=yesterday)
        commit_authors = (
            commit["author"]["login"] for commit in raw_commits if commit.get("author")
        )
        authors_commits_num = [
            RepositoryAuthorCommitsNum(author=k, commits_num=v)
            for k, v in Counter(commit_authors).items()
        ]

        return Repository(
            name=name,
            owner=owner,
            position=position,
            stars=raw_repo["stargazers_count"],
            watchers=raw_repo["watchers_count"],
            forks=raw_repo["forks_count"],
            language=raw_repo.get("language") or "Undefined",
            authors_commits_num_today=authors_commits_num,
        )

    async def get_repositories(self) -> list[Repository]:
        raw_repositories = await self._get_top_repositories()
        tasks = (
            self._handle_raw_repository(pos, repo)
            for pos, repo in enumerate(raw_repositories)
        )
        return await asyncio.gather(*tasks)

    async def get_repositories_iter(self) -> AsyncIterator[Repository]:
        raw_repositories = await self._get_top_repositories()
        tasks = [
            asyncio.create_task(self._handle_raw_repository(pos, repo))
            for pos, repo in enumerate(raw_repositories)
        ]
        for repo in asyncio.as_completed(tasks):
            yield await repo

    async def close(self):
        await self._session.close()
