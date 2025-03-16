import asyncio
import logging

from typing import Optional, NamedTuple, Self
from datetime import datetime
from aiochclient import ChClient

from scraper import GithubReposScrapper, Repository, RepositoryAuthorCommitsNum


class ClickHouseRepository(NamedTuple):
    name: str
    owner: str
    stars: int
    watchers: int
    forks: int
    language: str
    updated: datetime

    @classmethod
    def from_repository(
        cls, repo: Repository, updated: Optional[datetime] = None
    ) -> Self:
        return cls(
            repo.name,
            repo.owner,
            repo.stars,
            repo.watchers,
            repo.forks,
            repo.language,
            updated or datetime.now().replace(microsecond=0),
        )


class ClickHouseRepositoryAuthorCommits(NamedTuple):
    date: datetime
    repo: str
    author: str
    commits_num: int

    @classmethod
    def from_repository_author_commits_num(
        cls,
        repo_author_commits_num: RepositoryAuthorCommitsNum,
        repo: str,
        date: Optional[datetime] = None,
    ) -> Self:
        return cls(
            date or datetime.now().replace(microsecond=0),
            repo,
            repo_author_commits_num.author,
            repo_author_commits_num.commits_num,
        )


class ClickHouseRepositoryPosition(NamedTuple):
    date: datetime
    repo: str
    position: int

    @classmethod
    def from_repository(cls, repo: Repository, date: Optional[datetime] = None) -> Self:
        return cls(
            date or datetime.now().replace(microsecond=0),
            repo.name,
            repo.position,
        )


async def clickhouse_batch_insert(
    clickhouse_client: ChClient, repositories: list[Repository]
):
    logging.info(f"batch insert repositories: {repositories}")
    await asyncio.gather(
        clickhouse_client.execute(
            "INSERT INTO repositories VALUES",
            *(ClickHouseRepository.from_repository(repo) for repo in repositories),
        ),
        clickhouse_client.execute(
            "INSERT INTO repositories_authors_commits VALUES",
            *(
                ClickHouseRepositoryAuthorCommits.from_repository_author_commits_num(
                    author_commits_num, repo.name
                )
                for repo in repositories
                for author_commits_num in repo.authors_commits_num_today
            ),
        ),
        clickhouse_client.execute(
            "INSERT INTO repositories_positions VALUES",
            *(
                ClickHouseRepositoryPosition.from_repository(repo)
                for repo in repositories
            ),
        ),
    )
