import os
import asyncpg
import uvicorn
from fastapi import APIRouter, FastAPI, Depends, Request
from typing import Annotated, AsyncGenerator
from contextlib import asynccontextmanager

DB_URL = os.environ["DB_URL"]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    app.state.db_pool = await asyncpg.create_pool(DB_URL)
    yield
    await app.state.db_pool.close()


async def get_pg_connection(request: Request) -> asyncpg.Connection:
    db_pool = request.app.state.db_pool
    async with db_pool.acquire() as connection:
        yield connection


async def get_db_version(
    conn: Annotated[asyncpg.Connection, Depends(get_pg_connection)]
):
    return await conn.fetchval("SELECT version()")


def register_routes(app: FastAPI):
    router = APIRouter(prefix="/api")
    router.add_api_route(path="/db_version", endpoint=get_db_version)
    app.include_router(router)


def create_app() -> FastAPI:
    app = FastAPI(title="e-Comet", lifespan=lifespan)
    register_routes(app)
    return app


if __name__ == "__main__":
    uvicorn.run("main:create_app", factory=True)
