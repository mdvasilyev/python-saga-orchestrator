from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from tests.integration.models import Base


@pytest.fixture(scope="session")
def database_url() -> str:
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip(
            "TEST_DATABASE_URL is not set; skipping PostgreSQL integration tests"
        )
    if not url.startswith("postgresql+asyncpg://"):
        pytest.skip("TEST_DATABASE_URL must use postgresql+asyncpg driver")
    return url


@pytest_asyncio.fixture()
async def session_maker(
    database_url: str,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    schema_name = f"test_{uuid.uuid4().hex}"
    admin_engine = create_async_engine(database_url, echo=False)
    async with admin_engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

    engine = create_async_engine(
        database_url,
        echo=False,
        connect_args={"server_settings": {"search_path": schema_name}},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield async_sessionmaker(engine, expire_on_commit=False)

    await engine.dispose()
    async with admin_engine.begin() as conn:
        await conn.execute(text(f'DROP SCHEMA "{schema_name}" CASCADE'))
    await admin_engine.dispose()
