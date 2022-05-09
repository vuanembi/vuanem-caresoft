import os
import sys
from typing import Callable, Optional
from datetime import datetime
import asyncio
import math

import requests
import aiohttp
from asyncio_throttle import Throttler

HEADERS = {
    "Authorization": f"Bearer {os.getenv('ACCESS_TOKEN')}",
    "Content-Type": "application/json",
}
BASE_URL = "https://api.caresoft.vn/VUANEM/api/v1"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
API_COUNT = 500
LISTING_API_REQ_PER_SEC = 6
DETAILS_API_REQ_PER_SEC = 12

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def get_simple(endpoint: str, row_key: str) -> list[dict]:
    with requests.get(f"{BASE_URL}/{endpoint}", headers=HEADERS) as r:
        return r.json()[row_key]


async def get_incremental(
    endpoint: str,
    row_key: str,
    params_builder: Callable[[datetime, datetime], dict],
    start: datetime,
    end: datetime,
) -> list[dict]:
    throttler = Throttler(rate_limit=LISTING_API_REQ_PER_SEC, period=1)
    timeout = aiohttp.ClientTimeout(total=540)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        first_page = await get_one_incremental(
            session,
            throttler,
            endpoint,
            row_key,
            params_builder(start, end),
        )
        num_found = first_page["numFound"]
        tasks = [
            asyncio.create_task(
                get_one_incremental(
                    session,
                    throttler,
                    endpoint,
                    row_key,
                    params_builder(start, end),
                    page,
                )
            )
            for page in range(1, int(math.ceil(num_found / API_COUNT)) + 1)
        ]
        rows = await asyncio.gather(*tasks)
        return [i for j in [i[row_key] for i in rows] for i in j]


async def get_one_incremental(
    session: aiohttp.ClientSession,
    throttler: Throttler,
    endpoint: str,
    row_key: str,
    params: dict,
    page: int = 1,
) -> dict:
    async with throttler:
        async with session.get(
            f"{BASE_URL}/{endpoint}",
            params={**params, "page": page},
            headers=HEADERS,
        ) as r:
            if r.status == 429:
                await asyncio.sleep(0.5)
                return await get_one_incremental(
                    session, throttler, endpoint, row_key, params, page
                )
            else:
                r.raise_for_status()
                return await r.json()


async def get_one_details(
    session: aiohttp.ClientSession,
    throttler: Throttler,
    endpoint: str,
    row_key: str,
    id: int,
) -> Optional[dict]:
    async with throttler:
        async with session.get(
            f"{BASE_URL}/{endpoint}/{id}",
            headers=HEADERS,
        ) as r:
            if r.status in (404, 500):
                return None
            elif r.status == 429:
                await asyncio.sleep(0.5)
                return await get_one_details(session, throttler, endpoint, row_key, id)
            else:
                res = await r.json()
                return res[row_key]


async def get_many_details(endpoint: str, row_key: str, ids: list[int]) -> list[dict]:
    throttler = Throttler(rate_limit=DETAILS_API_REQ_PER_SEC, period=1)
    timeout = aiohttp.ClientTimeout(total=540)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [
            asyncio.create_task(
                get_one_details(session, throttler, endpoint, row_key, id)
            )
            for id in ids
        ]
        rows = await asyncio.gather(*tasks)
        return [i for i in rows if i]


def params_builder(
    start_key: str,
    end_key: str,
) -> Callable[[datetime, datetime], dict]:
    def build(start: datetime, end: datetime) -> dict:
        return {
            start_key: start.strftime(TIMESTAMP_FORMAT),
            end_key: end.strftime(TIMESTAMP_FORMAT),
            "count": API_COUNT,
        }

    return build


time_params_builder = params_builder("start_time_since", "start_time_to")
updated_params_builder = params_builder("updated_since", "updated_to")
