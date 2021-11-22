import os
from typing import Callable, Union
from datetime import datetime
import asyncio

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
API_REQ_PER_SEC = 15


def get_dimensions(model) -> list[dict]:
    with requests.get(
        f"{BASE_URL}/{model['endpoint']}",
        headers=HEADERS,
    ) as r:
        return r.json()[model["row_key"]]


def get_incremental(model, start: datetime, end: datetime) -> list[dict]:
    def get(session: requests.Session, page: int = 1):
        with session.get(
            f"{BASE_URL}/{model['endpoint']}",
            params={
                **model["params_builder"](start, end),
                "page": page,
            },
            headers=HEADERS,
        ) as r:
            res = r.json()
        data = res[model["row_key"]]
        return data + get(session, page + 1) if data else data

    with requests.Session() as session:
        return get(session)


async def get_one_details(
    session: aiohttp.ClientSession,
    throttler,
    model,
    id,
):
    async with throttler:
        async with session.get(
            f"{BASE_URL}/{model['endpoint']}/{id}",
            headers=HEADERS,
        ) as r:
            if r.status in (404, 500):
                return None
            elif r.status == 429:
                await asyncio.sleep(0.5)
                return get_one_details(session, throttler, model, id)
            else:
                res = await r.json()
                return res[model["row_key"]]


async def get_many_details(model, ids: list[int]) -> list[dict]:
    throttler = Throttler(rate_limit=API_REQ_PER_SEC, period=1)
    timeout = aiohttp.ClientTimeout(total=540)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [
            asyncio.create_task(get_one_details(session, throttler, model, id))
            for id in ids
        ]
        rows = await asyncio.gather(*tasks)
        return [i for i in rows if i]
