import os
import sys
from typing import Callable, Any, Union
import asyncio
import math

import httpx
from asyncio_throttle import Throttler

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
API_COUNT = 500
LISTING_API_REQ_PER_SEC = 6
DETAILS_API_REQ_PER_SEC = 12
DETAILS_LIMIT = 2500

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def _get_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url="https://api.caresoft.vn/VUANEM/api/v1/",
        headers={
            "Authorization": f"Bearer {os.getenv('ACCESS_TOKEN')}",
            "Content-Type": "application/json",
        },
        timeout=None,
    )


Row = dict
Data = list[Row]
ResFn = Callable[[dict[str, Any]], Any]


def get_dimension(uri: str, res_fn: ResFn):
    def _get(*args):
        async def __get() -> Data:
            async with _get_client() as client:
                r = await client.get(uri)
                res = r.json()
                return res_fn(res)

        return asyncio.run(__get())

    return _get


async def _get_one_listing(
    client: httpx.AsyncClient,
    throttler: Throttler,
    params: dict[str, Any],
    uri: str,
    res_fn: ResFn,
    page: int = 1,
) -> Union[Data, int]:
    async with throttler:
        r = await client.get(uri, params={**params, "page": page})
        if r.status_code == 429:
            await asyncio.sleep(0.5)
            return await _get_one_listing(client, throttler, params, uri, res_fn, page)
        elif r.status_code == 500:
            return []
        else:
            r.raise_for_status()
            res = r.json()
            return res_fn(res)


def get_listing(uri: str, res_fn: ResFn):
    def _get(params: dict[str, Any]):
        async def __get() -> Data:
            _params = params | {"count": API_COUNT}

            throttler = Throttler(rate_limit=LISTING_API_REQ_PER_SEC, period=1)

            async with _get_client() as client:
                num_found: int = await _get_one_listing(  # type: ignore
                    client,
                    throttler,
                    _params,
                    uri,
                    lambda x: x["numFound"],
                )
                tasks = [
                    asyncio.create_task(
                        _get_one_listing(
                            client,
                            throttler,
                            _params,
                            uri,
                            res_fn,
                            page,
                        )
                    )
                    for page in range(1, int(math.ceil(num_found / API_COUNT)) + 1)
                ]
                pages = await asyncio.gather(*tasks)
                return [i for j in pages for i in j]

        return asyncio.run(__get())

    return _get


async def _get_one_id(
    client: httpx.AsyncClient,
    throttler: Throttler,
    uri: str,
    id: int,
    res_fn: ResFn,
) -> Row:
    async with throttler:
        r = await client.get(f"{uri}/{id}")
        if r.status_code in (404, 500):
            return {}
        elif r.status_code == 429:
            await asyncio.sleep(0.5)
            return await _get_one_id(client, throttler, uri, id, res_fn)
        else:
            res = r.json()
            return res_fn(res)


def get_details(uri: str, res_fn: ResFn):
    def _get(ids: list[int]) -> Data:
        async def __get():
            throttler = Throttler(rate_limit=DETAILS_API_REQ_PER_SEC, period=1)
            async with _get_client() as client:
                tasks = [
                    asyncio.create_task(_get_one_id(client, throttler, uri, id, res_fn))
                    for id in ids
                ]
                pages = await asyncio.gather(*tasks)
                return [i for i in pages if i]

        return asyncio.run(__get())

    return _get
