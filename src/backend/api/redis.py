# coding=utf-8
import json
from contextlib import asynccontextmanager

from typing import (
    Optional,
    Union
)

from db.data_models import DataClassesJSONEncoder
from db.db_engine import RedisEngine
from db.models import (
    MirrorData,
)
from common.sentry import (
    get_logger,
)
from datetime import datetime

logger = get_logger(__name__)

CACHE_EXPIRED_TIME = 3600  # 1 hour


@asynccontextmanager
async def redis_context():
    redis_engine = RedisEngine.get_instance()
    try:
        yield redis_engine
    finally:
        await redis_engine.close()
        await redis_engine.connection_pool.disconnect()


async def get_mirrors_from_cache(
        key: str,
) -> Optional[list[MirrorData]]:
    """
    Get a cached list of mirrors for specified IP
    """
    async with redis_context() as redis_engine:
        mirrors_string = await redis_engine.get(str(key))
    if mirrors_string is not None:
        mirrors_json = json.loads(
            mirrors_string,
        )
        return [MirrorData.load_from_json(mirror_json)
                for mirror_json in mirrors_json]


async def set_mirrors_to_cache(
        key: str,
        mirrors: list[MirrorData],
) -> None:
    """
    Save a mirror list for specified IP to cache
    """
    async with redis_context() as redis_engine:
        mirrors = json.dumps(mirrors, cls=DataClassesJSONEncoder)
        await redis_engine.set(
            str(key),
            mirrors,
            CACHE_EXPIRED_TIME,
        )


async def get_geolocation_from_cache(
        key: str
) -> Union[tuple[float, float], tuple[None, None]]:
    """
    Get coordinates of a triple of country/state/city from cache
    """
    async with redis_context() as redis_engine:
        coords = await redis_engine.get(str(key))
    if coords:
        coords = json.loads(coords)
        return coords['latitude'], coords['longitude']
    else:
        return None, None


async def set_geolocation_to_cache(
        key: str,
        coords: dict[str, float]
) -> None:
    """
    Save coordinates of a triple of country/state/city to cache
    """
    async with redis_context() as redis_engine:
        await redis_engine.set(
            str(key),
            json.dumps(coords)
        )


async def get_url_types_from_cache() -> list[str]:
    """
    Get existing url types from cache
    """
    async with redis_context() as redis_engine:
        url_types_string = await redis_engine.get('url_types')
    if url_types_string is not None:
        return json.loads(url_types_string)


async def set_url_types_to_cache(url_types: list[str]):
    """
    Save existing url types to cache
    """
    async with redis_context() as redis_engine:
        await redis_engine.set(
            'url_types',
            json.dumps(url_types),
            CACHE_EXPIRED_TIME,
        )


async def set_mirror_flapped(mirror_name: str):
    """
    Save time of unavailability of a mirror to cache
    """
    async with redis_context() as redis_engine:
        await redis_engine.set(
            f'mirror_offline_{mirror_name}',
            int(datetime.utcnow().timestamp()),
            43200,  # 12 hours
        )


async def get_mirror_flapped(mirror_name: str) -> bool:
    """
    Get time of unavailability of a mirror from cache
    """
    async with redis_context() as redis_engine:
        return await redis_engine.get(f'mirror_offline_{mirror_name}')


async def set_mirror_list(
        mirrors: list[MirrorData],
        are_ok_and_not_from_clouds: bool = False,
) -> None:
    """
    Save a list of mirrors to cache
    :param are_ok_and_not_from_clouds: Save a list of not expired and not cloud
           mirrors if the param is True, else - save all mirrors
    :param mirrors: list of cached mirrors
    """
    async with redis_context() as redis_engine:
        mirrors = json.dumps(mirrors, cls=DataClassesJSONEncoder)
        redis_key = 'mirror_list_are_ok_and_not_from_clouds' \
            if are_ok_and_not_from_clouds else 'mirror_list'
        await redis_engine.set(redis_key, mirrors, 5400)


async def get_mirror_list(
        are_ok_and_not_from_clouds: bool = False,
) -> Optional[list[MirrorData]]:
    """
    Get a list of mirrors from cache
    :param are_ok_and_not_from_clouds: Get a list of not expired and not cloud
           mirrors if the param is True, else - get all mirrors
    """
    async with redis_context() as redis_engine:
        redis_key = 'mirror_list_are_ok_and_not_from_clouds' \
            if are_ok_and_not_from_clouds else 'mirror_list'
        mirror_list = await redis_engine.get(redis_key)
    if mirror_list is not None:
        return [
            MirrorData.load_from_json(json.loads(mirror))
            for mirror in json.loads(mirror_list)
        ]
