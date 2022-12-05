# coding=utf-8
import json
from contextlib import asynccontextmanager

from typing import (
    Optional,
    Union
)

from yaml_snippets.data_models import (
    DataClassesJSONEncoder,
    MirrorData,
)
from db.db_engine import RedisEngine
from common.sentry import (
    get_logger,
)
from datetime import datetime

logger = get_logger(__name__)

CACHE_EXPIRED_TIME = 3600 * 1  # 1 hour
FLAPPED_EXPIRED_TIME = 3600 * 3  # 3 hours
MIRRORS_LIST_EXPIRED_TIME = 3600 * 2  # 2 hours


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
        get_mirrors_with_full_set_of_isos: bool = False,
) -> Optional[list[MirrorData]]:
    """
    Get a cached list of mirrors for specified IP
    """
    if get_mirrors_with_full_set_of_isos:
        key = f'{key}_iso'
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
        get_mirrors_with_full_set_of_isos: bool = False,
) -> None:
    """
    Save a mirror list for specified IP to cache
    """
    if get_mirrors_with_full_set_of_isos:
        key = f'{key}_iso'
    async with redis_context() as redis_engine:
        mirrors = json.dumps(mirrors, cls=DataClassesJSONEncoder)
        await redis_engine.set(
            str(key),
            mirrors,
            CACHE_EXPIRED_TIME,
        )


async def get_subnets_from_cache(
        key: str,
) -> dict:
    """
    Get a cached subnets of Azure/AWS cloud
    """
    async with redis_context() as redis_engine:
        subnets_string = await redis_engine.get(str(key))
    if subnets_string is not None:
        subnets_json = json.loads(
            subnets_string,
        )
        return subnets_json


async def set_subnets_to_cache(
        key: str,
        subnets: dict,
) -> None:
    """
    Save a mirror list for specified IP to cache
    """
    async with redis_context() as redis_engine:
        subnets = json.dumps(subnets)
        await redis_engine.set(
            str(key),
            subnets,
            24 * 60 * 60,
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
            FLAPPED_EXPIRED_TIME,
        )


async def get_mirror_flapped(mirror_name: str) -> bool:
    """
    Get time of unavailability of a mirror from cache
    """
    async with redis_context() as redis_engine:
        return await redis_engine.get(f'mirror_offline_{mirror_name}')


async def set_mirror_list(
        mirrors: list[MirrorData],
        get_working_mirrors: bool = False,
        get_without_cloud_mirrors: bool = False,
        get_without_private_mirrors: bool = False,
        get_mirrors_with_full_set_of_isos: bool = False
) -> None:
    """
    Save a mirrors list to Redis cache
    :param mirrors: list of cached mirrors
    :param get_working_mirrors: select mirrors which are not expired
    :param get_without_cloud_mirrors: select mirrors without those who are
           hosted in clouds (Azure/AWS)
    :param get_without_private_mirrors: select mirrors without those who are
           hosted behind NAT
    :param get_mirrors_with_full_set_of_isos: select mirrors which have full
           set of ISOs and them artifacts (CHECKSUM, manifests)
           per each version and architecture
    """
    redis_key = _generate_redis_key_for_the_mirrors_list(
        get_working_mirrors=get_working_mirrors,
        get_without_cloud_mirrors=get_without_cloud_mirrors,
        get_without_private_mirrors=get_without_private_mirrors,
        get_mirrors_with_full_set_of_isos=get_mirrors_with_full_set_of_isos)
    async with redis_context() as redis_engine:
        mirrors = json.dumps(mirrors, cls=DataClassesJSONEncoder)
        await redis_engine.set(redis_key, mirrors, MIRRORS_LIST_EXPIRED_TIME)


def _generate_redis_key_for_the_mirrors_list(
        get_working_mirrors: bool = False,
        get_without_cloud_mirrors: bool = False,
        get_without_private_mirrors: bool = False,
        get_mirrors_with_full_set_of_isos: bool = False,
) -> str:
    """
    Generate key of a redis value by passed options
    :param get_working_mirrors: select mirrors which are not expired
    :param get_without_cloud_mirrors: select mirrors without those who are
           hosted in clouds (Azure/AWS)
    :param get_without_private_mirrors: select mirrors without those who are
           hosted behind NAT
    :param get_mirrors_with_full_set_of_isos: select mirrors which have full
           set of ISOs and them artifacts (CHECKSUM, manifests)
           per each version and architecture
    """
    redis_key = 'mirrors_list_'
    redis_key_suffixes = []
    if get_working_mirrors:
        redis_key_suffixes += 'actual'
    if get_without_cloud_mirrors:
        redis_key_suffixes += 'no_cloud'
    if get_without_private_mirrors:
        redis_key_suffixes += 'no_private'
    if get_mirrors_with_full_set_of_isos:
        redis_key_suffixes += 'iso'
    if redis_key_suffixes:
        redis_key_suffix = ','.join(sorted(redis_key_suffixes))
    else:
        redis_key_suffix = 'full'

    return redis_key + redis_key_suffix


async def get_mirror_list(
        get_working_mirrors: bool = False,
        get_without_cloud_mirrors: bool = False,
        get_without_private_mirrors: bool = False,
        get_mirrors_with_full_set_of_isos: bool = False
) -> Optional[list[MirrorData]]:
    """
    Get a list of mirrors from cache
    :param get_working_mirrors: select mirrors which are not expired
    :param get_without_cloud_mirrors: select mirrors without those who are
           hosted in clouds (Azure/AWS)
    :param get_without_private_mirrors: select mirrors without those who are
           hosted behind NAT
    :param get_mirrors_with_full_set_of_isos: select mirrors which have full
           set of ISOs and them artifacts (CHECKSUM, manifests)
           per each version and architecture
    """
    redis_key = _generate_redis_key_for_the_mirrors_list(
        get_working_mirrors=get_working_mirrors,
        get_without_cloud_mirrors=get_without_cloud_mirrors,
        get_without_private_mirrors=get_without_private_mirrors,
        get_mirrors_with_full_set_of_isos=get_mirrors_with_full_set_of_isos,
    )
    async with redis_context() as redis_engine:
        mirror_list = await redis_engine.get(redis_key)
    if mirror_list is not None:
        return [
            MirrorData.load_from_json(json.loads(mirror))
            for mirror in json.loads(mirror_list)
        ]
