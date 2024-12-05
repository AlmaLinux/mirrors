# coding=utf-8
import json
from typing import Union

from flask_caching import Cache

from common.sentry import (
    get_logger,
)

logger = get_logger(__name__)

CACHE_EXPIRED_TIME = 60 * 60  # 1 hour
FLAPPED_EXPIRED_TIME = 60 * 60  # 1 hour
MIRRORS_LIST_EXPIRED_TIME = 60 * 60  # 1 hour
URL_TYPES_LIST_EXPIRED_TIME = 60 * 60 * 24  # 24 hours
CLOUDS_SUBNETS_EXPIRED_TIME = 60 * 60 * 24  # 24 hours


async def get_subnets_from_cache(
    key: str,
    cache: Cache,
) -> dict:
    """
    Get a cached subnets of Azure/AWS cloud
    """
    subnets_string = cache.get(key=key)
    if subnets_string is not None:
        subnets_json = json.loads(
            subnets_string,
        )
        return subnets_json


async def set_subnets_to_cache(
    key: str,
    cache: Cache,
    subnets: dict,
) -> None:
    """
    Save a mirror list for specified IP to cache
    """
    cache.set(
        key=key,
        value=json.dumps(subnets),
        timeout=24 * 60 * 60,
    )


def get_geolocation_from_cache(
    key: str,
    cache: Cache,
) -> Union[tuple[float, float], tuple[None, None]]:
    """
    Get coordinates of a triple of country/state/city from cache
    """
    coords = cache.get(key=key)
    if coords is not None:
        coords = json.loads(coords)
        return coords['latitude'], coords['longitude']
    else:
        return None, None


def set_geolocation_to_cache(
    key: str,
    cache: Cache,
    latitude: float,
    longitude: float,
) -> None:
    """
    Save coordinates of a triple of country/state/city to cache
    """
    cache.set(
        key=key,
        value=json.dumps({
            'latitude': latitude,
            'longitude': longitude,
        }),
        timeout=CACHE_EXPIRED_TIME,
    )


def _generate_redis_key_for_the_mirrors_list(
    get_working_mirrors: bool = False,
    get_expired_mirrors: bool = False,
    get_without_cloud_mirrors: bool = False,
    get_without_private_mirrors: bool = False,
    get_mirrors_with_full_set_of_isos: bool = False,
) -> str:
    """
    Generate key of a redis value by passed options
    :param get_working_mirrors: select mirrors which have status 'ok'
    :param get_expired_mirrors: select mirrors which have status 'expired'
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
        redis_key_suffixes.append('actual')
    if get_expired_mirrors:
        redis_key_suffixes.append('expired')
    if get_without_cloud_mirrors:
        redis_key_suffixes.append('no_cloud')
    if get_without_private_mirrors:
        redis_key_suffixes.append('no_private')
    if get_mirrors_with_full_set_of_isos:
        redis_key_suffixes.append('iso')
    if redis_key_suffixes:
        redis_key_suffix = ','.join(sorted(redis_key_suffixes))
    else:
        redis_key_suffix = 'full'
    logger.info('Redis key suffix "%s"', redis_key_suffix)
    return redis_key + redis_key_suffix
