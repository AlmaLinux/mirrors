# coding=utf-8
import json

from typing import (
    AnyStr,
    Optional,
    List,
    Tuple,
    Dict
)

from db.db_engine import RedisEngine
from db.models import (
    MirrorData,
    DataClassesJSONEncoder,
)
from common.sentry import (
    get_logger,
)
from datetime import datetime

logger = get_logger(__name__)

CACHE_EXPIRED_TIME = 24 * 3600  # 24 hours


async def get_mirrors_from_cache(
        key: AnyStr,
) -> Optional[List[MirrorData]]:
    """
    Get a cached list of mirrors for specified IP
    """
    key = str(key)
    redis_engine = await RedisEngine.get_instance()
    mirrors_string = await redis_engine.get(key)
    if mirrors_string is not None:
        mirrors_json = json.loads(
            mirrors_string,
        )
        return [MirrorData.load_from_json(mirror_json)
                for mirror_json in mirrors_json]


async def set_mirrors_to_cache(
        key: AnyStr,
        mirrors: List[MirrorData],
) -> None:
    """
    Save a mirror list for specified IP to cache
    """
    key = str(key)
    redis_engine = await RedisEngine.get_instance()
    mirrors = json.dumps(mirrors, cls=DataClassesJSONEncoder)
    await redis_engine.set(
        key,
        mirrors,
        CACHE_EXPIRED_TIME,
    )


async def get_geolocation_from_cache(key: AnyStr) -> Optional[Dict]:
    key = str(key)
    redis_engine = await RedisEngine.get_instance()
    coords = await redis_engine.get(key)
    if coords:
        return json.loads(coords)


async def set_geolocation_to_cache(key: AnyStr, coords: Tuple) -> None:
    key = str(key)
    redis_engine = await RedisEngine.get_instance()
    await redis_engine.set(
        key,
        json.dumps(coords)
    )


async def get_url_types_from_cache() -> List[AnyStr]:
    redis_engine = await RedisEngine.get_instance()
    url_types_string = await redis_engine.get('url_types')
    if url_types_string is not None:
        return json.loads(url_types_string)


async def set_url_types_to_cache(url_types: List[AnyStr]):
    redis_engine = await RedisEngine.get_instance()
    await redis_engine.set('url_types', json.dumps(url_types), CACHE_EXPIRED_TIME)


async def log_mirror_offline(mirror_name: AnyStr):
    redis_engine = await RedisEngine.get_instance()
    await redis_engine.set(
        'mirror_offline_%s' % mirror_name,
        int(datetime.utcnow().timestamp()),
        43200
    )

async def get_mirror_flapped(mirror_name: AnyStr) -> bool:
    redis_engine = await RedisEngine.get_instance()
    return await redis_engine.get('mirror_offline_%s' % mirror_name)
