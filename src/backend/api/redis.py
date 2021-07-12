# coding=utf-8
from typing import AnyStr, Optional, List, Union, Dict

from db.db_engine import RedisEngine


def _get_value(key: AnyStr) -> Optional[AnyStr]:
    """
    Get a Redis value by a key
    """
    redis_engine = RedisEngine.get_instance()

    return redis_engine.get(key)


def _set_value(
        key: AnyStr,
        value: AnyStr,
        exp_time: int,
):
    """
    Set a Redis value by a key and with expired time
    """
    redis_engine = RedisEngine.get_instance()
    redis_engine.set(
        name=key,
        value=value,
        ex=exp_time,
    )


def get_mirrors(key: AnyStr) -> Optional[List[AnyStr]]:
    """
    Get a Redis ordered range by a key
    """
    redis_engine = RedisEngine.get_instance()
    redis_key = _get_value(key)
    if redis_key is not None:
        return redis_engine.zrange(redis_key, 0, -1)
    else:
        redis_engine.zremrangebyscore(key, 0, '+inf')


def set_range(key: AnyStr, range: List[AnyStr], exp_time: int) -> None:
    """
    Set a Redis ordered range by a key and with expired time
    """
    redis_engine = RedisEngine.get_instance()
    _set_value(
        key=key,
        value=key,
        exp_time=exp_time,
    )
    redis_engine.zremrangebyscore(key, 0, '+inf')
    redis_engine.zadd(
        key,
        {value: index for index, value in enumerate(range)},
    )
