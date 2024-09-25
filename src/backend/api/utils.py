# coding=utf-8
import inspect
import os
import asyncio

import time
import random
from collections import defaultdict
from functools import wraps
from typing import (
    Any,
    Optional,
    Union,
)

from aiohttp import (
    ClientSession,
    ClientConnectorError,
)
from aiohttp_retry.types import ClientType
from bs4 import BeautifulSoup
from geoip2.errors import AddressNotFoundError

from db.db_engine import GeoIPEngine, FlaskCacheEngine, ContinentEngine
from yaml_snippets.data_models import MirrorData
from api.exceptions import (
    BaseCustomException,
    AuthException,
)
from flask import (
    Response,
    jsonify,
    make_response,
    request,
)
from flask_api.status import HTTP_200_OK
from werkzeug.exceptions import InternalServerError
from common.sentry import (
    get_logger,
)
from haversine import haversine
from api.redis import (
    get_subnets_from_cache,
    set_subnets_to_cache,
)

logger = get_logger(__name__)
cache = FlaskCacheEngine.get_instance()

RANDOMIZE_WITHIN_KM = 500

AIOHTTP_TIMEOUT = 30


def jsonify_response(
        status: str,
        result: dict[str, Any],
        status_code: int,
) -> Response:
    return make_response(
        jsonify(
            status=status,
            result=result,
            timestamp=int(time.time())
        ),
        status_code,
    )


def textify_response(
        content: str,
        status_code: int,
) -> Response:
    response = make_response(
        content,
        status_code,
    )
    response.mimetype = 'text/plain'
    return response


def success_result(f):
    """
    Decorator: wrap success result
    """

    @wraps(f)
    async def decorated_function(*args, **kwargs):
        # logger.info('success f: %s', inspect.iscoroutinefunction(f))
        if inspect.iscoroutinefunction(f):
            result = await f(*args, **kwargs)
        else:
            result = f(*args, **kwargs)
        if request.method == 'POST':
            return jsonify_response(
                status='success',
                result=result,
                status_code=HTTP_200_OK,
            )
        elif request.method in ('GET', 'HEAD'):
            return textify_response(
                content=result,
                status_code=HTTP_200_OK
            )

    return decorated_function


def error_result(f):
    """
    Decorator: catch unknown exceptions and raise InternalServerError
    """

    @wraps(f)
    async def decorated_function(*args, **kwargs):
        try:
            if inspect.iscoroutinefunction(f):
                return await f(*args, **kwargs)
            else:
                return f(*args, **kwargs)
        except BaseCustomException:
            raise
        except Exception as err:
            raise InternalServerError(
                description=str(err),
                original_exception=err,
            )

    return decorated_function


def get_geo_data_by_ip(
        ip: str
) -> Optional[tuple[str, str, str, str, float, float]]:
    """
    The function returns continent, country and locations of IP in English
    """

    db = GeoIPEngine.get_instance()
    continent = ContinentEngine.get_instance()
    try:
        geoipdb = db.get(ip)
    # ValueError will be raised in case of incorrect IP
    except (AddressNotFoundError, ValueError):
        return
    if not geoipdb:
        return
    try:
        city_name = geoipdb['city']
    except TypeError:
        city_name = None
    try:
        state = geoipdb['region']
    except TypeError:
        state = None
    country = geoipdb['country']
    continent = continent[country]
    latitude = float(geoipdb['lat'])
    longitude = float(geoipdb['lng'])
    if any(item is None for item in (latitude, longitude)):
        return None
    return continent, country, state, city_name, latitude, longitude


async def get_azure_subnets_json(
        http_session: ClientSession,
) -> Optional[dict]:
    url = 'https://www.microsoft.com/en-us/download/confirmation.aspx?id=56519'
    link_attributes = {
        'data-bi-id': 'downloadretry',
    }
    try:
        async with http_session.get(
                url,
                timeout=AIOHTTP_TIMEOUT,
                raise_for_status=True
        ) as resp:
            response_text = await resp.text()
    except (ClientConnectorError, TimeoutError) as err:
        logger.error(
            'Cannot get json with Azure subnets by url "%s" because "%s"',
            url,
            err,
        )
        return
    try:
        soup = BeautifulSoup(response_text, features='lxml')
        link_tag = soup.find('a', attrs=link_attributes)
        link_to_json_url = link_tag.attrs['href']
    except (ValueError, KeyError) as err:
        logger.error(
            'Cannot get json link with Azure '
            'subnets from page content because "%s',
            err,
        )
        return
    try:
        async with http_session.get(
                link_to_json_url,
                timeout=AIOHTTP_TIMEOUT,
                raise_for_status=True
        ) as resp:
            response_json = await resp.json(
                content_type='application/octet-stream',
            )
    except (ClientConnectorError, asyncio.exceptions.TimeoutError) as err:
        logger.error(
            'Cannot get json with Azure subnets by url "%s" because "%s"',
            link_to_json_url,
            err,
        )
        return
    return response_json


async def get_aws_subnets_json(http_session: ClientSession) -> Optional[dict]:
    url = 'https://ip-ranges.amazonaws.com/ip-ranges.json'
    try:
        async with http_session.get(
                url,
                timeout=AIOHTTP_TIMEOUT,
                raise_for_status=True
        ) as resp:
            response_json = await resp.json()
    except (ClientConnectorError, TimeoutError) as err:
        logger.error(
            'Cannot get json with AWS subnets by url "%s" because "%s"',
            url,
            err,
        )
        return
    return response_json


async def get_azure_subnets(http_session: ClientType):
    subnets = await get_subnets_from_cache(
        key='azure_subnets',
        cache=cache,
    )
    if subnets is not None:
        return subnets
    data_json = await get_azure_subnets_json(http_session=http_session)
    subnets = dict()
    if data_json is None:
        return subnets
    values = data_json['values']
    for value in values:
        if value['name'].startswith('AzureCloud.'):
            properties = value['properties']
            subnets[properties['region'].lower()] = \
                properties['addressPrefixes']
    await set_subnets_to_cache(
        key='azure_subnets',
        cache=cache,
        subnets=subnets,
    )
    return subnets


async def get_aws_subnets(http_session: ClientType):
    subnets = await get_subnets_from_cache(
        key='aws_subnets',
        cache=cache,
    )
    if subnets is not None:
        return subnets
    data_json = await get_aws_subnets_json(http_session=http_session)
    subnets = defaultdict(list)
    if data_json is None:
        return subnets
    for v4_prefix in data_json['prefixes']:
        if v4_prefix['ip_prefix'] not in subnets[v4_prefix['region'].lower()]:
            subnets[v4_prefix['region'].lower()].append(v4_prefix['ip_prefix'])
    for v6_prefix in data_json['ipv6_prefixes']:
        if v6_prefix['ipv6_prefix'] not in subnets[v6_prefix['region'].lower()]:
            subnets[v6_prefix['region'].lower()].append(v6_prefix['ipv6_prefix'])
    await set_subnets_to_cache(
        key='aws_subnets',
        cache=cache,
        subnets=subnets,
    )
    return subnets


def set_subnets_for_hyper_cloud_mirror(
        subnets: dict[str, list[str]],
        mirror_info: MirrorData,
):
    cloud_regions = mirror_info.cloud_region.lower().split(',')
    cloud_type = mirror_info.cloud_type.lower()

    if subnets is not None:
        if cloud_type == 'aws' and len(cloud_regions) and \
                cloud_regions[0] in subnets:
            mirror_info.subnets = subnets[cloud_regions[0]]
        elif cloud_type == 'azure':
            total_subnets = []
            for cloud_region in cloud_regions:
                total_subnets.extend(subnets.get(cloud_region, []))
            mirror_info.subnets = total_subnets


def get_distance_in_km(
        mirror_coords: tuple[float, float],
        request_coords: tuple[float, float]
):
    km = int(haversine(mirror_coords, request_coords))
    return km


def sort_mirrors_by_distance_and_country(
        request_geo_data: tuple[float, float],
        mirrors: list[MirrorData],
        country: str,
) -> list[dict[str, Union[int, MirrorData]]]:
    mirrors_sorted = []
    for mirror in mirrors:
        mirrors_sorted.append({
            'distance': get_distance_in_km(
                mirror_coords=(
                    mirror.location.latitude,
                    mirror.location.longitude,
                ),
                request_coords=request_geo_data
            ),
            'mirror': mirror,
        })
    mirrors = sorted(
        mirrors_sorted,
        key=lambda i: (
            i['mirror'].geolocation.country != country,
            i['distance'],
        )
    )
    return mirrors


def randomize_mirrors_within_distance(
        mirrors: list[dict[str, Union[int, MirrorData]]],
        country: str,
        shuffle_distance: int = RANDOMIZE_WITHIN_KM,
):
    mirrors_in_country_shuffled = [
        mirror['mirror'] for mirror in mirrors if
        mirror['distance'] <= shuffle_distance and
        mirror['mirror'].geolocation.country == country
    ]
    mirrors_in_country = [
        mirror['mirror'] for mirror in mirrors if
        mirror['distance'] > shuffle_distance and
        mirror['mirror'].geolocation.country == country
    ]
    other_mirrors_shuffled = [
        mirror['mirror'] for mirror in mirrors if
        mirror['distance'] <= shuffle_distance and
        mirror['mirror'].geolocation.country != country
    ]
    other_mirrors = [
        mirror['mirror'] for mirror in mirrors if
        mirror['distance'] > shuffle_distance and
        mirror['mirror'].geolocation.country != country
    ]
    random.shuffle(mirrors_in_country_shuffled)
    random.shuffle(other_mirrors_shuffled)
    return mirrors_in_country_shuffled + \
        mirrors_in_country + \
        other_mirrors_shuffled + \
        other_mirrors
