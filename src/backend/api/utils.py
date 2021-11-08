# coding=utf-8
import inspect
import os
import asyncio

import time
import random
from collections import defaultdict
from functools import wraps
from typing import (
    Dict,
    Any,
    AnyStr,
    Tuple,
    Optional,
    List,
)

from aiohttp import ClientSession, client_exceptions
import geopy
from bs4 import BeautifulSoup
from geoip2.errors import AddressNotFoundError

from db.db_engine import GeoIPEngine
from db.data_models import (
    MirrorYamlData,
)
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
from sqlalchemy.orm import Session
from api.redis import (
    get_geolocation_from_cache,
    set_geolocation_to_cache
)

logger = get_logger(__name__)


AUTH_KEY = os.environ.get('AUTH_KEY')

RANDOMIZE_WITHIN_KM = 750

AIOHTTP_TIMEOUT=30


def jsonify_response(
        status: str,
        result: Dict[str, Any],
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
        content: AnyStr,
        status_code: int,
) -> Response:
    response = make_response(
        content,
        status_code,
    )
    response.mimetype = 'text/plain'
    return response


def auth_key_required(f):
    """
    Decorator: Check auth key
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if request.method == 'GET' or \
                AUTH_KEY == request.cookies.get('AUTH_KEY'):
            return f(*args, **kwargs)
        else:
            raise AuthException('Invalid auth key is passed')
    return decorated_function


def success_result(f):
    """
    Decorator: wrap success result
    """

    @wraps(f)
    async def decorated_function(*args, **kwargs):
        result = f(*args, **kwargs)
        if inspect.isawaitable(result):
            result = await result
        if request.method == 'POST':
            return jsonify_response(
                status='success',
                result=result,
                status_code=HTTP_200_OK,
            )
        elif request.method == 'GET':
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
    def decorated_function(*args, **kwargs):
        try:
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
        ip: AnyStr
) -> Optional[Tuple[AnyStr, AnyStr, AnyStr, AnyStr, float, float]]:
    """
    The function returns continent, country and locations of IP in English
    """

    db = GeoIPEngine.get_instance()
    try:
        city = db.city(ip)
    except AddressNotFoundError:
        return
    try:
        city_name = city.city.name
    except AttributeError:
        city_name = None
    try:
        state = city.subdivisions.most_specific.name
    except AttributeError:
        state = None
    country = city.country.iso_code
    continent = city.continent.name
    latitude = city.location.latitude
    longitude = city.location.longitude

    return continent, country, state, city_name, latitude, longitude


async def get_azure_subnets_json(http_session: ClientSession) -> Optional[Dict]:
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
    except client_exceptions.ClientConnectorError as err:
        logger.error(
            'Cannot get json with Azure subnets by url "%s" because "%s": %s',
            url,
            err,
            type(err)
        )
        return
    try:
        soup = BeautifulSoup(response_text, features='lxml')
        link_tag = soup.find('a', attrs=link_attributes)
        link_to_json_url = link_tag.attrs['href']
    except (ValueError, KeyError) as err:
        logger.error(
            'Cannot get json link with Azure '
            'subnets from page content because "%s"',
            err,
        )
        return
    try:
        async with http_session.get(
            link_to_json_url,
            timeout=AIOHTTP_TIMEOUT,
            raise_for_status=True
        ) as resp:
            response_json = await resp.json(content_type='application/octet-stream')
    except Exception as err:
        logger.error(
            'Cannot get json with Azure subnets by url "%s" because "%s"',
            link_to_json_url,
            err,
        )
    return response_json


async def get_aws_subnets_json(http_session: ClientSession) -> Optional[Dict]:
    url = 'https://ip-ranges.amazonaws.com/ip-ranges.json'
    try:
        async with http_session.get(
            url,
            timeout=AIOHTTP_TIMEOUT,
            raise_for_status=True
        ) as resp:
            response_json = await resp.json()
    except (aiohttp.client_exceptions.ClientConnectorError, asyncio.exceptions.TimeoutError) as err:
        logger.error(
            'Cannot get json with AWS subnets by url "%s" because "%s": %s',
            url,
            err,
            type(err)
        )
        return
    return response_json


async def get_azure_subnets(http_session: ClientSession):
    data_json = await get_azure_subnets_json(http_session=http_session)
    if data_json is None:
        return
    values = data_json['values']
    subnets = {}
    for value in values:
        if value['name'].startswith('AzureCloud.'):
            properties = value['properties']
            subnets[properties['region'].lower()] = \
                properties['addressPrefixes']
    return subnets


async def get_aws_subnets(http_session: ClientSession):
    data_json = await get_aws_subnets_json(http_session=http_session)
    subnets = defaultdict(list)
    if data_json is None:
        return
    for v4_prefix in data_json['prefixes']:
        subnets[v4_prefix['region'].lower()].append(v4_prefix['ip_prefix'])
    for v6_prefix in data_json['ipv6_prefixes']:
        subnets[v6_prefix['region'].lower()].append(v6_prefix['ipv6_prefix'])
    return subnets


def set_subnets_for_hyper_cloud_mirror(
        subnets: Dict[AnyStr, List[AnyStr]],
        mirror_info: MirrorYamlData,
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


async def get_coords_by_city(
        city: AnyStr,
        state: Optional[AnyStr],
        country: AnyStr,
        sem: asyncio.Semaphore
) -> Tuple[float, float]:
    geolocation_from_cache = await get_geolocation_from_cache('nominatim_%s_%s_%s' % (country, state, city))
    if geolocation_from_cache:
        return geolocation_from_cache['latitude'], geolocation_from_cache['longitude']

    try:
        async with sem:
            async with geopy.geocoders.Nominatim(
                user_agent="mirrors.almalinux.org",
                domain='nominatim.openstreetmap.org',
                adapter_factory=geopy.adapters.AioHTTPAdapter
            ) as geo:
                result = await geo.geocode(
                    query={
                        'city': city,
                        'state': state,
                        'country': country
                    },
                    exactly_one=True
                )
                if result is None:
                    return 0.0, 0.0
                await set_geolocation_to_cache('nominatim_%s_%s_%s' %
                                               (country, state, city),
                                               {'latitude': result.latitude, 'longitude': result.longitude}
                                               )
            # nominatim api AUP is 1req/s
            await asyncio.sleep(2)
    except geopy.exc.GeocoderServiceError as e:
        logger.error(
            'Error retrieving Nominatim data for "%s".  Exception: "%s"',
            f'{city}, {state}, {country}',
            e
        )
        return 0.0, 0.0
    except Exception as e:
        logger.error(
            'Unknown except occured in geopy/nominatim lookup. Exception Type: "%s". Exception: "%s".',
            type(e),
            e
        )
        return 0.0, 0.0

    try:
        return result.latitude, result.longitude
    except AttributeError:
        return 0.0, 0.0


def get_distance_in_km(
    mirror_coords: Tuple[float, float],
    request_coords: Tuple[float, float]
):
    km = int(haversine(mirror_coords, request_coords))
    return km


def sort_mirrors_by_distance(request_geo_data: Tuple[float, float], mirrors: list):
    mirrors_sorted = []
    for mirror in mirrors:
        mirrors_sorted.append({
            'distance': get_distance_in_km(
                mirror_coords=(mirror.location.latitude, mirror.location.longitude),
                request_coords=request_geo_data
            ),
            'mirror': mirror
        })
    mirrors = sorted(mirrors_sorted, key=lambda i: i['distance'])
    return mirrors


def randomize_mirrors_within_distance(mirrors: list, shuffle_distance: int = RANDOMIZE_WITHIN_KM):
    mirrors_shuffled = []
    other_mirrors = []
    for mirror in mirrors:
        if mirror['distance'] <= shuffle_distance:
            mirrors_shuffled.append(mirror['mirror'])
        else:
            other_mirrors.append(mirror['mirror'])

    random.shuffle(mirrors_shuffled)
    return mirrors_shuffled + other_mirrors
