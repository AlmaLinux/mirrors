# coding=utf-8
import os

import time
from collections import defaultdict
from functools import wraps
from typing import (
    Dict,
    Any,
    AnyStr,
    Tuple,
    Optional, List,
)

import requests
from bs4 import BeautifulSoup
from geoip2.errors import AddressNotFoundError

from db.db_engine import GeoIPEngine, AsnEngine
from db.models import (
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

logger = get_logger(__name__)


AUTH_KEY = os.environ.get('AUTH_KEY')


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
    def decorated_function(*args, **kwargs):
        result = f(*args, **kwargs)
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
) -> Optional[Tuple[AnyStr, AnyStr, float, float]]:
    """
    The function returns continent, country and locations of IP in English
    """

    db = GeoIPEngine.get_instance()
    try:
        city = db.city(ip)
    except AddressNotFoundError:
        return
    country = city.country.name
    continent = city.continent.name
    latitude = city.location.latitude
    longitude = city.location.longitude

    return continent, country, latitude, longitude


def get_asn_by_ip(
        ip: AnyStr,
) -> Optional[AnyStr]:
    """
    Get ASN by an IP
    """

    db = AsnEngine.get_instance()
    try:
        return db.asn(ip).autonomous_system_number
    except AddressNotFoundError:
        return


def get_azure_subnets_json() -> Optional[Dict]:
    url = 'https://www.microsoft.com/en-us/download/confirmation.aspx?id=56519'
    link_attributes = {
        'data-bi-id': 'downloadretry',
    }
    req = requests.get(url)
    try:
        req.raise_for_status()
    except requests.RequestException as err:
        logger.error(
            'Cannot get json with Azure subnets by url "%s" because "%s"',
            url,
            err,
        )
        return
    try:
        soup = BeautifulSoup(req.content, features='lxml')
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
        req = requests.get(link_to_json_url)
        req.raise_for_status()
    except requests.RequestException as err:
        logger.error(
            'Cannot get json with Azure subnets by url "%s" because "%s"',
            link_to_json_url,
            err,
        )
    return req.json()


def get_aws_subnets_json() -> Optional[Dict]:
    url = 'https://ip-ranges.amazonaws.com/ip-ranges.json'
    try:
        req = requests.get(url)
        req.raise_for_status()
    except requests.RequestException as err:
        logger.error(
            'Cannot get json with AWS subnets by url "%s" because "%s"',
            url,
            err,
        )
        return
    return req.json()


def get_azure_subnets():
    data_json = get_azure_subnets_json()
    if data_json is None:
        return
    values = data_json['values']
    subnets = {}
    for value in values:
        if value['name'].startswith('AzureCloud.'):
            properties = value['properties']
            subnets[properties['region'].lower()] = properties['addressPrefixes']
    return subnets


def get_aws_subnets():
    data_json = get_aws_subnets_json()
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
    cloud_region = mirror_info.cloud_region.lower()
    cloud_type = mirror_info.cloud_type.lower()
    if subnets is not None and \
            cloud_type in ('azure', 'aws') and \
            cloud_region in subnets:
        mirror_info.subnets = subnets[cloud_region]
