# coding=utf-8
import ipaddress
import os
from datetime import datetime
from typing import Optional

from dataclasses import asdict
from flask import (
    Flask,
    request,
    Response,
    render_template,
)

from api.handlers import (
    get_mirrors_list,
    get_all_mirrors,
    get_isos_list_by_countries,
    get_main_isos_table,
    SERVICE_CONFIG_JSON_SCHEMA_DIR_PATH,
    SERVICE_CONFIG_PATH, get_allowed_version, get_allowed_arch,
)
from werkzeug.exceptions import InternalServerError

from api.exceptions import (
    BaseCustomException,
    AuthException,
    UnknownRepoAttribute,
)
from api.redis import CACHE_EXPIRED_TIME, URL_TYPES_LIST_EXPIRED_TIME
from db.db_engine import FlaskCacheEngine, FlaskCacheEngineRo
from db.models import Url
from db.utils import session_scope
from yaml_snippets.utils import get_config
from api.utils import (
    success_result,
    error_result,
    jsonify_response,
    get_geo_data_by_ip,
)
from common.sentry import (
    init_sentry_client,
    get_logger,
)
from flask_api.status import HTTP_200_OK
from flask_bs4 import Bootstrap
from urllib.parse import urljoin

app = Flask('app')
app.url_map.strict_slashes = False
Bootstrap(app)
logger = get_logger(__name__)
if os.getenv('SENTRY_DSN'):
    init_sentry_client()
cache = FlaskCacheEngine.get_instance(app)
cache_ro = FlaskCacheEngineRo.get_instance(app)

@app.context_processor
def inject_now_date():
    return {
        'now': datetime.utcnow(),
    }


def _get_request_ip(*args, **kwargs) -> Optional[str]:
    test_ip_address = os.getenv('TEST_IP_ADDRESS', None)
    ip_address = request.headers.get('X-Forwarded-For')
    result = None
    if ',' in ip_address:
        for ip in ip_address.split(','):
            try:
                if not ipaddress.ip_address(ip.strip()).is_private:
                    result = ip.strip()
                    break
            except ValueError:
                logger.warning(
                    '%s does not appear to be an IPv4 or IPv6 address. '
                    'IP of a request: %s. Headers of a request: %s',
                    ip_address,
                    request.remote_addr,
                    request.headers,
                )
    else:
        result = ip_address
    return test_ip_address or result


def make_redis_key(ip = None, protocol = None, country = None, *args, **kwargs) -> str:
    if not ip:
        ip = _get_request_ip()
    cache_key = f'{ip}'
    if protocol:
        cache_key = f'{cache_key}_{protocol}'
    if country:
        cache_key = f'{cache_key}_{country}'
    return cache_key


def unless_make_cache(*args, **kwargs) -> bool:
    return _get_request_ip() is None


@app.route(
    '/debug/json/ip_info',
    methods=('GET',),
)
@error_result
def my_ip_and_headers():
    result = {}
    result.update(request.headers)
    ips = [request.remote_addr]
    for ip in [
                  request.headers.get('X-Real-Ip')
              ] + request.headers.get('X-Forwarded-For', '').split(','):
        if ip:
            ips.append(ip.strip())
    result['geodata'] = {}
    for ip in ips:
        match = get_geo_data_by_ip(ip)
        (
            continent,
            country,
            state,
            city_name,
            latitude,
            longitude,
        ) = (None, None, None, None, None, None) if not match else match
        result['geodata'][ip] = {
            'continent': continent,
            'country': country,
            'state': state,
            'city': city_name,
            'latitude': latitude,
            'longitude': longitude,
        }

    return jsonify_response(
        status='ok',
        result=result,
        status_code=HTTP_200_OK,
    )


@app.route(
    '/mirrorlist/<version>/<repository>',
    methods=('GET',),
)
@success_result
@error_result
def get_mirror_list(
        version: str,
        repository: str
):
    request_protocol = request.args.get('protocol')
    if request_protocol and request_protocol not in ["http","https"]:
        return "Invalid input for protocol, valid options: http, https"
    request_country = request.args.get('country')
    if request_country and len(request_country) != 2:
        return "Invalid input for country, valid options are 2 letter country codes"
    ip_address = _get_request_ip()

    mirrors = get_mirrors_list(
        ip_address=ip_address,
        version=version,
        arch=None,
        repository=repository,
        request_protocol=request_protocol,
        request_country=request_country,
        debug_info=False,
        redis_key=make_redis_key(ip=ip_address, protocol=request_protocol, country=request_country)
    )

    return '\n'.join(mirrors)


@app.route(
    '/debug/json/nearest_mirrors',
    methods=('GET',),
)
@error_result
def get_debug_mirror_list():
    ip_address = _get_request_ip()
    result = get_mirrors_list(
        ip_address=ip_address,
        version='8',
        arch='x86_64',
        repository=None,
        iso_list=True,
        debug_info=True,
    )
    return jsonify_response(
        status='ok',
        result=result,
        status_code=HTTP_200_OK,
    )


@app.route(
    '/debug/json/all_mirrors',
    methods=('GET',),
)
@error_result
def get_debug_all_mirrors():
    data = {}
    mirrors = get_all_mirrors()
    for mirror in mirrors:
        data[mirror.name] = asdict(mirror)
    return jsonify_response(
        status='ok',
        result=data,
        status_code=HTTP_200_OK,
    )


@app.route(
    '/isolist/<version>/<arch>',
    methods=('GET',),
)
@success_result
@error_result
def get_iso_list(
        version: str,
        arch: str,
):
    ip_address = _get_request_ip()
    return get_mirrors_list(
        ip_address=ip_address,
        version=version,
        arch=arch,
        repository=None,
        iso_list=True,
    )


@app.route(
    '/isos',
    methods=('GET',),
)
@app.route(
    '/isos.html',
    methods=('GET',),
)
@app.route(
    '/isos/<arch>/<version>',
    methods=('GET',),
)
@app.route(
    '/isos/<arch>/<version>.html',
    methods=('GET',),
)
def isos(
        arch: str = None,
        version: str = None,
):
    data = {
        'main_title': 'AlmaLinux ISOs links'
    }
    config = get_config(
        logger=logger,
        path_to_config=SERVICE_CONFIG_PATH,
        path_to_json_schema=SERVICE_CONFIG_JSON_SCHEMA_DIR_PATH,
    )
    if arch is None or version is None:
        data.update({
            'isos_list': get_main_isos_table(config=config),
        })

        return render_template('isos_main.html', **data)
    else:
        ip_address = _get_request_ip()
        (
            mirrors_by_countries,
            nearest_mirrors
        ) = get_isos_list_by_countries(
            ip_address=ip_address,
        )
        version = get_allowed_version(
            versions=config.versions,
            # ISOs are stored only for active versions (non-vault)
            vault_versions=[],
            duplicated_versions=config.duplicated_versions,
            version=version,
        )
        arch = get_allowed_arch(
            arch=arch,
            arches=config.arches,
        )
        data.update({
            'arch': arch,
            'version': version,
            'mirror_list': mirrors_by_countries,
            'nearest_mirrors': nearest_mirrors,
        })
        return render_template('isos.html', **data)


@cache.cached(
    timeout=URL_TYPES_LIST_EXPIRED_TIME,
    key_prefix='url_types',
)
def get_url_types() -> list[str]:
    with session_scope() as session:
        url_types = sorted(value[0] for value in session.query(
            Url.type
        ).distinct())
        return url_types


@app.route(
    '/debug/html/all',
    methods=('GET',),
)
def mirrors_all_table():
    return mirrors_table(all_mirrors=True)


@app.route(
    '/',
    methods=('GET',),
)
def mirrors_table(all_mirrors: bool = False):
    url_types = sorted(get_url_types())
    if all_mirrors:
        mirrors = get_all_mirrors()
    else:
        mirrors = get_all_mirrors(
            get_working_mirrors=True,
            get_expired_mirrors=True,
            get_without_private_mirrors=True,
            get_without_cloud_mirrors=True,
        )
    data = {
        'column_names': [
            'Name',
            'Sponsor',
            'Status',
            'Continent',
            'Region',
            *(item.upper() for item in url_types),
            'IPv6'
        ],
        'url_types': url_types,
        'mirror_list': mirrors,
        'main_title': 'AlmaLinux Mirrors',
    }
    return render_template('mirrors.html', **data)


@app.errorhandler(AuthException)
def handle_jwt_exception(error: BaseCustomException) -> Response:
    logger.exception(error.message, *error.args)
    return jsonify_response(
        status='error',
        result={
            'message': str(error),
        },
        status_code=error.response_code,
    )


@app.errorhandler(InternalServerError)
def handle_internal_server_error(error: InternalServerError) -> Response:
    logger.exception(error)
    return jsonify_response(
        status='error',
        result={
            'message': 'Internal server error',
        },
        status_code=error.code,
    )


@app.errorhandler(UnknownRepoAttribute)
def handle_unknown_repository_or_version(
        error: UnknownRepoAttribute,
) -> Response:
    logger.info(error.message, *error.args)
    return jsonify_response(
        status='error',
        result={
            'message': str(error),
        },
        status_code=error.response_code,
    )


if __name__ == '__main__':
    # from werkzeug.middleware.profiler import ProfilerMiddleware
    # app.wsgi_app = ProfilerMiddleware(app.wsgi_app)
    app.run(
        debug=True,
        host='0.0.0.0',
        port=int(os.getenv('LOCAL_FLASK_PORT', 8080)),
    )
