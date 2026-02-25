# coding=utf-8
import ipaddress
import os
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Optional

from flask import (
    Flask,
    request,
    Response,
    render_template,
)
from flask_api.status import HTTP_200_OK
from flask_bs4 import Bootstrap
from werkzeug.exceptions import InternalServerError

from api.exceptions import (
    BaseCustomException,
    AuthException,
    UnknownRepoAttribute,
)
from api.handlers import (
    get_mirrors_list,
    get_all_mirrors,
    get_isos_list_by_countries,
    get_main_isos_table,
    get_main_isos_table_kitten,
    check_optional_version,
    get_optional_module_from_version,
    get_allowed_version,
    get_allowed_arch,
)
from api.utils import (
    success_result,
    error_result,
    jsonify_response,
    get_geo_dict_by_ip,
    get_config
)
from common.sentry import (
    init_sentry_client,
    get_deploy_environment_name,
    get_logger,
)
from db.db_engine import FlaskCacheEngine

app = Flask('app')
# for profiling, comment when not profiling
# from werkzeug.middleware.profiler import ProfilerMiddleware
# app.wsgi_app = ProfilerMiddleware(
#     app.wsgi_app,
#     profile_dir='/home/mirror-service/profiles/',  # Directory to store profiling results
#     sort_by=('cumulative',)    # Sort output by cumulative time
# )
# end for profiling
app.url_map.strict_slashes = False
Bootstrap(app)
logger = get_logger(__name__)
if os.getenv('SENTRY_DSN'):
    init_sentry_client()
cache = FlaskCacheEngine.get_instance(app=app, ro=False)
_is_dev_environment = get_deploy_environment_name().lower() not in (
    'production', 'staging',
)


def _get_bypass_cache() -> bool:
    """Check if bypass_cache was requested and is allowed (dev only)."""
    if not _is_dev_environment:
        return False
    return request.args.get('bypass_cache', '').lower() in (
        '1', 'true', 'yes',
    )


@app.context_processor
def inject_now_date():
    return {
        'now': datetime.now(timezone.utc),
    }


def _get_request_ip() -> Optional[str]:
    test_ip_address = os.getenv('TEST_IP_ADDRESS', None)
    ip_address = request.headers.get('X-Forwarded-For')
    result = None
    if ip_address and ',' in ip_address:
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
    elif ip_address:
        result = ip_address
    resolved = test_ip_address or result or request.remote_addr
    # Validate the resolved IP; return None if unparseable so that
    # downstream callers fall back to returning the full mirror list.
    if resolved:
        try:
            ipaddress.ip_address(resolved.strip())
            return resolved.strip()
        except ValueError:
            logger.warning(
                'Could not parse a valid IP address from request. '
                'Resolved value: %s, X-Forwarded-For: %s, remote_addr: %s',
                resolved,
                ip_address,
                request.remote_addr,
            )
    return None


def _ip_to_network_prefix(ip: str) -> str:
    """
    Convert an IP address to its network prefix for cache grouping.
    IPv4 addresses are grouped by /24, IPv6 by /64.
    This improves cache hit rates by sharing cached mirror lists
    across all IPs within the same network block.
    """
    try:
        addr = ipaddress.ip_address(ip.strip())
        if isinstance(addr, ipaddress.IPv4Address):
            network = ipaddress.ip_network(f'{addr}/24', strict=False)
        else:
            network = ipaddress.ip_network(f'{addr}/64', strict=False)
        return str(network.network_address)
    except ValueError:
        return ip


def make_redis_key(
    ip: Optional[str] = None,
    protocol: Optional[str] = None,
    country: Optional[str] = None,
    module: Optional[str] = None,
) -> Optional[str]:
    if not ip:
        ip = _get_request_ip()
    if not ip:
        return None
    cache_key = _ip_to_network_prefix(ip)
    if protocol:
        cache_key = f'{cache_key}_{protocol}'
    if country:
        cache_key = f'{cache_key}_{country}'
    if module:
        cache_key = f'{cache_key}_{module}'
    return cache_key


@app.route(
    '/debug/json/ip_info',
    methods=('GET',),
)
@error_result
def my_ip_and_headers():
    result = dict(**request.headers)
    ips = [request.remote_addr]
    for ip in [
                  request.headers.get('X-Real-Ip')
              ] + request.headers.get('X-Forwarded-For', '').split(','):
        if ip:
            ips.append(ip.strip())
    result['geodata'] = {}
    for ip in ips:
        result['geodata'][ip] = get_geo_dict_by_ip(ip)

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
    config = get_config()
    
    # protocol get arg
    request_protocol = request.args.get('protocol')
    if request_protocol and request_protocol not in ['http', 'https']:
        return 'Invalid input for protocol, valid options: http, https'
    # country get arg
    request_country = request.args.get('country')
    if request_country and len(request_country) != 2:
        return (
            'Invalid input for country, '
            'valid options are 2 letter country codes'
        )
    # arch get arg
    request_arch = request.args.get('arch')
    if request_arch:
        if not get_allowed_arch(
            arch=request_arch,
            version=version,
            arches=config.arches,
        ):
            return (
                'Invalid arch/version combination requested, '
                f'valid options are {config.arches}'
            )
    # bypass_cache get arg (dev only)
    bypass_cache = _get_bypass_cache()
    
    # check if optional module
    module = None
    if version in check_optional_version(
        version=version,
        optional_module_versions=config.optional_module_versions,
    ):
        module = get_optional_module_from_version(
            version=version,
            optional_module_versions=config.optional_module_versions,
        )
    
    ip_address = _get_request_ip()

    mirrors = get_mirrors_list(
        ip_address=ip_address,
        version=version,
        arch=request_arch,
        repository=repository,
        request_protocol=request_protocol,
        request_country=request_country,
        debug_info=False,
        redis_key=make_redis_key(
            ip=ip_address,
            protocol=request_protocol,
            country=request_country,
            module=module,
        ),
        module=module,
        bypass_cache=bypass_cache,
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
    bypass_cache = _get_bypass_cache()
    ip_address = _get_request_ip()
    return get_mirrors_list(
        ip_address=ip_address,
        version=version,
        arch=arch,
        repository=None,
        iso_list=True,
        bypass_cache=bypass_cache,
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
        'main_title': 'AlmaLinux ISO links'
    }
    config = get_config()
    if arch is None or version is None:
        data.update({
            'isos_list': get_main_isos_table(config=config),
        })

        return render_template('isos_main.html', title='AlmaLinux ISOs', **data)
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
            optional_module_versions=config.optional_module_versions
        )
        arch = get_allowed_arch(
            arch=arch,
            version=version,
            arches=config.arches,
        )
        data.update({
            'arch': arch,
            'version': version,
            'mirror_list': mirrors_by_countries,
            'nearest_mirrors': nearest_mirrors,
        })
        return render_template('isos.html', title='AlmaLinux ISOs', **data)


@app.route(
    '/kitten/isos',
    methods=('GET',),
)
@app.route(
    '/kitten/isos.html',
    methods=('GET',),
)
@app.route(
    '/kitten/isos/<arch>/<version>',
    methods=('GET',),
)
@app.route(
    '/kitten/isos/<arch>/<version>.html',
    methods=('GET',),
)
def kitten_isos(
        arch: str = None,
        version: str = None,
):
    data = {
        'main_title': 'AlmaLinux Kitten ISO links',
        'kitten': True
    }
    config = get_config()
    if arch is None or version is None:
        data.update({
            'isos_list': get_main_isos_table_kitten(config=config),
        })

        return render_template('isos_main.html', title='AlmaLinux Kitten ISOs', **data)
    else:
        ip_address = _get_request_ip()
        (
            mirrors_by_countries,
            nearest_mirrors
        ) = get_isos_list_by_countries(
            ip_address=ip_address,
            module='kitten'
        )
        version = get_allowed_version(
            versions=config.versions,
            # ISOs are stored only for active versions (non-vault)
            vault_versions=[],
            duplicated_versions=config.duplicated_versions,
            version=version,
            optional_module_versions=config.optional_module_versions
        )
        arch = get_allowed_arch(
            arch=arch,
            version=version,
            arches=config.arches,
        )
        data.update({
            'arch': arch,
            'version': version,
            'mirror_list': mirrors_by_countries,
            'nearest_mirrors': nearest_mirrors,
        })
        return render_template('isos.html', title='AlmaLinux Kitten ISOs', **data)


URL_TYPES = ['http', 'https', 'rsync']


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
        'module': None,
        'column_names': [
            'Name',
            'Sponsor',
            'Status',
            'Continent',
            'Region',
            *(item.upper() for item in URL_TYPES),
            'IPv6'
        ],
        'url_types': URL_TYPES,
        'mirror_list': mirrors,
        'main_title': 'AlmaLinux Mirrors',
    }
    return render_template('mirrors.html', title='AlmaLinux Mirrors', **data)


@app.route(
    '/kitten',
    methods=('GET',),
)
def mirrors_table_kitten(all_mirrors: bool = False):
    if all_mirrors:
        mirrors = get_all_mirrors(request_module='kitten')
    else:
        mirrors = get_all_mirrors(
            get_working_mirrors=True,
            get_expired_mirrors=True,
            get_without_private_mirrors=True,
            get_without_cloud_mirrors=True,
            request_module='kitten'
        )
    data = {
        'module': 'kitten',
        'column_names': [
            'Name',
            'Sponsor',
            'Status',
            'Continent',
            'Region',
            *(item.upper() for item in URL_TYPES),
            'IPv6'
        ],
        'url_types': URL_TYPES,
        'mirror_list': mirrors,
        'main_title': 'AlmaLinux Kitten Mirrors',
    }
    return render_template('mirrors.html', title='AlmaLinux Kitten Mirrors', **data)


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
    app.run(
        debug=True,
        host='0.0.0.0',
        port=int(os.getenv('LOCAL_FLASK_PORT', 8080)),
    )
