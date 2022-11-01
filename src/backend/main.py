# coding=utf-8
import os
from datetime import datetime

from flask import (
    Flask,
    request,
    Response,
    render_template,
)
from werkzeug.exceptions import InternalServerError

from api.exceptions import (
    BaseCustomException,
    AuthException,
    UnknownRepositoryOrVersion,
)
from api.handlers import (
    update_mirrors_handler,
    get_mirrors_list,
    get_all_mirrors,
    get_url_types,
    get_isos_list_by_countries,
    get_main_isos_table,
    SERVICE_CONFIG_JSON_SCHEMA_DIR_PATH,
    SERVICE_CONFIG_PATH,
)
from yaml_snippets.utils import get_config
from api.utils import (
    success_result,
    error_result,
    auth_key_required,
    jsonify_response,
)
from common.sentry import (
    init_sentry_client,
    get_logger,
)
from flask_api.status import HTTP_200_OK
from flask_bs4 import Bootstrap


app = Flask('app')
Bootstrap(app)
init_sentry_client()
logger = get_logger(__name__)


@app.context_processor
def inject_now_date():
    return {
        'now': datetime.utcnow(),
    }


def _get_request_ip() -> str:
    test_ip_address = os.getenv('TEST_IP_ADDRESS', None)
    ip_address = request.headers.get('X-Forwarded-For') or request.remote_addr
    if ',' in ip_address:
        ip_address = [item.strip() for item in ip_address.split(',')][0]
    return test_ip_address or ip_address


@app.route(
    '/my_ip_and_headers',
    methods=('GET',),
)
@error_result
def my_ip_and_headers():
    result = {
        'my_ip': request.remote_addr,
    }
    result.update(request.headers)

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
async def get_mirror_list(
        version: str,
        repository: str,
):
    ip_address = _get_request_ip()
    return await get_mirrors_list(
        ip_address=ip_address,
        version=version,
        repository=repository,
    )


@app.route(
    '/update_mirrors',
    methods=('POST',),
)
@success_result
@error_result
@auth_key_required
async def update_mirrors():
    result = await update_mirrors_handler()
    return result


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
async def isos(
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
        mirrors_by_countries, nearest_mirrors = await get_isos_list_by_countries(
            arch=arch,
            version=version,
            ip_address=ip_address,
            config=config,
        )
        data.update({
            'arch': arch,
            'version': version,
            'mirror_list': mirrors_by_countries,
            'nearest_mirrors': nearest_mirrors,
        })
        return render_template('isos.html', **data)


@app.route(
    '/',
    methods=('GET',),
)
async def mirrors_table():
    url_types = sorted(await get_url_types())
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
        'mirror_list': await get_all_mirrors(),
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


@app.errorhandler(UnknownRepositoryOrVersion)
def handle_unknown_repository_or_version(
        error: UnknownRepositoryOrVersion,
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
