# coding=utf-8
from typing import AnyStr

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
    BadRequestFormatExceptioin,
)
from api.handlers import (
    update_mirrors_handler,
    get_mirrors_list,
    get_all_mirrors,
    get_url_types,
)
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
from flask_bootstrap import Bootstrap


app = Flask(__name__)
Bootstrap(app)
init_sentry_client()
logger = get_logger(__name__)


@app.route(
    '/mirrorlist/<version>/<repository>',
    methods=('GET',),
)
@success_result
@error_result
def get_mirror_list(
        version: AnyStr,
        repository: AnyStr,
):
    return get_mirrors_list(
        ip_address=request.remote_addr,
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
def update_mirrors():
    return update_mirrors_handler()


@app.route(
    '/',
    methods=('GET',),
)
def mirrors_table():
    url_types = get_url_types()
    data = {
        'column_names': [
            'Name',
            'Sponsor',
            'Status',
            'Continent',
            'Country',
            *url_types,
        ],
        'url_types': url_types,
        'mirror_list': get_all_mirrors(),
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


@app.errorhandler(BadRequestFormatExceptioin)
def handle_bad_request_format(error: BadRequestFormatExceptioin) -> Response:
    logger.exception(error.message, *error.args)
    return jsonify_response(
        status='error',
        result={
            'message': str(error),
        },
        status_code=error.response_code,
    )
