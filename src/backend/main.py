# coding=utf-8
from typing import AnyStr

from flask import (
    Flask,
    request,
    Response,
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


app = Flask(__name__)
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
    '/update_mirrors'
)
@success_result
@error_result
@auth_key_required
def update_mirrors():
    return update_mirrors_handler()


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


if __name__ == '__main__':
    app.run(
        host='localhost',
        port=8080,
        debug=True,
    )
