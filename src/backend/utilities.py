# coding=utf-8
from flask import (
    Flask,
    Response,
)

from api.handlers import update_mirrors_handler
from werkzeug.exceptions import InternalServerError

from api.exceptions import (
    BaseCustomException,
    AuthException,
    UnknownRepoAttribute,
)
from db.db_engine import FlaskCacheEngine
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
from flask_bs4 import Bootstrap

app = Flask('app')
app.url_map.strict_slashes = False
Bootstrap(app)
logger = get_logger(__name__)
init_sentry_client()
cache = FlaskCacheEngine.get_instance(app)


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
