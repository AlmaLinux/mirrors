# coding=utf-8

import logging
import multiprocessing
import os
from typing import Optional

import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration


def init_sentry_client(dsn: Optional[str] = None) -> None:
    """
    Initialize sentry client with default options
    :param dsn: project auth key
    """
    if dsn is None and os.getenv('SENTRY_DSN') is None:
        logging.warning('Sentry DSN is not defined')
    if os.getenv('SENTRY_DISABLED') == 'True':
        logging.warning('Sentry is disabled')
        return

    sentry_sdk.init(
        dsn=dsn,
        environment=os.getenv('DEPLOY_ENVIRONMENT'),
        ignore_errors=[
            KeyboardInterrupt,
        ],
        integrations=[
            FlaskIntegration(),
        ],
    )


def get_logger(logger_name: str):
    """
    Create or get existing logger
    :param logger_name: Name of the new or existing logger
    :return: logging object
    """

    # create logger or get existing
    logger = multiprocessing.get_logger()
    # Set handler if it doesn't exist
    if not len(logger.handlers):
        deploy_environment = os.getenv('DEPLOY_ENVIRONMENT')
        if deploy_environment.lower() == 'production':
            logging_level = logging.DEBUG
        else:
            logging_level = logging.DEBUG
        logger.setLevel(logging_level)
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging_level)
        formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(message)s'
        )
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    return logger
