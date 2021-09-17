# coding=utf-8

import requests
import logging
import multiprocessing
import os
from typing import Optional

import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from sentry_sdk.integrations.redis import RedisIntegration


def get_aws_instance_api() -> str:
    """
    Get IP of a current AWS instance
    """

    meta_data_url = 'http://169.254.169.254/latest/meta-data/public-ipv4'
    try:
        req = requests.get(url=meta_data_url)
        req.raise_for_status()
        return req.text
    except (requests.ConnectionError, requests.RequestException):
        return 'ItIsNotAWSInstance'


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

    if dsn is None:
        dsn = os.getenv('SENTRY_DSN')
    print('DSN: ', dsn)
    sentry_sdk.init(
        dsn=dsn,
        environment=os.getenv('DEPLOY_ENVIRONMENT'),
        ignore_errors=[
            KeyboardInterrupt,
        ],
        integrations=[
            FlaskIntegration(),
            RedisIntegration(),
        ],
    )
    with sentry_sdk.configure_scope() as scope:
        scope.set_tag('aws_instance_ip', get_aws_instance_api())


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
        deploy_environment = os.getenv('DEPLOY_ENVIRONMENT', '')
        if deploy_environment.lower() in ('production', 'staging'):
            logging_level = logging.WARNING
        else:
            logging_level = logging.INFO
        logger.setLevel(logging_level)
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging_level)
        formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(message)s'
        )
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    return logger
