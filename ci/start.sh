#! /usr/bin/env sh
set -e

# Start Supervisor, with Nginx and uWSGI
exec uwsgi --ini /src/app/uwsgi.ini -p "${UWSGI_PROCESSES:-2}"
