FROM python:3 AS python38
COPY requirements.txt requirements.txt
RUN pip install -U -r requirements.txt

COPY wait_for_port.sh /wait_for_port.sh
RUN chmod +x /wait_for_port.sh

WORKDIR /src/app

# uwsgi webserver image
FROM python38 AS python38-uwsgi
# Install uWSGI
RUN pip install uwsgi
RUN useradd webserver

COPY start.sh /start.sh
RUN chmod +x /start.sh
ENTRYPOINT /start.sh

# nginx webserver image
FROM nginx:1.18.0 as nginx