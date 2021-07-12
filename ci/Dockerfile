FROM python:3 AS python38-uwsgi
COPY requirements.txt requirements.txt
RUN pip install -U -r requirements.txt

WORKDIR /src/app

# Add webserver user
RUN useradd webserver

COPY start.sh /start.sh
RUN chmod +x /start.sh
ENTRYPOINT /start.sh