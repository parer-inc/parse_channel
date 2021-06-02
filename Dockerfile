FROM python:3.8

COPY . /usr/src/app
WORKDIR /usr/src/app

ENV PYTHONUNBUFFERED = 1

COPY requirements.txt requirements.txt

RUN python3 -m pip install -r requirements.txt
