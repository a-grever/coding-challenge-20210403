FROM python:3.8

WORKDIR /usr/src/import/

COPY ./requirements.txt /usr/src/import/requirements.txt
RUN pip install -r /usr/src/import/requirements.txt

COPY ./etls /usr/src/import/etls
COPY ./tests /usr/src/import/tests
COPY ./Makefile ./setup.cfg /usr/src/import/

ENV PYTHONPATH=/usr/src/import/etls
