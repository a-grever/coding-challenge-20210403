FROM python:3.8

WORKDIR /usr/src/import/

COPY ./requirements.txt ./requirements.dev.txt /usr/src/import/
RUN pip install -r /usr/src/import/requirements.txt && \
    pip install -r /usr/src/import/requirements.dev.txt

COPY ./etls /usr/src/import/etls
COPY ./tests /usr/src/import/tests
COPY ./Makefile ./setup.cfg ./pylint.cfg /usr/src/import/

ENV PYTHONPATH=/usr/src/import/etls
