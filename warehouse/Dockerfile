FROM python:3.8

WORKDIR /usr/src/warehouse/

COPY ./requirements.txt ./requirements.dev.txt /usr/src/warehouse/
RUN pip install -r /usr/src/warehouse/requirements.txt && \
    pip install -r /usr/src/warehouse/requirements.dev.txt

COPY ./warehouse /usr/src/warehouse/warehouse
COPY ./tests /usr/src/warehouse/tests
COPY ./Makefile ./setup.cfg ./pylint.cfg /usr/src/warehouse/

ENV PYTHONPATH=/usr/src/warehouse/warehouse
