FROM    continuumio/miniconda3:4.8.2

LABEL   maintainer="Abdelhak Marouane <am0089@uah.edu>"
RUN     apt-get update && \
        apt-get install -y libxml2-utils

WORKDIR /build

ENV     BUILD=/build

COPY    . $BUILD/

RUN     bash conda-requirements.sh && \
        python setup.py install

# Development
RUN     pip install -r requirements-dev.txt && \
        pytest test && \
        rm -rf test
