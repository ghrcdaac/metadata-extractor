FROM    continuumio/miniconda3:4.8.2

LABEL   maintainer="Abdelhak Marouane <am0089@uah.edu>"
RUN     apt-get --allow-releaseinfo-change update && \
        apt-get install -y libxml2-utils

WORKDIR /build

ENV     BUILD=/build

COPY ["./conda-requirements.sh", "requirements.txt", "/build/"]

RUN     bash conda-requirements.sh && \
        pip install -r requirements.txt

# For development
COPY ["./requirements-dev.txt", "/build/"]
RUN     pip install ipython && \
        pip install -r requirements-dev.txt


COPY    . $BUILD/

RUN     python setup.py install

RUN     python -m pytest --junitxml=./test_results/test_metadata_extractor.xml test && \
        rm -rf test
