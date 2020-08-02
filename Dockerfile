FROM    centos:8

RUN     mkdir -p /usr/local/bin
RUN     export PATH=$PATH:/usr/local/bin

RUN     dnf update -y && \
        dnf install wget -y && \
        wget https://repo.anaconda.com/miniconda/Miniconda3-py38_4.8.2-Linux-x86_64.sh && \
        bash Miniconda3-py38_4.8.2-Linux-x86_64.sh -b && \
        rm Miniconda3-py38_4.8.2-Linux-x86_64.sh

WORKDIR /build

ENV     BUILD=/build PATH="/root/miniconda3/bin:${PATH}"

COPY    __init__.py requirements*.txt setup.py setup.cfg    $BUILD/
COPY    ./granule_metadata_extractor    $BUILD/granule_metadata_extractor
COPY    ./process_mdx  $BUILD/process_mdx

RUN     conda install -c conda-forge pyhdf && \
        python setup.py install
