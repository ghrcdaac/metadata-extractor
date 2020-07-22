FROM    centos:8

RUN     mkdir -p /usr/local/bin
RUN     export PATH=$PATH:/usr/local/bin

WORKDIR /build

ENV     BUILD=/build

COPY    __init__.py                     $BUILD/
COPY    requirements.txt                $BUILD/
COPY    requirements-dev.txt            $BUILD/
COPY    setup.py                        $BUILD/
COPY    setup.cfg                       $BUILD/
COPY    granule_metadata_extractor/     $BUILD/granule_metadata_extractor/
COPY    process_mdx/                    $BUILD/process_mdx/

RUN     dnf update -y

RUN     dnf install wget -y
RUN     wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
RUN     bash Miniconda3-latest-Linux-x86_64.sh -b
ENV     PATH="/root/miniconda3/bin:${PATH}"

RUN     python setup.py install
