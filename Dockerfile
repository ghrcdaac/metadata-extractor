FROM continuumio/miniconda3:25.3.1-1

ARG stage

SHELL ["/bin/bash","-lc"]

RUN apt --allow-releaseinfo-change update && \
    apt -y upgrade

RUN apt-get install -y \
    libxml2-utils \
    gcc \
    git \
    nano

RUN conda create -n mdx python=3.10 -y
ENV PATH="/opt/conda/envs/mdx/bin:$PATH"
RUN echo "source activate mdx" > ~/.bashrc

ARG HOME=/home/metadata-extractor
ARG MDX=${HOME}/mdx
WORKDIR ${HOME}
COPY ./mdx ${MDX}
COPY pyproject.toml ${HOME}

ENV PIP_ROOT_USER_ACTION=ignore


RUN if [ "$stage" != "prod" ] ; then  \
        conda run -n mdx pip install -e '.[test]'; \
        conda run -n mdx python -m pytest -vv; \
    else \
        conda run -n mdx pip install .; \
    fi

# Smoke test to catch bad imports
# RUN conda run -n mdx process-mdx --help

WORKDIR ${MDX}