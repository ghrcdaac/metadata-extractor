FROM ghcr.io/ghrcdaac/mdx:base

LABEL maintainer="Abdelhak Marouane <am0089@uah.edu>"


ARG stage


# Only if stage is other than dev
ADD mdx ${LAMBDA_TASK_ROOT}

COPY requirements*.txt /tmp/.

RUN     pip install -r /tmp/requirements.txt --target "${LAMBDA_TASK_ROOT}"

RUN if [ "$stage" != "prod" ] ; then  \
    pip install -r /tmp/requirements-dev.txt && \
    python -m pytest --junitxml=./test_results/test_metadata_extractor.xml test; \

  fi

RUN rm -rf test

#CMD [ "app.handler" ]
ENTRYPOINT ["/bin/bash"]
