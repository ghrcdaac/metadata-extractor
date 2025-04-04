FROM ghcr.io/ghrcdaac/mdx:base

ARG stage

COPY requirements*.txt /tmp/

RUN pip install --upgrade --force-reinstall -r /tmp/requirements.txt --target "${LAMBDA_TASK_ROOT}"

ADD mdx ${LAMBDA_TASK_ROOT}

# Only if stage is other than dev
RUN if [ "$stage" != "prod" ] ; then  \
     pip install -r /tmp/requirements-dev.txt && \
     python -m pytest --junitxml=./test_results/test_metadata_extractor.xml test; \
   fi

RUN rm -rf test

CMD [ "main.handler" ]
