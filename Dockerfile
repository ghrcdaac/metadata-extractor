FROM public.ecr.aws/lambda/python:3.12

RUN dnf install -y gcc && \
        dnf install -y git

RUN dnf install -y libxml2

RUN dnf install -y nano

ARG stage

COPY requirements*.txt /tmp/

RUN pip install --upgrade pip

RUN pip install --upgrade --force-reinstall -r /tmp/requirements.txt --target "${LAMBDA_TASK_ROOT}"

ADD mdx ${LAMBDA_TASK_ROOT}

# Only if stage is other than dev
RUN if [ "$stage" != "prod" ] ; then  \
     pip install -r /tmp/requirements-dev.txt && \
     python -m pytest test --doctest-modules --junitxml=./test_results/test_metadata_extractor.xml --cov=com --cov-report=xml --cov-report=html; \
   fi

RUN rm -rf test

CMD [ "main.handler" ]
