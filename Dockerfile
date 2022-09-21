FROM public.ecr.aws/lambda/python:3.8


RUN yum install -y gcc && \
        yum install -y git


RUN pip install git+https://github.com/ARM-DOE/pyart.git#egg=arm-pyart --target "${LAMBDA_TASK_ROOT}"

RUN pip install pyhdf --target "${LAMBDA_TASK_ROOT}"

RUN yum install -y libxml2

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

# 1 - Clean up extra files
# 2 - Install MDX
# 3 - Create an env variable to add dev dependencies
# 4 - Create src folder with all mdx files? COPY src ${LAMBDA_TASK_ROOT}
# 5 - Run pytest in the env is !prod  and mount the test report
# 6 - delete all test folders




#RUN     pip install pytest
#RUN     python -m pytest --junitxml=./test_results/test_metadata_extractor.xml test && \
#

#CMD [ "app.handler" ]
ENTRYPOINT ["/bin/bash"]
