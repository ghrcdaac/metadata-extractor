FROM public.ecr.aws/lambda/python:3.10

RUN yum install -y gcc && \
        yum install -y git

RUN yum install -y libxml2

RUN yum install -y nano

COPY requirements.txt /tmp/requirements.txt

RUN pip install -r /tmp/requirements.txt  --target "${LAMBDA_TASK_ROOT}"
