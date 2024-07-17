FROM public.ecr.aws/lambda/python:3.10

RUN yum install -y gcc git libxml2 nano

COPY requirements.txt /tmp/requirements.txt

RUN pip install -r /tmp/requirements.txt  --target "${LAMBDA_TASK_ROOT}"
