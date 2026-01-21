FROM public.ecr.aws/lambda/python:3.12

RUN dnf install -y gcc && \
        dnf install -y git

RUN dnf install -y libxml2

RUN dnf install -y nano

# COPY requirements.txt /tmp/requirements.txt

# RUN pip install -r /tmp/requirements.txt  --target "${LAMBDA_TASK_ROOT}"
