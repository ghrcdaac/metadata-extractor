FROM public.ecr.aws/lambda/python:3.8


RUN yum install -y gcc && \
        yum install -y git


RUN pip3 install git+https://github.com/ARM-DOE/pyart.git#egg=arm-pyart --target "${LAMBDA_TASK_ROOT}"

RUN pip3 install pyhdf --target "${LAMBDA_TASK_ROOT}"

RUN yum install -y libxml2
# For development
RUN     pip3 install ipython

# 1 - Clean up extra files
# 2 - Install MDX 
# 3 - Create an env variable to add dev dependencies 
# 4 - Create src folder with all mdx files? COPY src ${LAMBDA_TASK_ROOT}
# 5 - Run pytest in the env is !prod  and mount the test report 
# 6 - delete all test folders 
COPY . ${LAMBDA_TASK_ROOT}

RUN     pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"


CMD [ "app.handler" ]
#ENTRYPOINT [ "/bin/bash" ]

