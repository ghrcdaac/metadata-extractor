#!/bin/sh
if [ -z "${AWS_LAMBDA_RUNTIME_API}" ]; then
  exec /usr/local/bin/aws-lambda-rie /var/runtime/bootstrap -m awslambdaric $@
else
  exec /var/runtime/bootstrap -m awslambdaric $@
fi     