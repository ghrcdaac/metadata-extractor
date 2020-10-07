#!/bin/bash

function check_exit {
EXIT_STATUS=$?
if [ $EXIT_STATUS -ne 0 ]; then
   exit $EXIT_STATUS
fi
}

REPO_NAME=mdx
docker build -t ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/$REPO_NAME .
check_exit
$(aws ecr get-login --no-include-email --region ${AWS_REGION} --profile $AWS_PROFILE)
aws ecr create-repository --repository-name $REPO_NAME --profile $AWS_PROFILE --region ${AWS_REGION} 2> /dev/null
docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/$REPO_NAME
check_exit
