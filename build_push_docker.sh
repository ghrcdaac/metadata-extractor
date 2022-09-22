#!/bin/bash

function check_exit {
EXIT_STATUS=$?
if [ $EXIT_STATUS -ne 0 ]; then
   exit $EXIT_STATUS
fi
}

export REPO_NAME=mdx_lamdba
AWS_REGION=us-west-2
read -rp 'AWS_PROFILE: ' AWS_PROFILE
read -rp 'Stack Prefix: ' STACK_PREFIX
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --profile $AWS_PROFILE | tr -d '"')


docker build -t ghcr.io/ghrcdaac/mdx:base ./mdx_base
docker build -t ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/$REPO_NAME .
check_exit
aws ecr get-login-password \
    --region ${AWS_REGION} \
| docker login \
    --username AWS \
    --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
check_exit
aws ecr create-repository --repository-name $REPO_NAME --profile $AWS_PROFILE --region ${AWS_REGION} 2> /dev/null
docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/$REPO_NAME
check_exit
