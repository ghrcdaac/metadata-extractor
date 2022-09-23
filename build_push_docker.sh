#!/bin/bash

function check_exit {
EXIT_STATUS=$?
if [ $EXIT_STATUS -ne 0 ]; then
   exit $EXIT_STATUS
fi
}

export REPO_NAME=mdx_lambda
export AWS_REGION=us-west-2
read -rp 'AWS_PROFILE: ' AWS_PROFILE
read -rp 'Stack Prefix: ' STACK_PREFIX
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --profile $AWS_PROFILE | tr -d '"')


docker build -t ghcr.io/ghrcdaac/mdx:base ./mdx_base
docker build -t ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/$REPO_NAME .
check_exit
aws ecr get-login-password \
    --region ${AWS_REGION} \
    --profile $AWS_PROFILE \
| docker login \
    --username AWS \
    --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
check_exit
aws ecr create-repository --repository-name $REPO_NAME --profile $AWS_PROFILE --region ${AWS_REGION} 2> /dev/null
docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/$REPO_NAME
aws lambda update-function-code \
--function-name $STACK_PREFIX-mdx_lambda \
--image-uri ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/$REPO_NAME:latest \
--region ${AWS_REGION} \
--profile $AWS_PROFILE

check_exit
