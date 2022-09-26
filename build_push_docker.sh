#!/bin/bash

function check_exit {
EXIT_STATUS=$?
if [ $EXIT_STATUS -ne 0 ]; then
   exit $EXIT_STATUS
fi
}




export REPO_NAME=mdx_docker_lambda
export AWS_REGION=us-west-2
read -rp 'AWS_PROFILE: ' AWS_PROFILE
read -rp 'Stack Prefix: ' STACK_PREFIX
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --profile $AWS_PROFILE | tr -d '"')

function update_lambda {
docker_image_name=$1.dkr.ecr.$AWS_REGION.amazonaws.com/$REPO_NAME
aws lambda update-function-code \
--function-name $2-mdx_lambda \
--image-uri ${docker_image_name}:latest \
--region ${AWS_REGION} \
--profile ${AWS_PROFILE}
}

function update_lambda_or_skip() {
  check_lambda_exist=$(aws lambda get-function --region $AWS_REGION --profile $AWS_PROFILE --function-name $2-$REPO_NAME 2> /dev/null)
    if [[ ! -n "${check_lambda_exist}" ]]; then
    echo "NO lambda found ${REPO_NAME} SKIPPING"
    else
      update_lambda $1 $2
  fi
}

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
update_lambda_or_skip $ACCOUNT_NUMBER $prefix

check_exit
