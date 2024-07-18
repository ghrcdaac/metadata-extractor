#!/bin/bash
source ./common.sh


function check_exit {
EXIT_STATUS=$?
if [ $EXIT_STATUS -ne 0 ]; then
   exit $EXIT_STATUS
fi
}

function update_lambda_or_skip_local() {
  echo "Checking Lambda Update Status..."
  check_lambda_exist=$(aws lambda get-function --region $AWS_REGION $ADD_PROFILE --function-name $2-$REPO_NAME 2>/dev/null)
  if [[ ! -n "${check_lambda_exist}" ]]; then
    echo "Lambda ${2}-${REPO_NAME} Does Not Exist SKIPPING UPDATE"
  else
    echo "Lambda ${2}-${REPO_NAME} Exists"
    echo "Updating Lambda ${2}-${REPO_NAME}"
    docker_image_name=$1.dkr.ecr.$AWS_REGION.amazonaws.com/$REPO_NAME
    aws lambda update-function-code \
        --function-name $2-$REPO_NAME \
        --image-uri ${docker_image_name}:latest \
        --region ${AWS_REGION} \
        $ADD_PROFILE
    echo "Lambda ${2}-${REPO_NAME} Updated"
  fi
}

read -rp 'AWS_PROFILE: ' AWS_PROFILE
read -rp 'Stack Prefix: ' STACK_PREFIX
AWS_ACCOUNT_ID=$(get_account_id $AWS_PROFILE)

if [[ $(uname -m) == arm64* ]]; then
  docker_build="docker buildx build --push --platform linux/arm64,linux/amd64 -t"
else
  docker_build="docker build -t"
fi
${docker_build} ghcr.io/ghrcdaac/mdx:base ./mdx_base
${docker_build} ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/$REPO_NAME .
check_exit

create_ecr_repo_or_skip $AWS_ACCOUNT_ID $STACK_PREFIX
check_exit

push_to_ecr $AWS_ACCOUNT_ID $STACK_PREFIX
check_exit

update_lambda_or_skip_local $AWS_ACCOUNT_ID $STACK_PREFIX
check_exit
