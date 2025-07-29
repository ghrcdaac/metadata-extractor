#!/bin/bash
source ./common.sh


function check_exit {
EXIT_STATUS=$?
if [ $EXIT_STATUS -ne 0 ]; then
   exit $EXIT_STATUS
fi
}

function update_lambda_or_skip_local() {
  # $1 ACCOUNT_NUMBER
  # $2 PREFIX
  # $3 LAMBDA_NAME
  echo "Checking Lambda Update Status..."
  check_lambda_exist=$(aws lambda get-function --region $AWS_REGION $ADD_PROFILE --function-name $2-$3 2>/dev/null)
  if [[ ! -n "${check_lambda_exist}" ]]; then
    echo "Lambda ${2}-${3} Does Not Exist SKIPPING UPDATE"
  else
    echo "Lambda ${2}-${3} Exists"
    echo "Updating Lambda ${2}-${3}"
    docker_image_name=$1.dkr.ecr.$AWS_REGION.amazonaws.com/$REPO_NAME
    aws lambda update-function-code \
        --function-name $2-$3 \
        --image-uri ${docker_image_name}:latest \
        --region ${AWS_REGION} \
        $ADD_PROFILE
    echo "Lambda ${2}-${3} Updated"
  fi
}

if [[ -z "${AWS_PROFILE}" ]]; then
  echo $AWS_PROFILE
  read -rp 'AWS_PROFILE: ' AWS_PROFILE
fi

if [[ -z "${STACK_PREFIX}" ]]; then
  read -rp 'STACK_PREFIX: ' STACK_PREFIX
fi

AWS_ACCOUNT_ID=$(get_account_id $AWS_PROFILE)


docker build -t ghcr.io/ghrcdaac/mdx:base ./mdx_base
docker build -t ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/$REPO_NAME .
check_exit

create_ecr_repo_or_skip $AWS_ACCOUNT_ID $STACK_PREFIX
check_exit

push_to_ecr $AWS_ACCOUNT_ID $STACK_PREFIX
check_exit

update_lambda_or_skip_local $AWS_ACCOUNT_ID $STACK_PREFIX "mdx_docker_lambda_1g"
check_exit

update_lambda_or_skip_local $AWS_ACCOUNT_ID $STACK_PREFIX "mdx_docker_lambda_3g"
check_exit

stop_mdx_task $STACK_PREFIX
check_exit