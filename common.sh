#! /bin/bash

export REPO_NAME=mdx_docker_lambda
export AWS_REGION=${bamboo_AWS_REGION:-us-west-2}

if [[ -z "${AWS_PROFILE}" ]]; then
  ADD_PROFILE=""
else
  ADD_PROFILE="--profile $AWS_PROFILE"
fi

function update_lambda_or_skip() {
  echo "Checking Lambda Update Status..."
  check_lambda_exist=$(aws lambda get-function --region $AWS_REGION $ADD_PROFILE --function-name $2-$REPO_NAME 2>/dev/null)
  if [[ ! -n "${check_lambda_exist}" ]]; then
    echo "Lambda ${2}-${REPO_NAME} Does Not Exist SKIPPING UPDATE"
  else
    echo "Lambda ${2}-${REPO_NAME} Exists"
    update_lambda $1 $2 2>/dev/null
  fi
}

function update_lambda() {
  echo "Updating Lambda ${2}-${REPO_NAME}"
  docker_image_name=$1.dkr.ecr.$AWS_REGION.amazonaws.com/$REPO_NAME
  export cmd="aws lambda update-function-code \
    --function-name $2-$REPO_NAME \
    --image-uri ${docker_image_name}:latest \
    --region ${AWS_REGION} \
    $ADD_PROFILE"
  echo $cmd
  echo `$cmd`
  echo "Lambda ${2}-${REPO_NAME} Updated"
}

function push_to_ecr() {
  # $ACCOUNT_NUMBER = $1
  # $prefix = $2
  docker_image_name=$1.dkr.ecr.$AWS_REGION.amazonaws.com/$REPO_NAME
  docker tag mdx $docker_image_name

  echo "Signing into ECR"
  aws ecr get-login-password \
    --region $AWS_REGION |
    docker login \
      --username AWS \
      --password-stdin $1.dkr.ecr.$AWS_REGION.amazonaws.com

  echo "Pushing Image to ECR"
  docker push $docker_image_name
  echo "Removing Image from Local"
  docker rmi $docker_image_name
}

function create_ecr_repo_or_skip() {
  echo "Checking ECR Repo Status..."
  check_repo_exist=$(aws ecr describe-repositories --region $AWS_REGION --repository-names $REPO_NAME 2>/dev/null)
  if [[ ! -n "${check_repo_exist}" ]]; then
    echo "Repo creation needed for ${REPO_NAME}"
    aws ecr create-repository --repository-name $REPO_NAME --region $AWS_REGION
    echo "${REPO_NAME} was created"
  else
    echo "${REPO_NAME} already exists"
  fi

}

function get_account_id() {
  # $1 AWS_PROFILE
  echo $(aws sts get-caller-identity --query Account --profile $1 | tr -d '"')
}
