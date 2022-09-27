#! /bin/bash

export REPO_NAME=mdx_docker_lambda
export AWS_REGION=${bamboo_AWS_REGION:-us-west-2}


function update_lambda_or_skip() {
  check_lambda_exist=$(aws lambda get-function --region $AWS_REGION --profile $AWS_PROFILE --function-name $2-$REPO_NAME 2> /dev/null)
    if [[ ! -n "${check_lambda_exist}" ]]; then
    echo "NO lambda found ${REPO_NAME} SKIPPING"
    else
      update_lambda $1 $2
  fi
}

function update_lambda {
docker_image_name=$1.dkr.ecr.$AWS_REGION.amazonaws.com/$REPO_NAME
aws lambda update-function-code \
--function-name $2-mdx_lambda \
--image-uri ${docker_image_name}:latest \
--region ${AWS_REGION} \
--profile ${AWS_PROFILE}
}


function push_to_ecr() {
  # $ACCOUNT_NUMBER = $1
  # $prefix = $2
  docker_image_name=$1.dkr.ecr.$AWS_REGION.amazonaws.com/$REPO_NAME
  docker tag mdx $docker_image_name

  aws ecr get-login-password \
      --region $AWS_REGION \
  | docker login \
      --username AWS \
      --password-stdin $1.dkr.ecr.$AWS_REGION.amazonaws.com


  echo "pushing image to ecr"
  docker push $docker_image_name


  docker rmi $docker_image_name
}


function create_ecr_repo_or_skip() {
  check_repo_exist=$(aws ecr describe-repositories --region $AWS_REGION --repository-names $REPO_NAME 2> /dev/null)
  if [[ ! -n "${check_repo_exist}" ]]; then
    echo "we need to create ${REPO_NAME}"
    aws ecr create-repository --repository-name $REPO_NAME --region $AWS_REGION
    echo "${REPO_NAME} was created"
  fi

}

function get_account_id() {
  # $1 AWS_PROFILE
  echo $(aws sts get-caller-identity --query Account --profile $1 | tr -d '"')
}
