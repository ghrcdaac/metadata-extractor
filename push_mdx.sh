#!/bin/bash

# set -o errexit
# set -o nounset
# set -o pipefail

function loginECR {
    # $1 is AWS_ACCOUNT_ID $2 is AWS_PROFILE
    account_id=$1
    pass=$(aws ecr get-login-password \
    --region ${AWS_REGION:-us-west-2} \
    --profile $2)
    echo $pass  | docker login  --username AWS  --password-stdin $account_id.dkr.ecr.${AWS_REGION:-us-west-2}.amazonaws.com
}

function stop_ecs_task() {
  task_arns=$(aws ecs list-tasks --cluster $1-CumulusECSCluster --family $1-$2 --query "taskArns[*]" --region $AWS_REGION  --profile $3| tr -d '"[],')
  for task in $task_arns
  do
    aws ecs stop-task --cluster $1-CumulusECSCluster --task $task --region $AWS_REGION --profile $3
  done
}

function build_docker {
if [[ $(uname -m) == arm64* ]]; then
  docker_build="docker buildx build --push --platform linux/arm64,linux/amd64 -t"
else
 docker_build="docker build -t"
fi
${docker_build} $1 .

}


REPO_NAME=mdx_lambda
AWS_REGION=us-west-2
read -rp 'AWS_PROFILE: ' AWS_PROFILE
read -rp 'Stack Prefix: ' STACK_PREFIX
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --profile $AWS_PROFILE | tr -d '"')
DOCKER_IAMGE=${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/$REPO_NAME
build_docker ${DOCKER_IAMGE}
loginECR $AWS_ACCOUNT_ID $AWS_PROFILE
# Uncomment this if the repo is not already created in ECR
aws ecr create-repository --repository-name $REPO_NAME --profile $AWS_PROFILE --region ${AWS_REGION} 2> /dev/null
docker push ${DOCKER_IAMGE}
