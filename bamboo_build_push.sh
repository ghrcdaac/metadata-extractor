#!/bin/bash

set -o nounset
set -o pipefail
export REPO_NAME=mdx_docker_lambda
export AWS_REGION=$bamboo_AWS_REGION
#access_keys=( $bamboo_AWS_SBX_ACCESS_KEY $bamboo_AWS_SIT_ACCESS_KEY $bamboo_AWS_UAT_ACCESS_KEY $bamboo_AWS_PROD_ACCESS_KEY )
#secret_keys=( $bamboo_AWS_SBX_SECRET_ACCESS_KEY $bamboo_AWS_SIT_SECRET_ACCESS_KEY $bamboo_AWS_UAT_SECRET_ACCESS_KEY $bamboo_AWS_PROD_SECRET_ACCESS_KEY )
#prefixes=( $bamboo_PREFIX_SBX $bamboo_PREFIX_SIT $bamboo_PREFIX_UAT $bamboo_PREFIX_PROD )
#account_numbers=( $bamboo_ACCOUNT_NUMBER_SBX $bamboo_ACCOUNT_NUMBER_SIT $bamboo_ACCOUNT_NUMBER_UAT $bamboo_ACCOUNT_NUMBER_PROD )

access_keys=( $bamboo_AWS_SBX_ACCESS_KEY $bamboo_AWS_SIT_ACCESS_KEY $bamboo_AWS_UAT_ACCESS_KEY )
secret_keys=( $bamboo_AWS_SBX_SECRET_ACCESS_KEY $bamboo_AWS_SIT_SECRET_ACCESS_KEY $bamboo_AWS_UAT_SECRET_ACCESS_KEY )
prefixes=( $bamboo_PREFIX_SBX $bamboo_PREFIX_SIT $bamboo_PREFIX_UAT )
account_numbers=( $bamboo_ACCOUNT_NUMBER_SBX $bamboo_ACCOUNT_NUMBER_SIT $bamboo_ACCOUNT_NUMBER_UAT )


function create_ecr_repo_or_skip() {
  check_repo_exist=$(aws ecr describe-repositories --region $AWS_REGION --repository-names $REPO_NAME 2> /dev/null)
  if [[ ! -n "${check_repo_exist}" ]]; then
    echo "we need to create ${REPO_NAME}"
    aws ecr create-repository --repository-name $REPO_NAME --region $AWS_REGION
    echo "${REPO_NAME} was created"
  fi

}

function update_lambda_or_skip() {
  check_lambda_exist=$(aws lambda get-function --region $AWS_REGION --function-name $2-$REPO_NAME 2> /dev/null)
    if [[ ! -n "${check_lambda_exist}" ]]; then
    echo "NO lambda found $2-${REPO_NAME} SKIPPING"
    else
      update_lambda $1 $2
  fi
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



function build_docker {
echo $bamboo_GITHUB_REGISTRY_READ_TOKEN_SECRET | docker login ghcr.io -u bamboo_login --password-stdin
if [[ $(uname -m) == arm64* ]]; then
  docker_build="docker buildx build --push --platform linux/arm64,linux/amd64 -t"
else
 docker_build="docker build -t"
fi
${docker_build} $1 .

}

function update_lambda {
docker_image_name=$1.dkr.ecr.$AWS_REGION.amazonaws.com/$REPO_NAME
aws lambda update-function-code \
--function-name $2-mdx_lambda \
--image-uri ${docker_image_name}:latest \
--region ${AWS_REGION}
}

len=${#access_keys[@]}

# Check keys
for (( i=0; i<$len; i++ ))
do
  export AWS_ACCESS_KEY_ID=${access_keys[$i]}
  export AWS_SECRET_ACCESS_KEY=${secret_keys[$i]}
  aws sts get-caller-identity
  (($? != 0)) && { printf '%s\n' "Command exited with non-zero. AWS keys invalid"; exit 1; }
done

build_docker mdx

# Copy test results
docker run --rm -v $PWD/test_results:/opt/mount --entrypoint cp  mdx  /var/task/test_results/test_metadata_extractor.xml  /opt/mount/test_metadata_extractor.xml

for (( i=0; i<$len; i++ ))
do
  export AWS_ACCESS_KEY_ID=${access_keys[$i]}
  export AWS_SECRET_ACCESS_KEY=${secret_keys[$i]}
  export ACCOUNT_NUMBER=${account_numbers[$i]}
  export prefix=${prefixes[$i]}
  create_ecr_repo_or_skip
  # Push mdx to all account's ecr
  push_to_ecr $ACCOUNT_NUMBER $prefix
  update_lambda_or_skip $ACCOUNT_NUMBER $prefix



done

docker rmi mdx

