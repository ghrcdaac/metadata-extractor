#!/bin/bash
source ./common.sh
set -o nounset
set -o pipefail

# Need to push to legacy prod ecr
legacy_prod_access_key=$bamboo_AWS_PROD_ACCESS_KEY
legacy_prod_secret_key=$bamboo_AWS_PROD_SECRET_ACCESS_KEY
legacy_prod_prefix=$bamboo_PREFIX_PROD
legacy_prod_account_number=$bamboo_ACCOUNT_NUMBER_PROD

# Then need to update consolidated UAT/PROD lambdas
consolidated_access_keys=( $bamboo_AWS_CC_PROD_ACCESS_KEY $bamboo_AWS_CC_UAT_ACCESS_KEY )
consolidated_access_keys_len=${#consolidated_access_keys[@]}
consolidated_secret_keys=( $bamboo_AWS_CC_PROD_SECRET_ACCESS_KEY $bamboo_AWS_CC_UAT_SECRET_ACCESS_KEY )
consolidated_prefixes=( $bamboo_PREFIX_CC_PROD $bamboo_PREFIX_CC_UAT )
consolidated_account_numbers=( $bamboo_ACCOUNT_NUMBER_CC_PROD $bamboo_ACCOUNT_NUMBER_CC_UAT )


function build_docker {
  echo "Building Docker"
  echo $bamboo_GITHUB_REGISTRY_READ_TOKEN_SECRET | docker login ghcr.io -u bamboo_login --password-stdin
  if [[ $(uname -m) == arm64* ]]; then
    docker_build="docker buildx build --push --platform linux/arm64,linux/amd64 -t"
  else
    docker_build="docker build -t"
  fi
  ${docker_build} $1 .

}

function validate_keys {
  export AWS_ACCESS_KEY_ID=$1
  export AWS_SECRET_ACCESS_KEY=$2
  aws sts get-caller-identity
  (($? != 0)) && {
    printf '%s\n' "Command exited with non-zero. AWS keys invalid"
    exit 1
  }
}

# Check legacy PROD keys
validate_keys legacy_prod_access_key legacy_prod_secret_key

# Check consolidated keys
for ((i = 0; i < $consolidated_access_keys_len; i++)); do
  validate_keys ${consolidated_access_keys[$i]} ${consolidated_secret_keys[$i]}
done

build_docker mdx

# Copy test results
docker run --rm -v $PWD/test_results:/opt/mount --entrypoint cp mdx /var/task/test_results/test_metadata_extractor.xml /opt/mount/test_metadata_extractor.xml

function export_keys_push_to_ecr {
  export AWS_ACCESS_KEY_ID=$1
  export AWS_SECRET_ACCESS_KEY=$2
  create_ecr_repo_or_skip
  push_to_ecr legacy_prod_account_number legacy_prod_prefix
}

# Push MDX to PROD ECR
export_keys_push_to_ecr

function update_mdx_lambdas {
  update_lambda_or_skip $1 $2 "mdx_docker_lambda_1g"
  update_lambda_or_skip $1 $2 "mdx_docker_lambda_3g"
}

# For each consolidated account update mdx lambda lambda
for ((i = 0; i < $consolidated_access_keys_len; i++)); do
  export AWS_ACCESS_KEY_ID=${consolidated_access_keys[$i]}
  export AWS_SECRET_ACCESS_KEY=${consolidated_secret_keys[$i]}
  export prefix=${consolidated_prefixes[$i]}

  update_mdx_lambdas legacy_prod_account_number prefix
done

docker rmi mdx
