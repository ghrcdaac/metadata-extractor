#!/bin/bash
source ./common.sh
set -o nounset
set -o pipefail


access_keys=( $bamboo_AWS_SBX_ACCESS_KEY $bamboo_AWS_SIT_ACCESS_KEY $bamboo_AWS_UAT_ACCESS_KEY $bamboo_AWS_PROD_ACCESS_KEY )
secret_keys=( $bamboo_AWS_SBX_SECRET_ACCESS_KEY $bamboo_AWS_SIT_SECRET_ACCESS_KEY $bamboo_AWS_UAT_SECRET_ACCESS_KEY $bamboo_AWS_PROD_SECRET_ACCESS_KEY )
prefixes=( $bamboo_PREFIX_SBX $bamboo_PREFIX_SIT $bamboo_PREFIX_UAT $bamboo_PREFIX_PROD )
account_numbers=( $bamboo_ACCOUNT_NUMBER_SBX $bamboo_ACCOUNT_NUMBER_SIT $bamboo_ACCOUNT_NUMBER_UAT $bamboo_ACCOUNT_NUMBER_PROD )


function build_docker {
echo $bamboo_GITHUB_REGISTRY_READ_TOKEN_SECRET | docker login ghcr.io -u bamboo_login --password-stdin
if [[ $(uname -m) == arm64* ]]; then
  docker_build="docker buildx build --push --platform linux/arm64,linux/amd64 -t"
else
 docker_build="docker build -t"
fi
${docker_build} $1 .

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

