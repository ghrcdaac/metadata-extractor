#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail
export REPO_NAME=mdx
export AWS_REGION=$bamboo_AWS_REGION
export S3_KEY_PATH=$bamboo_S3_KEY_PATH
export LAMBDA_BASE_NAME=$bamboo_LAMBDA_BASE_NAME
# access_keys=( $bamboo_AWS_SBX_ACCESS_KEY $bamboo_AWS_SIT_ACCESS_KEY $bamboo_AWS_UAT_ACCESS_KEY $bamboo_AWS_PROD_ACCESS_KEY )
# secret_keys=( $bamboo_AWS_SBX_SECRET_ACCESS_KEY $bamboo_AWS_SIT_SECRET_ACCESS_KEY $bamboo_AWS_UAT_SECRET_ACCESS_KEY $bamboo_AWS_PROD_SECRET_ACCESS_KEY )
# prefixes=( $bamboo_PREFIX_SBX $bamboo_PREFIX_SIT $bamboo_PREFIX_UAT $bamboo_PREFIX_PROD )
# account_numbers=( $bamboo_ACCOUNT_NUMBER_SBX $bamboo_ACCOUNT_NUMBER_SIT $bamboo_ACCOUNT_NUMBER_UAT $bamboo_ACCOUNT_NUMBER_PROD )

access_keys=( $bamboo_AWS_SBX_ACCESS_KEY  )
secret_keys=( $bamboo_AWS_SBX_SECRET_ACCESS_KEY )
prefixes=( $bamboo_PREFIX_SBX )
account_numbers=( $bamboo_ACCOUNT_NUMBER_SBX )


function stop_mdx_task() {
  task_arns=$(aws ecs list-tasks --cluster $1-CumulusECSCluster --family $1-MDX --query "taskArns[*]" --region $AWS_REGION | tr -d '"[],')
  for task in $task_arns
  do
    aws ecs stop-task --cluster $1-CumulusECSCluster --task $task --region $AWS_REGION
  done
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

  #aws ecr create-repository --repository-name $REPO_NAME 2> /dev/null
  echo "pushing image to ecr"
  docker push $docker_image_name

  stop_mdx_task $2

  docker rmi $docker_image_name
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

docker build -t mdx .
#curl -L --output /tmp/mdx_lambda_artifact.zip --header "PRIVATE-TOKEN: $bamboo_GIT_TOKEN_SECRET" "https://gitlab.com/api/v4/projects/ghrc-cloud%2Fmetadata-extractor/jobs/artifacts/master/raw/mdx_lambda_artifact.zip?job=BuildArtifact"
python3 create-artifact.py
# Copy test results
docker run --rm -v $PWD/test_results:/opt/mount --entrypoint cp  mdx  /build/test_results/test_metadata_extractor.xml  /opt/mount/test_metadata_extractor.xml

for (( i=0; i<$len; i++ ))
do
  export AWS_ACCESS_KEY_ID=${access_keys[$i]}
  export AWS_SECRET_ACCESS_KEY=${secret_keys[$i]}
  export ACCOUNT_NUMBER=${account_numbers[$i]}
  export prefix=${prefixes[$i]}

#   # Push mdx to all account's ecr
#   push_to_ecr $ACCOUNT_NUMBER $prefix

#   # Push mdx artifact to all account's s3
#   aws s3 cp ./mdx_lambda_artifact.zip s3://$prefix-internal/$prefix/$S3_KEY_PATH --region $AWS_REGION

#   # Update mdx lambda source unless env is prod or uat
#  aws lambda update-function-code \
#   --function-name $prefix-$LAMBDA_BASE_NAME \
#   --s3-bucket $prefix-internal \
#   --s3-key $prefix/$S3_KEY_PATH \
#   --region $AWS_REGION

aws ecr get-login-password \
      --region $AWS_REGION \
  | docker login \
      --username AWS \
      --password-stdin $1.dkr.ecr.$AWS_REGION.amazonaws.com
docker pull 322322076095.dkr.ecr.us-west-2.amazonaws.com/mdx
docker push 322322076095.dkr.ecr.us-west-2.amazonaws.com/mdx

done

# docker rmi mdx
# rm -rf ./mdx_lambda_artifact.zip


