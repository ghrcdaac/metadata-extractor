#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail
export REPO_NAME=mdx
export AWS_REGION=$bamboo_AWS_REGION
#access_keys=( $bamboo_AWS_SIT_ACCESS_KEY $bamboo_AWS_UAT_ACCESS_KEY $bamboo_AWS_PROD_ACCESS_KEY )
#secret_keys=( $bamboo_AWS_SIT_SECRET_ACCESS_KEY $bamboo_AWS_UAT_SECRET_ACCESS_KEY $bamboo_AWS_PROD_SECRET_ACCESS_KEY )
#prefixes=( $bamboo_PREFIX_SIT $bamboo_PREFIX_UAT $bamboo_PREFIX_PROD )
#account_numbers=( $bamboo_ACCOUNT_NUMBER_SIT $bamboo_ACCOUNT_NUMBER_UAT $bamboo_ACCOUNT_NUMBER_PROD )

access_keys=( $bamboo_AWS_SIT_ACCESS_KEY $bamboo_AWS_UAT_ACCESS_KEY )
secret_keys=( $bamboo_AWS_SIT_SECRET_ACCESS_KEY $bamboo_AWS_UAT_SECRET_ACCESS_KEY)
prefixes=( $bamboo_PREFIX_SIT $bamboo_PREFIX_UAT)
account_numbers=( $bamboo_ACCOUNT_NUMBER_SIT $bamboo_ACCOUNT_NUMBER_UAT)

function stop_mdx_task() {
  task_arns=$(aws ecs list-tasks --cluster $1-CumulusECSCluster --family $1-MDX --query "taskArns[*]" --region $AWS_REGION | tr -d '"[],')
  for task in $task_arns
  do
          aws ecs stop-task --cluster $1-CumulusECSCluster --task $task --region $AWS_REGION
  done
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

# Copy test results
docker run --rm -v $PWD/test_results:/opt/mount --entrypoint cp  mdx  /build/test_results/test_metadata_extractor.xml  /opt/mount/test_metadata_extractor.xml

for (( i=0; i<$len; i++ ))
do
 	export AWS_ACCESS_KEY_ID=${access_keys[$i]}
	export AWS_SECRET_ACCESS_KEY=${secret_keys[$i]}
	export ACCOUNT_NUMBER=${account_numbers[$i]}
	export prefix=${prefixes[$i]}

docker_image_name=${ACCOUNT_NUMBER}.dkr.ecr.${AWS_REGION}.amazonaws.com/$REPO_NAME
docker tag mdx $docker_image_name

aws ecr get-login-password \
    --region $AWS_REGION \
| docker login \
    --username AWS \
    --password-stdin $ACCOUNT_NUMBER.dkr.ecr.$AWS_REGION.amazonaws.com

#aws ecr create-repository --repository-name $REPO_NAME 2> /dev/null
echo "pushing image to ecr"
docker push $docker_image_name

stop_mdx_task $prefix

docker rmi $docker_image_name


done

docker rmi mdx




