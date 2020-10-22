#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail
export REPO_NAME=mdx
export AWS_REGION=$bamboo_AWS_REGION
access_keys=( $bamboo_ACCESS_KEY_SIT $bamboo_ACCESS_KEY_UAT $bamboo_ACCESS_KEY_PROD)
secret_keys=( $bamboo_SECRET_KEY_SIT $bamboo_SECRET_KEY_UAT $bamboo_SECRET_KEY_PROD)
prefixes=( $bamboo_PREFIX_SIT $bamboo_PREFIX_UAT $bamboo_PREFIX_PROD)
account_numbers=( $bamboo_ACCOUNT_NUMBER_SIT $bamboo_ACCOUNT_NUMBER_UAT $bamboo_ACCOUNT_NUMBER_PROD )


#access_keys=( $bamboo_ACCESS_KEY_SBX )
#secret_keys=( $bamboo_SECRET_KEY_SBX)
#account_numbers=( $bamboo_ACCOUNT_NUMBER_SBX )
#prefixes=( $bamboo_PREFIX_SBX )

function stop_mdx_task() {
  task_arn=$(./aws ecs list-tasks --cluster $1-CumulusECSCluster --service-name $1-MDX --query "taskArns[0]" --region $AWS_REGION | tr -d '"')
  ./aws ecs stop-task --cluster $1-CumulusECSCluster --task $task_arn --region $AWS_REGION
}


len=${#access_keys[@]}



docker build -t mdx .

# Copy test results
docker run --rm -v $PWD/test_results:/opt/mount --entrypoint cp  mdx  /build/test_results/test_metadata_extractor.xml  /opt/mount/test_metadata_extractor.xml

for (( i=0; i<$len; i++ ))
do
 	export AWS_ACCESS_KEY_ID=${access_keys[$i]}
	export AWS_SECRET_ACCESS_KEY=${secret_keys[$i]}
	export ACCOUNT_NUMBER=${account_numbers[$i]}
	export prefix=${prefixes[$i]}
  cat > aws <<EOS
#!/usr/bin/env bash
set -o errexit
set -o nounset
set -o pipefail
# enable interruption signal handling
trap - INT TERM
docker run --rm \
	-e "AWS_ACCESS_KEY_ID=\${AWS_ACCESS_KEY_ID}" \
	-e "AWS_SECRET_ACCESS_KEY=\${AWS_SECRET_ACCESS_KEY}" \
	-e "AWS_DEFAULT_REGION=\${AWS_REGION}" \
	-v "\$(pwd):/project" \
	amazon/aws-cli:2.0.58  \
	"\$@"
EOS
chmod a+x aws
docker_image_name=${ACCOUNT_NUMBER}.dkr.ecr.${AWS_REGION}.amazonaws.com/$REPO_NAME
docker tag mdx $docker_image_name

./aws ecr get-login-password \
    --region $AWS_REGION \
| docker login \
    --username AWS \
    --password-stdin $ACCOUNT_NUMBER.dkr.ecr.$AWS_REGION.amazonaws.com

#./aws ecr create-repository --repository-name $REPO_NAME 2> /dev/null
echo "pushing image to ecr"
docker push $docker_image_name

stop_mdx_task $prefix

docker rmi $docker_image_name
rm ./aws


done

docker rmi mdx




