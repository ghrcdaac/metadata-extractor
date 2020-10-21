#!/bin/bash

export REPO_NAME=mdx
export AWS_REGION=$bamboo_AWS_REGION
#access_keys=( $bamboo_ACCESS_KEY_SIT $bamboo_ACCESS_KEY_UAT $bamboo_ACCESS_KEY_PROD)
#secret_keys=( $bamboo_SECRET_KEY_SIT $bamboo_SECRET_KEY_UAT $bamboo_SECRET_KEY_PROD)
#prefixes=( $bamboo_PREFIX_SIT $bamboo_PREFIX_UAT $bamboo_PREFIX_PROD)
#account_numbers=( $bamboo_ACCOUNT_NUMBER_SIT $bamboo_ACCOUNT_NUMBER_UAT $bamboo_ACCOUNT_NUMBER_PROD )


access_keys=( $bamboo_ACCESS_KEY_SBX )
secret_keys=( $bamboo_SECRET_KEY_SBX)
account_numbers=( $bamboo_ACCOUNT_NUMBER_SBX )
prefixes=( $bamboo_PREFIX_SBX )

function stop_mdx_task() {
  task_arn=$(./aws ecs list-tasks --cluster $1-CumulusECSCluster --service-name $1-MDX --query "taskArns[0]" --region $AWS_REGION | tr -d '"')
  IFS='/' read -ra ADDR <<< "arn:aws:ecs:us-west-2:322322076095:task/263b89b5-7696-4c0b-9315-a39ea3182476"
  for i in "${ADDR[@]}"; do
    task_id=$i
  done
 ./aws ecs stop-task --cluster $1-CumulusECSCluster --task $task_id --region $AWS_REGION

}


len=${#access_keys[@]}

function check_exit {
EXIT_STATUS=$?
if [ $EXIT_STATUS -ne 0 ]; then
   exit $EXIT_STATUS
fi
}

docker build -t mdx .
check_exit
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
	-t \$(tty &>/dev/null && echo "-i") \
	-e "AWS_ACCESS_KEY_ID=\${AWS_ACCESS_KEY_ID}" \
	-e "AWS_SECRET_ACCESS_KEY=\${AWS_SECRET_ACCESS_KEY}" \
	-e "AWS_DEFAULT_REGION=\${AWS_REGION}" \
	-v "\$(pwd):/project" \
	maven.earthdata.nasa.gov/aws-cli \
	"\$@"
EOS
chmod a+x aws
docker_image_name=${ACCOUNT_NUMBER}.dkr.ecr.${AWS_REGION}.amazonaws.com/$REPO_NAME
docker tag mdx $docker_image_name
check_exit
ECR=$(./aws ecr get-login --no-include-email --region ${AWS_REGION})
#./aws ecr create-repository --repository-name $REPO_NAME 2> /dev/null
echo "creating login temp file"
_ECR=$(echo ${ECR} | tr -d '\r')
echo ${_ECR} > ecr.out
echo "login into ecr"
$(cat ecr.out)
echo "pushing image to ecr"
docker push $docker_image_name
check_exit
stop_mdx_task $prefix
check_exit
echo "removing temp file"
rm ecr.out
docker rmi $docker_image_name
check_exit

done

docker rmi mdx
check_exit



