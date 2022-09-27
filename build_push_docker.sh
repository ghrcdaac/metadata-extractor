#!/bin/bash
source ./common.sh


function check_exit {
EXIT_STATUS=$?
if [ $EXIT_STATUS -ne 0 ]; then
   exit $EXIT_STATUS
fi
}


read -rp 'AWS_PROFILE: ' AWS_PROFILE
read -rp 'Stack Prefix: ' STACK_PREFIX
AWS_ACCOUNT_ID=$(get_account_id $AWS_PROFILE)


docker build -t ghcr.io/ghrcdaac/mdx:base ./mdx_base
docker build -t ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/$REPO_NAME .
check_exit

update_lambda_or_skip $AWS_ACCOUNT_ID $STACK_PREFIX

check_exit
