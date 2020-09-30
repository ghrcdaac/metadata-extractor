#!/bin/bash
REPO_NAME=mdx
docker build -t ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/$REPO_NAME .
$(aws ecr get-login --no-include-email --region ${AWS_REGION} --profile $AWS_PROFILE)
aws ecr create-repository --repository-name $REPO_NAME --profile $AWS_PROFILE --region ${AWS_REGION} || echo "ECR $REPO_NAME exist"
docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/$REPO_NAME