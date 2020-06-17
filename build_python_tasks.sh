#! /bin/bash

LAMBDA_NAME=`basename $PWD`
BUILD_DIR=/tmp/${LAMBDA_NAME}
DESTINATION_DIR=${PWD}/dist
rm -rf dist ${BUILD_DIR}
mkdir ${BUILD_DIR}
pip install -r requirements.txt --target ${BUILD_DIR}
rsync -av --progress --exclude="test" . ${BUILD_DIR}/.
cd ${BUILD_DIR}
chmod -R 755 .
zip -r9 ${LAMBDA_NAME}.zip .
mkdir ${DESTINATION_DIR}
mv ${LAMBDA_NAME}.zip ${DESTINATION_DIR}/.
cd -
rm -rf ${BUILD_DIR}