#!/bin/sh
set -eu
yum install -y awscli python3
pip3 install boto3 psycopg2-binary pandas sqlalchemy sagemaker numpy

aws s3 cp "$PYTHON_SCRIPT" ./script.py
python3 ./script.py
