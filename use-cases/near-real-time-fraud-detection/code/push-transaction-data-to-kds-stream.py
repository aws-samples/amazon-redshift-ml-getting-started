from datetime import datetime
import calendar
import time
import json
import sys
import shutil
import os
import pandas as pd
import boto3

##################
#Input start
source_folder_name='/home/ec2-user/kds-stream/source'
archive_folder_name='/home/ec2-user/kds-stream/source_archive'
kinesis_client = boto3.client('kinesis', region_name='us-west-2')
stream_name = 'blog-customer-payment-stream'
#Input end
##################

push_to_kinesis = []

record_count = 0
jsondata = []

starttime = datetime.now()

for obj in os.listdir(source_folder_name):
    if obj.endswith(".csv") :
        keys = []

        ##Get the list of files and create a key
        keys.append(obj)

        ##Use pandas to read csv
        source_file_path = source_folder_name+'/'+obj
        archive_file_path = source_folder_name+'/'+obj
        df = pd.read_csv(source_file_path)

        ##Make JSON records out of the pandas data frame
        jsondata = df.to_json(orient='records')
        jsonconverted = json.loads(jsondata)

        push_to_kinesis = []
        record_count = 0

        ##Push 500 batched records to Kinesis data stream for every file
        for everyrecord in jsonconverted:
            push_to_kinesis.append(
                {'Data': json.dumps(everyrecord), 'PartitionKey': str(hash(everyrecord['CUSTOMER_ID']))})
            record_count += 1
            if record_count >= 500:
                kinesis_client.put_records(Records=push_to_kinesis, StreamName=stream_name)
                push_to_kinesis = []
                time.sleep(1)
                record_count = 0
        kinesis_client.put_records(Records=push_to_kinesis, StreamName=stream_name)

        ##Archive the file and delete from the source if processed
        shutil.move(source_file_path, archive_folder_name)

        currenttime = datetime.now()
        totalruntime = (currenttime - starttime).days * 24 * 60
