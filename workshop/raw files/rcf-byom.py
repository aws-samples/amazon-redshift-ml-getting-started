import boto3
import botocore
import sagemaker
from sagemaker import RandomCutForest
import sys
import pandas as pd
import time
import numpy as np
import os
import json

session = sagemaker.Session()

REDSHIFT_IAM_ROLE=os.environ['REDSHIFT_IAM_ROLE']
REDSHIFT_USER=os.environ['REDSHIFT_USER']
REDSHIFT_ENDPOINT = os.environ['REDSHIFT_ENDPOINT']
SAGEMAKER_S3_BUCKET=os.environ['SAGEMAKER_S3_BUCKET']
PREFIX = "sagemaker/random-cut-forest"
execution_role = sagemaker.get_execution_role()

print(f'REDSHIFT_IAM_ROLE:{REDSHIFT_IAM_ROLE},REDSHIFT_USER:{REDSHIFT_USER},REDSHIFT_ENDPOINT:{REDSHIFT_ENDPOINT},SAGEMAKER_S3_BUCKET:{SAGEMAKER_S3_BUCKET}')


def s3_put(script_s3_path, object):
    bucket, key = script_s3_path.replace("s3://", "").split("/", 1)
    boto3.client('s3').put_object(Bucket=bucket, Key=key, Body=object)

def run_sql(sql_text):
    client = boto3.client("redshift-data")
    res = client.execute_statement(Database=REDSHIFT_ENDPOINT.split('/')[1], DbUser=REDSHIFT_USER, Sql=sql_text,
                                   ClusterIdentifier=REDSHIFT_ENDPOINT.split('.')[0])
    query_id = res["Id"]
    done = False
    while not done:
        time.sleep(1)
        status_description = client.describe_statement(Id=query_id)
        status = status_description["Status"]
        if status == "FAILED":
            raise Exception('SQL query failed:' + query_id + ": " + status_description["Error"])
        elif status == "FINISHED":
            if status_description['ResultRows']>0:
                results = client.get_statement_result(Id=query_id)
                metadata=dict()
                column_labels = []
                #dtypes = []
                for i in range(len(results["ColumnMetadata"])): column_labels.append(results["ColumnMetadata"][i]['label'])
                for i in range(len(results["ColumnMetadata"])):
                    if (results["ColumnMetadata"][i]['typeName'])=='varchar':
                        typ='str'
                    elif ((results["ColumnMetadata"][i]['typeName'])=='int4' or (results["ColumnMetadata"][i]['typeName'])=='numeric') :
                        typ='float'
                    else:
                        typ = 'str'
                    metadata[results["ColumnMetadata"][i]['label']]=typ
                    #dtypes.append(typ)


                records = []

                for record in results.get('Records'):
                    records.append([list(rec.values())[0] for rec in record])
                df = pd.DataFrame(np.array(records), columns=column_labels)
                df = df.astype(metadata)
                return df
            else:
                return query_id


model_data = run_sql("""
    select
        age,balance,day,duration,campaign,days_since_last_contact,previous,job_blue_collor,job_entrepreneur,
        job_housemaid,job_management,job_retired,job_self_employed,job_student,job_technician,job_unemployed,job_unknown,
        job_services,month_jan,month_feb,month_mar,month_may,month_jun,month_jul,month_aug,month_sep,month_oct,month_nov,month_dec,
        marital_married,marital_single,education_secondary,
        education_tertiary,education_unknown,default_yes,housing_yes,loan_yes,contact_telephone,contact_unknown
    from
        marketing_campaign_encoded_data;
"""
)

rcf = RandomCutForest(
    role=execution_role,
    instance_count=1,
    instance_type="ml.m4.xlarge",
    data_location=f"s3://{SAGEMAKER_S3_BUCKET}/{PREFIX}/",
    output_path=f"s3://{SAGEMAKER_S3_BUCKET}/{PREFIX}/output",
    num_samples_per_tree=512,
    num_trees=50,
)

newData=model_data.to_numpy()
rcf.fit(rcf.record_set(newData))

print(f"Training job name: {rcf.latest_training_job.job_name}")
rcf_inference = rcf.deploy(initial_instance_count=1, instance_type="ml.m4.xlarge")

endpoint=rcf_inference.endpoint

config = {'REDSHIFT_IAM_ROLE': REDSHIFT_IAM_ROLE,
          'REDSHIFT_USER': REDSHIFT_USER,
          'REDSHIFT_ENDPOINT': REDSHIFT_ENDPOINT,
          'SAGEMAKER_S3_BUCKET': SAGEMAKER_S3_BUCKET,
          'SAGEMAKER_ENDPOINT': endpoint}

s3_put(SAGEMAKER_S3_BUCKET + '/workshop/sagemaker_config.json', json.dumps(config))
s3_put(SAGEMAKER_S3_BUCKET + '/customer_raw_data/', '')

print(f"Endpoint name: {endpoint}")

sql_text=("""
drop model if exists public.marketing_campaign_anomalies;

CREATE MODEL public.marketing_campaign_anomalies
FUNCTION marketing_campaign_anomalies_function (float	,float	,float	,float	,float	,float	,float	,float	,float	,float	,float	,float	,float
,float	,float	,float	,float	,float	,float	,float	,float	,float	,float	,float	,float	,float	,float	,float	,float	,float	,float	,
float	,float	,float	,float	,float	,float	,float	,float	)
RETURNS decimal(10,6)
SAGEMAKER'{}'
IAM_ROLE'{}';

drop view if exists customer_prediction_data;

create or replace view customer_prediction_data as
SELECT
 c.customerid
 ,customer_churn_function(
   c.state
  ,c.zipcode
  ,c.monthlycharges
  ,c.totalcharges
  ,c.customerservicecalls
  ,c.gender
  ,c.seniorcitizen
  ,c.tenuremonths
  ,c.phoneservice
  ,c.multiplelines
  ,c.internetservice
  ,c.onlinesecurity
  ,c.onlinebackup
  ,c.deviceprotection
  ,c.streamingtv
  ,c.streamingmovies
  ,c.contract
  ,c.paperlessbilling
  ,c.paymentmethod
  ) AS churnlabel
 ,marketing_campaign_anomalies_function(
  e.age
 ,e.balance
 ,e.day
 ,e.duration
 ,e.campaign
 ,e.days_since_last_contact
 ,e.previous
 ,e.job_blue_collor
 ,e.job_entrepreneur
 ,e.job_housemaid
 ,e.job_management
 ,e.job_retired
 ,e.job_self_employed
 ,e.job_services
 ,e.job_student
 ,e.job_technician
 ,e.job_unemployed
 ,e.job_unknown
 ,e.month_aug
 ,e.month_dec
 ,e.month_feb
 ,e.month_jan
 ,e.month_jul
 ,e.month_jun
 ,e.month_mar
 ,e.month_may
 ,e.month_nov
 ,e.month_oct
 ,e.month_sep
 ,e.marital_married
 ,e.marital_single
 ,e.education_secondary
 ,e.education_tertiary
 ,e.education_unknown
 ,e.default_yes
 ,e.housing_yes
 ,e.loan_yes
 ,e.contact_telephone
 ,e.contact_unknown) AS anomaly_score
,c.record_date
,c.monthlycharges
,c.totalcharges
,c.customerservicecalls
,c.state
,c.city
,c.zipcode
,c.latitude
,c.longitude
,c.gender
,c.seniorcitizen
,c.partner
,c.dependents
,c.tenuremonths
,c.phoneservice
,c.multiplelines
,c.internetservice
,c.onlinesecurity
,c.onlinebackup
,c.deviceprotection
,c.techsupport
,c.streamingtv
,c.streamingmovies
,c.contract
,c.paperlessbilling
,c.paymentmethod
,c.cltv
,m.age
,m.job
,m.marital
,m.education
,m.is_default
,m.balance
,m.housing
,m.loan
,m.contact
,m.day
,m.month
,m.duration
,m.campaign
,m.days_since_last_contact
,m.previous
,m.poutcome
,m.accepted
FROM   customer_raw_data c
JOIN marketing_campaign_raw_data m ON c.customerid = m.customerid
JOIN marketing_campaign_encoded_data e ON c.customerid = e.customerid;

""")
sql_text=sql_text.format(endpoint,REDSHIFT_IAM_ROLE)
print(sql_text)
df=run_sql(sql_text)
print(df)
