/* DISCLAIMER: Please replace <your-amazon-redshift-sagemaker-iam-role-arn> with the IAM role ARN of your Amazon Redshift cluster in the SQL scripts below


/* Data Preparation */

CREATE TABLE customer_activity (
state varchar(2), 
account_length int, 
area_code int,
phone varchar(8), 
intl_plan varchar(3), 
vMail_plan varchar(3),
vMail_message int, 
day_mins float, 
day_calls int, 
day_charge float,
total_charge float,
eve_mins float, 
eve_calls int, 
eve_charge float, 
night_mins float,
night_calls int, 
night_charge float, 
intl_mins float, 
intl_calls int,
intl_charge float, 
cust_serv_calls int, 
churn varchar(6),
record_date date);


COPY customer_activity 
FROM 's3://redshift-downloads/redshift-ml/customer_activity/' 
IAM_ROLE '<your-amazon-redshift-sagemaker-iam-role-arn>' delimiter ',' IGNOREHEADER 1  
region 'us-east-1';


