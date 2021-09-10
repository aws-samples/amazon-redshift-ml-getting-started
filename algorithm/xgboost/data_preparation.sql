/* Data preparation steps for XGBoost model creation in Redshift ML */

/* DISCLAIMER: Please replace <your-amazon-redshift-sagemaker-iam-role-arn> with the IAM role ARN of your Amazon Redshift cluster in the SQL scripts below


/* Data Preparation */

--train table 
CREATE TABLE banknoteauthentication_train(
variance FLOAT,
skewness FLOAT,
curtosis FLOAT,
entrophy FLOAT,
class INT);

--Load 
COPY banknoteauthentication_train FROM 's3://redshiftbucket-ml-sagemaker/banknote_authentication/train_data/' IAM_ROLE '<your-amazon-redshift-sagemaker-iam-role-arn>' REGION 'us-west-2' IGNOREHEADER 1 CSV;

--test table 
CREATE TABLE banknoteauthentication_test(
variance FLOAT,
skewness FLOAT,
curtosis FLOAT,
entrophy FLOAT,
class INT);

--Load 
COPY banknoteauthentication_test FROM 's3://redshiftbucket-ml-sagemaker/banknote_authentication/test_data/' IAM_ROLE '<your-amazon-redshift-sagemaker-iam-role-arn>' REGION 'us-west-2' IGNOREHEADER 1 CSV;



