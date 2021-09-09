/* DISCLAIMER: Please replace <your-amazon-redshift-sagemaker-iam-role-arn> with the IAM role ARN of your Amazon Redshift cluster in the SQL scripts below


/* Data Preparation */

DROP TABLE IF EXISTS public.rcf_taxi_data CASCADE;

CREATE TABLE public.rcf_taxi_data
(
ride_timestamp timestamp,
nbr_passengers int
);

COPY public.rcf_taxi_data
FROM 's3://sagemaker-sample-files/datasets/tabular/anomaly_benchmark_taxi/NAB_nyc_taxi.csv'
iam_role 'arn:aws:iam:::<accountid>:role/RedshiftML' ignoreheader 1 csv delimiter ',';


