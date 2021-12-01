drop table if exists marketing_campaign_raw_data cascade;

CREATE TABLE marketing_campaign_raw_data(
customerid varchar(100),
serialnumber integer,
age numeric,
job varchar,
marital varchar,
education varchar,
is_default varchar,
balance integer,
housing varchar,
loan varchar,
contact varchar,
day integer,
month varchar,
duration numeric,
campaign numeric,
days_since_last_contact numeric,
previous numeric,
poutcome varchar(100),
accepted varchar(10)  ) ;

COPY marketing_campaign_raw_data
FROM 's3://{redshift_s3_bucket}/workshop/marketing_campaign_raw_data/'
IAM_ROLE '{redshift_iam_role}'
CSV IGNOREHEADER 1 delimiter ',' gzip;

commit;

DROP VIEW if exists marketing_campaign_encoded_data;

create or replace view marketing_campaign_encoded_data
as select
customerid,age, balance, day	,duration	,campaign	,days_since_last_contact,previous,
case when job ='blue-collar' then 1 else 0 end as job_blue_collor,
case when job ='entrepreneur' then 1 else 0 end as job_entrepreneur,
case when job ='housemaid' then 1 else 0 end as job_housemaid,
case when job ='management' then 1 else 0 end as job_management,
case when job ='retired' then 1 else 0  end as job_retired,
case when job ='self-employed' then 1 else 0  end as job_self_employed,
case when job ='student'then 1 else 0  end as job_student,
case when job ='technician' then 1 else 0  end as job_technician,
 case when job ='unemployed' then 1 else 0 end as job_unemployed,
case when job ='unknown' then 1 else 0 end as job_unknown,
case when job ='services' then 1 else 0  end as job_services,
case when month='jan' Then 1 else 0 end as month_jan,
case when month='feb' Then 1 else 0 end as month_feb,
case when month='mar' Then 1 else 0 end as month_mar,
case when month='apr' Then 1 else 0 end as month_apr,
case when month='may' Then 1 else 0 end as month_may,
case when month='jun' Then 1 else 0 end as month_jun,
case when month='jul' Then 1 else 0 end as month_jul,
case when month='aug' Then 1 else 0 end as month_aug,
case when month='sep' Then 1 else 0 end as month_sep,
case when month='oct' Then 1 else 0 end as month_oct,
case when month='nov' Then 1 else 0 end as month_nov,
case when month='dec' Then 1 else 0 end as month_dec,
case when marital='married' Then 1 else 0 end as marital_married,
case when marital='single' Then 1 else 0 end as marital_single,
case when education='secondary' Then 1 else 0 end as education_secondary,
case when education='tertiary' Then 1 else 0 end as education_tertiary,
case when education='unknown' Then 1 else 0 end as education_unknown,
case when is_default='yes' Then 1 else 0 end as default_yes,
case when housing='yes' Then 1 else 0 end as housing_yes,
case when loan='yes' Then 1 else 0 end as loan_yes,
case when contact='telephone' Then 1 else 0 end as contact_telephone,
case when contact='unknown' Then 1 else 0 end as contact_unknown
from marketing_campaign_raw_data;

drop table if exists customer;

CREATE TABLE customer (
serialnumber bigint,
customerid varchar(256),
record_date date,
monthlycharges numeric(18,8),
totalcharges numeric(18,8),
customerservicecalls integer,
state varchar(256),
city varchar(256),
zipcode integer,
latitude numeric(18,8),
longitude numeric(18,8),
gender varchar(256),
seniorcitizen varchar(256),
partner varchar(256),
dependents varchar(256),
tenuremonths varchar(256),
phoneservice varchar(256),
multiplelines varchar(256),
internetservice varchar(256),
onlinesecurity varchar(256),
onlinebackup varchar(256),
deviceprotection varchar(256),
techsupport varchar(256),
streamingtv varchar(256),
streamingmovies varchar(256),
contract varchar(256),
paperlessbilling varchar(256),
paymentmethod varchar(256),
cltv integer,
churnreason varchar(256),
churnlabel varchar(256));

copy customer
 from 's3://{redshift_s3_bucket}/workshop/customers/'
 iam_role '{redshift_iam_role}'
 DELIMITER ',' CSV gzip;


drop table if exists customer_raw_data;

CREATE TABLE customer_raw_data (
customerid varchar(256),
record_date date,
monthlycharges numeric(18,8),
totalcharges numeric(18,8),
customerservicecalls integer,
state varchar(256),
city varchar(256),
zipcode integer,
latitude numeric(18,8),
longitude numeric(18,8),
gender varchar(256),
seniorcitizen varchar(256),
partner varchar(256),
dependents varchar(256),
tenuremonths varchar(256),
phoneservice varchar(256),
multiplelines varchar(256),
internetservice varchar(256),
onlinesecurity varchar(256),
onlinebackup varchar(256),
deviceprotection varchar(256),
techsupport varchar(256),
streamingtv varchar(256),
streamingmovies varchar(256),
contract varchar(256),
paperlessbilling varchar(256),
paymentmethod varchar(256),
cltv integer);

commit;

drop table if exists training cascade;

create table training as
  select * from customer where mod(serialnumber,10) < 8 ;

drop table if exists validation cascade;

create table validation as
  select * from customer where mod(serialnumber,10) >= 8;

commit;

CREATE MODEL customer_churn
FROM (
  SELECT STATE
  ,zipcode
  ,monthlycharges
  ,totalcharges
  ,customerservicecalls
  ,gender
  ,seniorcitizen
  ,tenuremonths
  ,phoneservice
  ,multiplelines
  ,internetservice
  ,onlinesecurity
  ,onlinebackup
  ,deviceprotection
  ,streamingtv
  ,streamingmovies
  ,contract
  ,paperlessbilling
  ,paymentmethod
  ,churnlabel
	FROM training
) TARGET churnlabel
FUNCTION customer_churn_function
IAM_ROLE '{redshift_iam_role}'
SETTINGS (
  S3_BUCKET '{redshift_s3_bucket}',
            s3_garbage_collect off,
            max_runtime 9999);
