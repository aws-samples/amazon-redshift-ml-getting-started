# Multi-Class Classification

**Note**: The contents of this folder was taken from the AWS Blog Post: [Build multi-class classification models with Amazon Redshift ML](https://aws.amazon.com/blogs/machine-learning/build-multi-class-classification-models-with-amazon-redshift-ml/).


## Introduction

In this folder, we have a working example to create models in Amazon Redshift using the multi-class classification problem type, which consists on classifying instances into one of three or more classes. For example, you can predict whether a transaction is fraudulent, failed or successful, whether a customer will remain active for 3 months, six months, nine months, 12 months, or whether a news is tagged as sports, world news, business.

## Pre-requisites

Use the [Cloudformation template](../cloud-formation/cloud-formation-template.yaml) to setup your Amazon Redshift cluster with Redshift ML or create an IAM role allowing Amazon Redshift to communicate with Amazon SageMaker.   

## Use Case

For our use case, we want to target our most active customers for a special customer loyalty program. We use Amazon Redshift ML and multi-class classification to predict how many months a customer will be active over a 13-month period. This translates into up to 13 possible classes, which makes this a better fit for multi-class classification. Customers with predicted activity of 7 months or greater are targeted for a special customer loyalty program.

### Using Amazon SageMaker Notebook

You may follow the step-by-step instructions in the [multi-class-classification notebook](./multi-class-classification.ipynb) for this setup which is auto-provisioned in the [Cloudformation template](../cloud-formation/cloud-formation-template.yaml).

## Data Preparation

Please use the the [data preparation SQL file](./data_preparation.sql) to populate the input tables for this example setup.

## Create Model

We can use the create model statement in Amazon Redshift to create our ML model using Multiclass_Classification. We specify the problem type but we let AutoML take care of everything else. In this model, the target we want to predict is `nbr_months_active`. Amazon SageMaker creates the function predict_customer_activity, which we use to do inference in Amazon Redshift. See the following code:

```sql
create model ecommerce_customer_activity
from
  (
select   
  customerid,
  country,
  stockcode,
  description,
  invoicedate,
  sales_amt,
  nbr_months_active  
 from ecommerce_sales_training)
 TARGET nbr_months_active FUNCTION predict_customer_activity
 IAM_ROLE '<<your-amazon-redshift-sagemaker-iam-role-arn>>''
 problem_type MULTICLASS_CLASSIFICATION  
  SETTINGS (
    S3_BUCKET '<<your-amazon-s3-bucket-name>>',
    S3_GARBAGE_COLLECT OFF
  );
  ```

 ## Evaluation

 In this step, we evaluate the accuracy of our ML model against our validation data.

 While creating the model, Amazon SageMaker Autopilot automatically splits the input data into train and validation sets, and selects the model with the best objective metric, which is deployed in the Amazon Redshift cluster. You can use the show model statement in your cluster to view various metrics, including the accuracy score. If you don’t specify explicitly, SageMaker automatically uses accuracy for the objective type. See the following code:

 Show model ecommerce_customer_activity;

 You can see the validation:accuracy value in the output of the SHOW MODEL command. A value closer to 100 is better.

 Run the inference query:

```sql
 select
 cast(sum(t1.match)as decimal(7,2)) as predicted_matches
,cast(sum(t1.nonmatch) as decimal(7,2)) as predicted_non_matches
,cast(sum(t1.match + t1.nonmatch) as decimal(7,2))  as total_predictions
,predicted_matches / total_predictions as pct_accuracy
from
(select   
  customerid,
  country,
  stockcode,
  description,
  invoicedate,
  sales_amt,
  nbr_months_active,
  predict_customer_activity(customerid, country, stockcode, description, invoicedate, sales_amt) as predicted_months_active,
  case when nbr_months_active = predicted_months_active then 1
      else 0 end as match,
  case when nbr_months_active <> predicted_months_active then 1
    else 0 end as nonmatch
  from ecommerce_sales_validation
  )t1;
  ```

 ## ML Predictions

 Let’s run a query to see which customers qualify for our customer loyalty program by being active for at least 7 months:
```sql
 select
  customerid,  
  predict_customer_activity(customerid, country, stockcode, description, invoicedate, sales_amt) as predicted_months_active
  from ecommerce_sales_prediction
  where predicted_months_active >=7
  group by 1,2
 limit 10;
```
