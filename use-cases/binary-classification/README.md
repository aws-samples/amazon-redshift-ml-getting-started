# Binary Classification

**Note**: The contents of this folder was taken from the AWS Blog Post: [Create, train, and deploy machine learning models in Amazon Redshift using SQL with Amazon Redshift ML](https://aws.amazon.com/blogs/big-data/create-train-and-deploy-machine-learning-models-in-amazon-redshift-using-sql-with-amazon-redshift-ml/).

## Introduction

In this folder, we have a working example to create models in Amazon Redshift using the binary classification problem type, which consists of predicting a true/false binary outcome. For example, you can predict customer churn.


## Prerequisites

Please check out [Prerequisites for enabling Amazon Redshift ML](https://aws.amazon.com/blogs/big-data/create-train-and-deploy-machine-learning-models-in-amazon-redshift-using-sql-with-amazon-redshift-ml/) for step-by-step instructions.

Use the [Cloudformation template](../cloud-formation/cloud-formation-template.yaml) to setup your Amazon Redshift cluster with Redshift ML or create an IAM role allowing Amazon Redshift to communicate with Amazon SageMaker.   

## Use Case

For our use case, we want to predict the likelihood of customers' churn. We use Amazon Redshift ML and binary classification to predict that.


## Using Amazon SageMaker Notebook

You may follow the step-by-step instructions in the [binary-classification notebook](./binary-classification.ipynb) for this setup which is auto-provisioned in the [Cloudformation template](../cloud-formation/cloud-formation-template.yaml).

## Data Preparation

Please use the the [data preparation SQL file](./data_preparation.sql) to populate the input tables for this example setup.

## Create Model

The `CREATE MODEL` statement offers flexibility in the number of parameters used to create the model. Depending on their needs or problem type, users can choose their preferred preprocessors, algorithms, problem types, or hyperparameters.

In this example, the training data provides features regarding state, area code, average daily spend, and average daily cases for the customers that have been active earlier than January 1, 2020. The target column `churn` indicates whether the customer still has an active membership or has suspended their membership.


```SQL
CREATE MODEL customer_churn_model
  FROM (SELECT state,
               area_code,
               total_charge/account_length AS average_daily_spend,
               cust_serv_calls/account_length AS average_daily_cases,
               churn
          FROM customer_activity
         WHERE record_date < '2020-01-01'
     )
TARGET churn
FUNCTION predict_customer_churn
IAM_ROLE '<<your-amazon-redshift-sagemaker-iam-role-arn>>'
SETTINGS (
  S3_BUCKET '<<your-amazon-s3-bucket-name>>'
);
```


The `SELECT` query in the `FROM` clause specifies the training data. The `TARGET` clause specifies which column is the label that the `CREATE MODEL` builds a model to predict. The other columns in the training query are the features (input) used for the prediction.

For more information about CREATE MODEL syntax, see the [Amazon Redshift Database Developer Guide](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_MODEL.html).

## Evaluation


You can use the following example SQL query as an illustration to see which predictions are incorrect based on the ground truth:

```sql
 WITH infer_data AS (
      SELECT area_code ||phone  accountid, churn,
             predict_customer_churn(
             state,
             area_code,
             total_charge/account_length ,
             cust_serv_calls/account_length )
          AS predicted
 FROM customer_activity
WHERE record_date <  '2020-01-01'
)

SELECT * FROM infer_data where churn!=predicted;
```

You can see the F1 value for the example model `customer_churn_model` in the output of the SHOW MODEL command. The F1 amount signifies the statistical measure of the precision and recall of all the classes in the model. The value ranges between 0–1; the higher the score, the better the accuracy of the model.


```sql
SHOW MODEL customer_churn_model;
```

## ML Predictions

You can use your SQL function to apply the ML model to your data in queries, reports, and dashboards. For example, you can run the `predict_customer_churn` SQL function on new customer data in Amazon Redshift regularly to predict customers at risk of churning and feed this information to sales and marketing teams so they can take preemptive actions, such as sending these customers an offer designed to retain them.

```sql
SELECT area_code || phone accountid,
       predict_customer_churn(
          state,
          area_code,
          total_charge/account_length ,
          cust_serv_calls/account_length )
          AS "predictedActive"
  FROM customer_activity
 WHERE area_code='408' and record_date > '2020-01-01';
```
