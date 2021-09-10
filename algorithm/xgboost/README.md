# XGBoost Algorithm

**Note**: The contents of this folder was taken from the AWS Blog Post: [Build XGBoost models with Amazon Redshift ML](https://aws.amazon.com/blogs/machine-learning/build-xgboost-models-with-amazon-redshift-ml/).


## Introduction

In this folder, we have a working example to create models in Amazon Redshift using the [XGBoost](https://docs.aws.amazon.com/sagemaker/latest/dg/xgboost.html) algorithm type. The XGBoost algorithm (eXtreme Gradient Boosting) is an optimized open-source implementation of the gradient boosted trees algorithm. XGBoost is designed from the ground up to handle many data science problems in a highly efficient, flexible, portable, and accurate way. XGBoost algorithm can be used for regression, binary classification, multi-class classification, and ranking problems.

## Pre-requisites

Use the [Cloudformation template](../cloud-formation/cloud-formation-template.yaml) to setup your Amazon Redshift cluster with Redshift ML or create an IAM role allowing Amazon Redshift to communicate with Amazon SageMaker.   

## Use Case

To illustrate XGBoost algorithm, we use the [banknote authentication dataset](https://archive.ics.uci.edu/ml/datasets/banknote+authentication), which is a  binary classification problem to predict whether a given banknote is genuine or forged. Image recognition and the Wavelet Transform tool were used to obtain spectral features like variance, skewness, kurtosis, and entropy (of an image) from banknote specimens used as the input parameters for the model. Based on these measurements, you can employ ML methodologies to predict an original note from a forged one.

### Using Amazon SageMaker Notebook

You may follow the step-by-step instructions in the [xgboost-model notebook](./xgboost-model.ipynb) for this setup which is auto-provisioned in the [Cloudformation template](../cloud-formation/cloud-formation-template.yaml).

## Data Preparation

Please use the the [data preparation SQL file](./data_preparation.sql) to populate the input tables for this example setup.

## Create Model

We can use the [create model](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_MODEL.html) statement in Amazon Redshift to create our ML model using XGBoost. We create the model in Redshift ML with `AUTO OFF` and `XGBoost` as the model type. We specified the `class` column as the target (label) that we want to predict, and specified `func_model_banknoteauthentication_xgboost_binary` as the function. As an advanced data scientist, we also specify advanced options like `objective` and `hyperparameters`. Amazon SageMaker creates the function `func_model_banknoteauthentication_xgboost_binary`, which we use to do inference in Amazon Redshift. See the following code:

```sql
--Create model
CREATE MODEL model_banknoteauthentication_xgboost_binary FROM banknoteauthentication_train
TARGET class
FUNCTION func_model_banknoteauthentication_xgboost_binary
IAM_ROLE '<<your-amazon-redshift-sagemaker-iam-role-arn>>'
AUTO OFF
MODEL_TYPE xgboost
OBJECTIVE 'binary:logistic'
PREPROCESSORS 'none'
HYPERPARAMETERS DEFAULT EXCEPT(NUM_ROUND '100')
SETTINGS(S3_BUCKET '<<your-amazon-s3-bucket-name>>');
```

 ## Evaluation

 In this step, we evaluate the accuracy of our ML model against our test data in `banknoteauthentication_test` table.

 While creating the model, Amazon SageMaker Autopilot automatically splits the input data into train and validation sets, and selects the model with the best objective metric, which is deployed in the Amazon Redshift cluster. You can use the `show model` statement in your cluster to view various metrics, including the accuracy score.

```sql
 SHOW MODEL model_banknoteauthentication_xgboost_binary;
```
XGBoost with `AUTO OFF` provides `train:error` metric in `SHOW MODEL` command output, which is a measure of accuracy. For example a value of  `0.99` indicates the model is close to 99% accurate.

For binary and multi-class classification problems, we compute the accuracy as the model metric. Accuracy can be calculated based on the following:

```
accuracy = (sum (actual == predicted)/total) *100
```

You may run below query to view the model accuracy:

```sql
-- check accuracy
WITH infer_data AS (
SELECT class AS label,
func_model_banknoteauthentication_xgboost_binary (variance, skewness, curtosis, entrophy) AS predicted,
CASE
   WHEN label IS NULL
       THEN 0
   ELSE label
   END AS actual,
CASE
   WHEN actual = predicted
       THEN 1::INT
   ELSE 0::INT
   END AS correct
FROM banknoteauthentication_test),
aggr_data AS (
SELECT SUM(correct) AS num_correct,
COUNT(*) AS total
FROM infer_data)
SELECT (num_correct::FLOAT / total::FLOAT) AS accuracy FROM aggr_data;
```

 ## ML Predictions

Let's run the prediction query on the `banknoteauthentication_test` to get the count of original vs. counterfeit banknotes:

```sql
--check prediction
WITH infer_data AS (
    SELECT func_model_banknoteauthentication_xgboost_binary(variance, skewness, curtosis, entrophy) AS predicted
    FROM banknoteauthentication_test
    )SELECT CASE
        WHEN predicted = '0'
            THEN 'Original banknote'
        WHEN predicted = '1'
            THEN 'Counterfeit banknote'
        ELSE 'NA'
        END AS banknote_authentication
    ,COUNT(1) AS count FROM infer_data GROUP BY 1;
```
