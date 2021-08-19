# Regression

**Note**: The contents of this folder was taken from the AWS Blog Post: [Build regression models with Amazon Redshift ML](https://aws.amazon.com/blogs/machine-learning/build-regression-models-with-amazon-redshift-ml/).


## Introduction

In this folder, we have a working example to create models in Amazon Redshift using the regression problem type, which is used to predict a numerical outcome, like the price of a house or how many people will use a city’s bike rental service.

## Pre-requisites

Use the [Cloudformation template](../cloud-formation/cloud-formation-template.yaml) to setup your Amazon Redshift cluster with Redshift ML or create an IAM role allowing Amazon Redshift to communicate with Amazon SageMaker.   

## Use Case

For our use case, we want to build a regression model that predicts the number of people that may use the city of Toronto’s bike sharing service at any given hour of a day. The model accounts for various aspects, including holidays and weather conditions. Because we need to predict a numerical outcome, we create a regression model.

### Using Amazon SageMaker Notebook

You may follow the step-by-step instructions in the [multi-class-classification notebook](./multi-class-classification.ipynb) for this setup which is auto-provisioned in the [Cloudformation template](../cloud-formation/cloud-formation-template.yaml).

## Data Preparation

Please use the the [data preparation SQL file](./data_preparation.sql) to populate the input tables for this example setup.

## Create Model

We can use the create model statement in Amazon Redshift to create our ML model using regression. We specify the problem type but we let AutoML take care of everything else. In this model, the target we want to predict is `trip_count`. Amazon SageMaker creates the function predict_customer_activity, which we use to do inference in Amazon Redshift. See the following code:

```sql
CREATE MODEL predict_rental_count
FROM training_data
TARGET trip_count
FUNCTION predict_rental_count
IAM_ROLE '<<your-amazon-redshift-sagemaker-iam-role-arn>>'
PROBLEM_TYPE regression
OBJECTIVE 'mse'
SETTINGS (s3_bucket '<<your-amazon-s3-bucket-name>>',
          s3_garbage_collect off,
          max_runtime 5000);
  ```

 ## Evaluation

 In this step, we evaluate the accuracy of our ML model against our validation data.

 While creating the model, Amazon SageMaker Autopilot automatically splits the input data into train and validation sets, and selects the model with the best objective metric, which is deployed in the Amazon Redshift cluster. You can use the show model statement in your cluster to view various metrics, including the accuracy score. If you don’t specify explicitly, SageMaker automatically uses accuracy for the objective type. See the following code:

 Show model predict_rental_count;

 You can see the validation:mse value in the output of the `SHOW MODEL` command. A value closer to 100 is better.

 Run the inference query:

```sql
SELECT
    trip_time
  , actual_count
  , predicted_count
  , ( actual_count - predicted_count ) difference
FROM  
  (SELECT
       trip_time
       , trip_count AS actual_count
       , PREDICT_RENTAL_COUNT (trip_hour, trip_day, trip_month, trip_year, trip_quarter, trip_month_week, trip_week_day, temp_c, precip_amount_mm, is_holiday) predicted_count
   FROM  
       validation_data) LIMIT 5;
  ```

 ## ML Predictions

 Let’s run a query to see what time the customers would take a bike-ride:
```sql
SELECT
     trip_time
     , trip_count AS actual_count
     , PREDICT_RENTAL_COUNT (trip_hour, trip_day, trip_month, trip_year, trip_quarter, trip_month_week, trip_week_day, temp_c, precip_amount_mm, is_holiday) predicted_count
 FROM  
     validation_data
 limit 10;
```
