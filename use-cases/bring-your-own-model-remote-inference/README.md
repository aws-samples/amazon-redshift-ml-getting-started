# Unsupervised Learning - Bring your own model using External Reference for Random Cut Forest Algorithm

**Note**: The contents of the Jupyter Notebook which build the model in Amazon Sagemaer was taken from the Github project : https://github.com/aws/amazon-sagemaker-examples/blob/master/introduction_to_amazon_algorithms/random_cut_forest/random_cut_forest.ipynb


## Introduction

Amazon SageMaker Random Cut Forest (RCF) is an algorithm designed to detect anomalous data points within a dataset. Examples of when anomalies are important to detect include when website activity uncharactersitically spikes, when temperature data diverges from a periodic behavior, or when changes to public transit ridership reflect the occurrence of a special event.

In this notebook, we will use the SageMaker RCF algorithm to train an RCF model on the Numenta Anomaly Benchmark (NAB) NYC Taxi dataset which records the amount New York City taxi ridership over the course of six months. We will then use this model to predict anomalous events by emitting an "anomaly score" for each data point.

We then show you how you can alternately create a model using Redshift ML which will do remote inference by accessing the SageMaker endpoint generated previously.  

## Pre-requisites

Use the [Cloudformation template](../cloud-formation/cloud-formation-template.yaml) to setup your Amazon Redshift cluster with Redshift ML or create an IAM role allowing Amazon Redshift to communicate with Amazon SageMaker.   


### Using Amazon SageMaker Notebook

You may follow the step-by-step instructions in the [BYOM_remote_inference notebook] (BYOM_remote_inference.ipynb) 

## Data Preparation

Please use the the [data preparation SQL file](./data_preparation.sql) to populate the input tables for this example setup, if you choose to do the Redshift ML portion outside of the notebook.

## Create Model

We can use the create model statement in Amazon Redshift to create our ML model using the Endpoint created by Amazon SageMaker. 
```sql

CREATE MODEL public.remote_random_cut_forest
FUNCTION remote_fn_rcf(int)
RETURNS decimal(10,6)
SAGEMAKER 'randomcutforest-xxxxxxxxx'
IAM_ROLE 'arn:aws:iam::<accountid>:role/RedshiftML';
  ```

 ## Inference 

 In this step we compute anomoly scores across the entire taxi dataset.

 Show model public.remote_random_cut_forest.

 You can see the endpoint and function name.alidation:

 Run the inference query:

```sql
 select ride_timestamp, nbr_passengers, public.remote_fn_rcf(nbr_passengers) as score
from public.rcf_taxi_data;
  ```

 ## Check for high anomolies

Note that the anomaly score spikes where our eyeball-norm method suggests there is an anomalous data point as well as in some places where our eyeballs are not as accurate.

Below we run a query for  any data points with scores greater than 3 standard deviations (approx 99.9th percentile) from the mean score.

```sql
with score_cutoff as
(select stddev(public.remote_fn_rcf(nbr_passengers)) as std, avg(public.remote_fn_rcf(nbr_passengers)) as mean, ( mean + 3 * std ) as score_cutoff_value
from public.rcf_taxi_data)

select ride_timestamp, nbr_passengers, public.remote_fn_rcf(nbr_passengers) as score
from public.rcf_taxi_data
where score > (select score_cutoff_value from score_cutoff);
 
```
