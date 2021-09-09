# Bring your own model - local Inference

## Introduction

In this Folder, we have a working example to create "bring your own model" (BYOM) using Amazon Redshift ML. BYOM uses a pre-trained model which is trained and validated on Amazon Sagemaker.  In this use case, the model that Redshift ML BYOM uses, is built using the XGBOOST linear regression problem type, which is used to predict a numerical outcome, like the price of a house or how many people will use a city’s bike rental service or age of an abalone.

## Pre-requisites

Use the [Cloudformation template](../cloud-formation/cloud-formation-template.yaml) to setup your Amazon Redshift cluster with Redshift ML, Amazon Sagemaker Studio and other necessary security groups and policies. 
 
## Use Case

We use the [Abalone data](https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/regression.html) originally from the UCI data repository [1]. More details about the original dataset can be found [here](https://archive.ics.uci.edu/ml/machine-learning-databases/abalone/abalone.names).   

### Using Amazon SageMaker Studio

Follow below steps to access Sagemaker Studio

    Once Cloud Formation Template stack is created 
    * Navigate to the Amazon Sagemaker console
    * On left hand side, Click on <mark>Amazon Sagemaker Studio</mark>
    * Under SageMaker Studio Control Panel, Select <mark>redshift-ml-data-scientist</mark> user
    * for redshift-ml-data-scientist user on right hand side, Click on <mark>"open Studio"</mark>. This will take you to Sagemaker Studio. 

To Create BYOM Local Inference, you are going to follow step-by-step instructions provided in [bring-your-own-model-local-inference notebook](./bring-your-own-model-local-inference.ipnyb).  To access this notebook in Sagemaker studio follow below steps.

### Clone Redshift ML Repository 

    * Once in the Sagemaker Studio, from the options, Select <mark>Git</mark> and select <mark>Clone Repository</mark>
    * Provide following URL https://github.com/aws-samples/amazon-redshift-ml-getting-started.git
    * Now on left hand side, click on folder icon, which will show Git Repo Folder
    * Navigate as below to find bring-your-own-model-local-inference notebook
      * amazon-redshift-ml-getting-started/use-cases/bring-your-own-model-local-inference.ipynb
    * double click on bring-your-own-model-local-inference.ipynb file
    * Now you should have the Jupyter Notebook in Sagemaker Studio up and running. 

### Training the XGBOOST Model on Sagemaker

Execute first part of the notebook which has code to create, train and Validate a XGBOOST model using Amazon Sagemaker services.  If you already have a pre-trained XBGBOOST model then you can jump to Create BYOM part of the notebook and use the code with small modifications to use your own model.

**NOTE**:  If you choose to skip Part 1 then you would need path of Sagemaker training model data file and a dataset loaded onto Redshift Cluster to run inferences on Redshift cluster.

# Create BYOM and run predictions on Redshift

## Data Preparation

* Run below commands to create a data set to run predictions . 

```sql
create table abalone_test
(
Rings int,
sex int,
Length_ float,
Diameter float, 
Height float, 
WholeWeight float,
ShuckedWeight float, 
VisceraWeight float, 
ShellWeight float
);

copy abalone_test
from 's3://jumpstart-cache-prod-us-east-1/1p-notebooks-datasets/abalone/text-csv/test/'
IAM_ROLE '<<Redshift Cluster role - Get this from Cloud formation Stack output>>'
FORMAT AS CSV QUOTE AS '"'
;
```

## Create BYOM Model 

*  Run below create model command. If you notice the <mark>create model </mark> syntax has a <mark>from </mark>clause where we are going to pass path of training artifact created by Sagemaker. In the notebook this value is coming through model_data parameter.  Command will take about 5 minutes to run.  Create Model BYOM also expects Redshift IAM role and S3_bucket.  S3 Bucket is Amazon S3 location that is used to store intermediate results.

```sql
Drop model if exists predict_abalone_age; 
CREATE MODEL predict_abalone_age
FROM '<<Sagemaker Training Model s3 path - get this from jupyter >>' 
FUNCTION <<your redshift ml predict function name>> ( feature1 datatype, feature2 datatype, ...) 
RETURNS int 
IAM_ROLE '<<your-amazon-redshift-sagemaker-iam-role-arn>>' 
settings( S3_BUCKET '<<S3 bucket for intermediate results - get this is from cloud formation stack output>>') 
;
  ```

* Once Model is created, to view the status, you can rum show model command. 

```sql
show model predict_abalone_age;
```

 ## ML Predictions

 Let’s run a query to predict age of abalone

```sql
Select original_age, predicted_age, original_age-predicted_age as Error 
From( 
select predict_abalone_age(Rings,sex, 
Length_ , 
Diameter ,  
Height , 
WholeWeight ,
ShuckedWeight ,  
VisceraWeight , 
ShellWeight ) predicted_age, rings as original_age 
from abalone_test ); 
```

 ## Evaluation

If you choose to validate the predicted results, you can run below commands. 

### R<sup>2</sup>:

```sql
SELECT 1-(SUM(POWER(rings - (predict_abalone_age(Rings,sex, 
Length_ , 
Diameter , 
Height , 
WholeWeight , 
ShuckedWeight , 
VisceraWeight , 
ShellWeight )),2)))/(SUM(POWER(rings - (SELECT AVG(rings) FROM abalone_test),2))) R2_Value FROM abalone_test; 
  ```

### RMSE Value:

```sql
SELECT SQRT(Avg( POWER(rings - (predict_abalone_age(Rings,sex, 
Length_ , 
Diameter , 
Height , 
WholeWeight , 
ShuckedWeight ,  
VisceraWeight ,  
ShellWeight )) , 2) ) ) as abalone_age_rmse FROM abalone_test;
  ```