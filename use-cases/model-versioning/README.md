# Model Versioning
 

You can use these scripts and customize them as needed. These contain all the components needed to
automate the process of performing model versioning. These example scripts use Bash scripts with
RSQL running on an EC2 instance. If you prefer, you can also install RSQL on Windows or macOS X.
You may find more information on using RSQL to interact with Amazon Redshift here:
 https://docs.aws.amazon.com/redshift/latest/mgmt/rsql-query-tool-getting-started.html.
 

Before running the scripts, we need to create the schema and the table needed to generate the create
model command for the model version. Run the following sql commands 

## Create the table to contain the metadata needed to auto-generate the create model command:
```sql
create table local_inf_ml_model_components
(model_name varchar(500),
schema_name varchar(500),
automlJobName varchar(500),
functionName varchar(500),
inputs_data_type varchar(500),
target_column varchar(50),
returns_data_type varchar(50),
model_arn varchar (500),
S3_Bucket varchar (200) );
```
## Initialize the local_inf_ml_components table.
Note that you will just need to initialize this table once, with the model name, schema name,
the data type of the target value we are predicting, the Amazon Resource Name (ARN) of the
IAM role, and the S3 bucket to be used for the Redshift ML artifacts. The table will get updated
with the additional data needed as part of the automation script:
insert into local_inf_ml_model_components
values
(
'<your model name>',
'<your schema name>',
' ',' ',' ',' ','float8',
'<arn of your IAM ROLE>'
'<your S3 Bucket>)';


## Execute script step1_create_model_version.sh to create a version of your model
The contents of the step1_create_model_version.sh script is also shown in the following
code snippet. As you can see, it calls other scripts and commands as follows:
#! /bin/bash
create SHOW MODEL sql command
./generate_show_model_sql.sh '<<your model name>>' 
 Read SHOW MODEL output and write to file
./show_model.sh
copy SHOW MODEL output to the model info table
aws s3 cp create_model.txt s3://<your-s3-bucket>>
load SHOW MODEL output and prep table to generate create model
./prep_create_model.sh
generate sql to create model version
./generate_create_model_version_sql.sh
execute the sql to create model verson
./execute_create_model_version.sh

# Instructions to run scripts

Before you execute the above script, read through the following sub-sections as they contain instructions
on some setup steps.


## Creating the show_model_sqlcommand
We have a simple script called generate_show_model_sql.sh with code as shown here:
#!/bin/bash
modelname=$1
echo $1
echo SHOW MODEL $1 ';' > show_model.sql
This script takes as input the model name. In the script provided, we have already supplied the model
name in the step1_create_model_version.sh driver script. You can modify this as needed
for your models.
The script creates a SHOW MODEL command that is written to a file called show_model.sql to
be read in the show_model.sh script.


## Reading the SHOW MODEL output and writing it to a file
This step executes an Amazon Redshift RSQL script called show_model.sh, which reads the
show_model.sql file and writes the output to a file called create_model.txt.


## Copying the SHOW MODEL output to the model info table
This copies the create_model.txt file into an S3 bucket.


## Loading the SHOW MODEL output and prepping the table to generate create model
This step executes another Amazon Redshift RSQL script called prep_create_model.sh, which
performs the following:
• Creates and loads the model_info table.
• Updates local_inf_ml_model_components from the model_info table so that the
create model statement can be generated for the model version.
• Inserts the generated create model statement into the create_model_sql table.


## Generating the SQL to create the model version
This step calls an Amazon Redshift RSQL script called generate_create_model_version_
sql.sh, which reads the create_model table and writes the SQL to a text file called model_
version.txt.

## Executing the SQL to create the model version
This step calls Amazon Redshift RSQL script called execute_create_model_version.sh,
which creates the version of our previously created model.

Now you can drop and create your model since we have the model version.
