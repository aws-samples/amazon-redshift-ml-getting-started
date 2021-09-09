/* DISCLAIMER: Below syntax is for reference only. Please replace parameters <> carefully*/

/*Sample data set to run predictions on Redshift cluster*/

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


