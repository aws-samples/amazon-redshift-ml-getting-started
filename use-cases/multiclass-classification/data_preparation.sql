/* DISCLAIMER: Please replace <your-amazon-redshift-sagemaker-iam-role-arn> with the IAM role ARN of your Amazon Redshift cluster in the SQL scripts below


/* Data Preparation */

CREATE TABLE IF NOT EXISTS ecommerce_sales
(
	invoiceno VARCHAR(30)   
	,stockcode VARCHAR(30)   
	,description VARCHAR(60)    
	,quantity DOUBLE PRECISION   
	,invoicedate VARCHAR(30)    
	,unitprice    DOUBLE PRECISION
	,customerid BIGINT    
	,country VARCHAR(25)    
)
;

Copy ecommerce_sales
From 's3://redshift-ml-multiclass/ecommerce_data.txt'
iam_role 'your-amazon-redshift-sagemaker-iam-role-arn' delimiter '\t' IGNOREHEADER 1 region 'us-east-1' maxerror 100;


/* -- assign random id to be used to split out data sets -- */
create table ecommerce_sales_data as (
  select
    t1.stockcode,
    t1.description,
    t1.invoicedate,
    t1.customerid,
    t1.country,
    t1.sales_amt,
    cast(random() * 100 as int) as data_group_id
  from
    (
      select
        stockcode,
        description,
        invoicedate,
        customerid,
        country,
        sum(quantity * unitprice) as sales_amt
      from
        ecommerce_sales
      group by
        1,
        2,
        3,
        4,
        5
    ) t1
);


/* -- create training set  */
create table ecommerce_sales_training as (
  select
    a.customerid,
    a.country,
    a.stockcode,
    a.description,
    a.invoicedate,
    a.sales_amt,
    (b.nbr_months_active) as nbr_months_active
  from
    ecommerce_sales_data a
    inner join (
      select
        customerid,
        count(
          distinct(
            DATE_PART(y, cast(invoicedate as date)) || '-' || LPAD(
              DATE_PART(mon, cast(invoicedate as date)),
              2,
              '00'
            )
          )
        ) as nbr_months_active
      from
        ecommerce_sales_data
      group by
        1
    ) b on a.customerid = b.customerid
  where
    a.data_group_id < 80
);
 

/*  create validation set  */
create table ecommerce_sales_validation as (
  select
    a.customerid,
    a.country,
    a.stockcode,
    a.description,
    a.invoicedate,
    a.sales_amt,
    (b.nbr_months_active) as nbr_months_active
  from
    ecommerce_sales_data a
    inner join (
      select
        customerid,
        count(
          distinct(
            DATE_PART(y, cast(invoicedate as date)) || '-' || LPAD(
              DATE_PART(mon, cast(invoicedate as date)),
              2,
              '00'
            )
          )
        ) as nbr_months_active
      from
        ecommerce_sales_data
      group by
        1
    ) b on a.customerid = b.customerid
  where
    a.data_group_id between 80
    and 90
);

/* create inference set */
create table ecommerce_sales_prediction as (
  select
    customerid,
    country,
    stockcode,
    description,
    invoicedate,
    sales_amt
  from
    ecommerce_sales_data
  where
    data_group_id > 90);
