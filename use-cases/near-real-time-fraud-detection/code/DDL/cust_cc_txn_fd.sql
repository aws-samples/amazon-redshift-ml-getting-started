 CREATE model public.cust_cc_txn_fd
  FROM (
         SELECT *
         FROM   public.cust_credit_card_txn_hist
         WHERE  cast(tx_datetime AS date) BETWEEN '2022-01-01’ AND '2022-06-15’ ) target tx_fraud
  function fn_customer_cc_fd
    iam_role DEFAULT
    settings (
              s3_bucket 'rs-ml-cust-fd’,
              s3_garbage_collect off,
              max_runtime 9999
              ); 

SHOW MODEL public.cust_cc_txn_fd;