 CREATE materialized VIEW public.cust_cc_txn_fd_vw
AS SELECT a.transaction_id,
          a.tx_datetime,
          a.customer_id,
          a.tx_amount,
          a.terminal_id,
          a.tx_time_days,
          b.full_name          AS customer_Name,
          b.phone_number       AS customer_phone,
          b.email_address      AS customer_email,
Fn_customer_cc_fd (transaction_id, transaction_id,
Cast(tx_datetime AS
TIMESTAMP), a.customer_id, terminal_id, tx_amount, tx_time_seconds, tx_time_days
, tx_fraud_scenario) AS class_predicted
FROM   custpaytxn.cust_payment_tx_stream a
       inner join customer_info b
               ON a.customer_id = b.customer_id
WHERE  class_predicted = 1;  