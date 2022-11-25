CREATE VIEW public.cust_payment_tx_fraud_predictions
as
select a.approximate_arrival_timestamp,
     d.full_name , d.email_address, d.phone_number,
a.TRANSACTION_ID,   a.TX_DATETIME,  a.CUSTOMER_ID,  a.TERMINAL_ID,  
a.TX_AMOUNT ,   
 a.TX_TIME_SECONDS   ,
  a.TX_TIME_DAYS  ,
public.fn_customer_cc_fd(a.TX_AMOUNT ,   
    a.TX_DURING_WEEKEND,
a.TX_DURING_NIGHT,
c.CUSTOMER_ID_NB_TX_1DAY_WINDOW ,
c.CUSTOMER_ID_AVG_AMOUNT_1DAY_WINDOW    ,
 c.CUSTOMER_ID_NB_TX_7DAY_WINDOW ,
 c.CUSTOMER_ID_AVG_AMOUNT_7DAY_WINDOW    ,
c.CUSTOMER_ID_NB_TX_30DAY_WINDOW    ,
   c.CUSTOMER_ID_AVG_AMOUNT_30DAY_WINDOW   ,
 t.TERMINAL_ID_NB_TX_1DAY_WINDOW ,
   t.TERMINAL_ID_RISK_1DAY_WINDOW  ,
   t.TERMINAL_ID_NB_TX_7DAY_WINDOW ,
   t.TERMINAL_ID_RISK_7DAY_WINDOW  ,
    t.TERMINAL_ID_NB_TX_30DAY_WINDOW    ,
    t.TERMINAL_ID_RISK_30DAY_WINDOW ) Fraud_prediction
From
      (select
      Approximate_arrival_timestamp,
      TRANSACTION_ID,   TX_DATETIME,  CUSTOMER_ID,  TERMINAL_ID,  
      TX_AMOUNT ,   
      TX_TIME_SECONDS   ,
      TX_TIME_DAYS  ,
      case when extract(dow from cast(TX_DATETIME as timestamp)) in (1,7) then 1 else 0 end as TX_DURING_WEEKEND,
      case when extract(hour from cast(TX_DATETIME as timestamp)) between 00 and 06 then 1 else 0 end as TX_DURING_NIGHT
       FROM cust_payment_tx_stream) a
join terminal_transformations t
on a.terminal_id = t.terminal_id
join customer_transformations c
on a.customer_id = c.customer_id
join customer_info d
on a.customer_id = d.customer_id
;
