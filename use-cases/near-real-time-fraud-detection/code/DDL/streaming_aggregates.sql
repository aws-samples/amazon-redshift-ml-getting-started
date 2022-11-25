CREATE VIEW public.cust_payment_tx_fraud_predictions
as
select
a.TRANSACTION_ID,   a.TX_DATETIME,  a.CUSTOMER_ID,  a.TERMINAL_ID,  
a.TX_AMOUNT ,   
 a.TX_TIME_SECONDS   ,
  a.TX_TIME_DAYS  ,


fn_customer_cc_fd(a.TX_AMOUNT ,   
   case when extract(dow from cast(a.TX_DATETIME as timestamp)) in (1,7) then 1 else 0 end as TX_DURING_WEEKEND,
case when extract(hour from cast(a.TX_DATETIME as timestamp)) between 00 and 06 then 1 else 0 end as TX_DURING_NIGHT,
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

    from
public.cust_payment_tx_stream a
join terminal_transformations t
on a.terminal_id = t.terminal_id
join customer_transformations c
on a.customer_id = c.customer_id
;
