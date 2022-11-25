CREATE VIEW public.customer_transformations
as
select customer_id, CUSTOMER_ID_NB_TX_1DAY_WINDOW, CUSTOMER_ID_AVG_AMOUNT_1DAY_WINDOW,CUSTOMER_ID_NB_TX_7DAY_WINDOW,
CUSTOMER_ID_AVG_AMOUNT_7DAY_WINDOW,CUSTOMER_ID_NB_TX_30DAY_WINDOW, CUSTOMER_ID_AVG_AMOUNT_30DAY_WINDOW
from (
 Select customer_id,
sum(case when cast(a.TX_DATETIME as date) = cast(getdate() as date) then TX_AMOUNT  else 0 end) as CUSTOMER_ID_NB_TX_1DAY_WINDOW,
avg(case when cast(a.TX_DATETIME as date) = cast(getdate() as date) then TX_AMOUNT else 0 end )as CUSTOMER_ID_AVG_AMOUNT_1DAY_WINDOW,
sum(case when cast(a.TX_DATETIME as date) BETWEEN  cast(getdate() as date) -7 AND cast(getdate() as date) then TX_AMOUNT else 0 end) as CUSTOMER_ID_NB_TX_7DAY_WINDOW,
avg(case when cast(a.TX_DATETIME as date) BETWEEN  cast(getdate() as date) -7 AND cast(getdate() as date) then TX_AMOUNT  else 0 end ) as CUSTOMER_ID_AVG_AMOUNT_7DAY_WINDOW,
sum(case when cast(a.TX_DATETIME as date) BETWEEN  cast(getdate() as date) -30 AND cast(getdate() as date) then TX_AMOUNT else 0 end) as CUSTOMER_ID_NB_TX_30DAY_WINDOW,
avg( case when cast(a.TX_DATETIME as date) BETWEEN  cast(getdate() as date) -30 AND cast(getdate() as date) then TX_AMOUNT  else 0 end ) as CUSTOMER_ID_AVG_AMOUNT_30DAY_WINDOW
FROM
(select customer_id, terminal_id, TX_AMOUNT, cast(TX_DATETIME as timestamp) TX_DATETIME
 from cust_payment_tx_stream  --retrieve streaming data
 where cast(tx_datetime as date) between cast(getdate() as date) -37 AND cast(getdate() as date)
 union
 select  customer_id, terminal_id, TX_AMOUNT,cast(TX_DATETIME as timestamp) TX_DATETIME
 from cust_payment_tx_history   -- retrieve historical data
 where  cast(tx_datetime as date) between cast(getdate() as date) -37 AND cast(getdate() as date)
 ) a
 group by 1
);
