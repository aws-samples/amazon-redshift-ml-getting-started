CREATE VIEW public.terminal_transformations
as
select terminal_id,
terminal_id_nb_tx_1day_window, terminal_id_risk_1day_window, terminal_id_nb_tx_7day_window, terminal_id_risk_7day_window,
terminal_id_nb_tx_30day_window, terminal_id_risk_30day_window
from (
 Select terminal_id,  max(cast(a.TX_DATETIME as date) ) maxdt, min(cast(a.TX_DATETIME as date) ) mindt, max(tx_fraud) mxtf, min(tx_fraud) mntf, sum(case when tx_fraud =1 then 1 else 0 end) sumtxfraud,
SUM(case when cast(a.TX_DATETIME as date) BETWEEN  cast(getdate() as date) -7 AND cast(getdate() as date)  and tx_fraud = 1 then tx_fraud else 0 end) as NB_FRAUD_DELAY1,
SUM(case when cast(a.TX_DATETIME as date) BETWEEN  cast(getdate() as date) -7 AND cast(getdate() as date) then tx_fraud else 0 end)  as NB_TX_DELAY1 ,
SUM(case when cast(a.TX_DATETIME as date) BETWEEN  cast(getdate() as date) -8 AND cast(getdate() as date)  and tx_fraud = 1 then tx_fraud else 0 end) as NB_FRAUD_DELAY_WINDOW1,
SUM(case when cast(a.TX_DATETIME as date) BETWEEN  cast(getdate() as date) -8 AND cast(getdate() as date) then 1 else 0 end)  as NB_TX_DELAY_WINDOW1,
NB_FRAUD_DELAY_WINDOW1-NB_FRAUD_DELAY1 as NB_FRAUD_WINDOW1,
NB_TX_DELAY_WINDOW1-NB_TX_DELAY1 as terminal_id_nb_tx_1day_window,
case when terminal_id_nb_tx_1day_window = 0 then 0 else
NB_FRAUD_WINDOW1/ terminal_id_nb_tx_1day_window  end  as terminal_id_risk_1day_window ,
--7 day
sum(case when cast(a.TX_DATETIME as date) BETWEEN  cast(getdate() as date) -7 AND cast(getdate() as date)  and tx_fraud = 1 then tx_fraud else 0 end ) as NB_FRAUD_DELAY7,
sum(
case when cast(a.TX_DATETIME as date) BETWEEN  cast(getdate() as date) -7 AND cast(getdate() as date) then
1 else 0 end)  as NB_TX_DELAY7,
sum(
case when cast(a.TX_DATETIME as date) BETWEEN  cast(getdate() as date) -14 AND cast(getdate() as date)  and tx_fraud = 1 then
tx_fraud else 0 end) as NB_FRAUD_DELAY_WINDOW7,
sum(
case when cast(a.TX_DATETIME as date) BETWEEN  cast(getdate() as date) -14 AND cast(getdate() as date) then
1 else 0 end)  as NB_TX_DELAY_WINDOW7,
NB_FRAUD_DELAY_WINDOW7-NB_FRAUD_DELAY7 as NB_FRAUD_WINDOW7,
NB_TX_DELAY_WINDOW7-NB_TX_DELAY7 as terminal_id_nb_tx_7day_window,
case when terminal_id_nb_tx_7day_window = 0 then 0 else
NB_FRAUD_WINDOW7/ terminal_id_nb_tx_7day_window  end  as terminal_id_risk_7day_window,
--30 day period
sum(case when cast(a.TX_DATETIME as date) BETWEEN  cast(getdate() as date)-7 AND cast(getdate() as date)  and tx_fraud = 1 then tx_fraud else 0 end) as NB_FRAUD_DELAY30,
sum(
case when cast(a.TX_DATETIME as date) BETWEEN  cast(getdate() as date)-7 AND cast(getdate() as date) then
1 else 0 end)  as NB_TX_DELAY30,
sum(
case when cast(a.TX_DATETIME as date) BETWEEN  cast(getdate() as date)-37 AND cast(getdate() as date)  and tx_fraud = 1 then
tx_fraud else 0 end) as NB_FRAUD_DELAY_WINDOW30,
sum(
case when cast(a.TX_DATETIME as date) BETWEEN  cast(getdate() as date)-37 AND cast(getdate() as date) then
1 else 0 end)  as NB_TX_DELAY_WINDOW30,
NB_FRAUD_DELAY_WINDOW30-NB_FRAUD_DELAY30 as NB_FRAUD_WINDOW30,
NB_TX_DELAY_WINDOW30-NB_TX_DELAY30 as terminal_id_nb_tx_30day_window,
case when terminal_id_nb_tx_30day_window = 0 then 0 else
NB_FRAUD_WINDOW30/ terminal_id_nb_tx_30day_window  end  as terminal_id_risk_30day_window
FROM
(select terminal_id, TX_AMOUNT, cast(TX_DATETIME as timestamp) TX_DATETIME, 0 as TX_FRAUD
 from cust_payment_tx_stream
 where cast(tx_datetime as date) between cast(getdate() as date) -37 AND cast(getdate() as date)
 union all
 select  terminal_id, TX_AMOUNT,TX_DATETIME, TX_FRAUD
 from cust_payment_tx_history  
 where  cast(tx_datetime as date) between cast(getdate() as date) -37 AND cast(getdate() as date)
 ) a
 group by 1
);
