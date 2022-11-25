CREATE OR REPLACE PROCEDURE public.sp_load_cust_credit_card_txn_hist() LANGUAGE plpgsql AS $$ BEGIN

truncate table public.cust_credit_card_txn_stg;

insert into public.cust_credit_card_txn_stg
(TRANSACTION_ID,TX_DATETIME,CUSTOMER_ID,TERMINAL_ID,TX_AMOUNT,TX_TIME_SECONDS,TX_TIME_DAYS,TX_FRAUD,TX_FRAUD_SCENARIO,APPX_RECORD_ARRVL_TS)
SELECT
    TRANSACTION_ID,
    cast(TX_DATETIME as datetime) as TX_DATETIME,
    CUSTOMER_ID,
    TERMINAL_ID,
    TX_AMOUNT,
    TX_TIME_SECONDS,
    TX_TIME_DAYS,
    TX_FRAUD,
    TX_FRAUD_SCENARIO,
    approximatearrivaltimestamp as  APPX_RECORD_ARRVL_TS
   
    from custpaytxn.cust_payment_tx_stream;

/* Step 3 : Load Data into Final Table from STG Table */  
 
--Delete any existing record
DELETE FROM public.cust_credit_card_txn_hist
USING public.cust_credit_card_txn_stg
WHERE public.cust_credit_card_txn.TRANSACTION_ID = public.cust_credit_card_txn_stg.TRANSACTION_ID;
 
--Insert Data from STG into Final Customer payment Transaction
insert into public.cust_credit_card_txn_hist
(TRANSACTION_ID,TX_DATETIME,CUSTOMER_ID,TERMINAL_ID,TX_AMOUNT,TX_TIME_SECONDS,TX_TIME_DAYS,TX_FRAUD,TX_FRAUD_SCENARIO)
SELECT
    TRANSACTION_ID,
    cast(TX_DATETIME as datetime) as TX_DATETIME,
    CUSTOMER_ID,
    TERMINAL_ID,
    TX_AMOUNT,
    TX_TIME_SECONDS,
    TX_TIME_DAYS,
    TX_FRAUD,
    TX_FRAUD_SCENARIO
    from public.cust_credit_card_txn_stg;
    
END;
$$ 