 CREATE TABLE "public"."cust_credit_card_txn_stg"
             (
                          transaction_id                         BIGINT NOT NULL encode az64,
                          tx_datetime timestamp without          time zone encode az64,
                          customer_id                            BIGINT encode az64,
                          terminal_id                            INTEGER encode az64,
                          tx_amount                              NUMERIC(18,2) encode az64,
                          tx_time_seconds                        INTEGER encode az64,
                          tx_time_days                           INTEGER encode az64,
                          tx_fraud                               INTEGER encode az64,
                          tx_fraud_scenario                      INTEGER encode az64,
                          appx_record_arrvl_ts timestamp without time zone encode az64,
                          etl_insert_dt timestamp without        time zone encode az64,
                          CONSTRAINT cust_credit_card_txn_stg_pkey PRIMARY KEY(transaction_id)
             )
             distkey
             (
                          transaction_id
             ); 