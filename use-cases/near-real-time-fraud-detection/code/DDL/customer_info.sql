 CREATE TABLE public.customer_info
  (
     customer_id   BIGINT,
     job_title     VARCHAR(500),
     email_address VARCHAR(100),
     full_name     VARCHAR(200),
     phone_number  VARCHAR(20)
  ) distkey (customer_id);  