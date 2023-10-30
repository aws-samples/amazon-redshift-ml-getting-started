
DROP MODEL predict_rental_count;
CREATE MODEL predict_rental_count
FROM training_data
TARGET trip_count
FUNCTION predict_rental_count
IAM_ROLE 'arn:aws:iam::<accountid>:role/RedshiftML'
PROBLEM_TYPE regression
OBJECTIVE 'mse'
SETTINGS (s3_bucket 'redshiftml-<your-account-id>',
          s3_garbage_collect off,
          max_runtime 5000);