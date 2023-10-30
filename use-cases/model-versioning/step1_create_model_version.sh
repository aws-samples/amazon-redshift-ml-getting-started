#! /bin/bash
#Create show model sql command
./generate_show_model_sql.sh  'public.predict_rental_count'
#Collect show model output and write to file
./show_model.sh
#copy the show model output to the model info table
aws s3 cp create_model.txt s3://<your-s3-bucket>>
./prep_create_model.sh 
#generate sql to create model version
./generate_create_model_version_sql.sh
#execute the sql to create model verson
./execute_create_model_version.sh                                     
