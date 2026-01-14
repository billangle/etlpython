#! /bin/sh

#python3 deploy.py --config config/flpidssteam.json --region us-east-1 --project-type flpids
./01_gls_generic_ods.sh
sleep 1
./02_plas_rc540_weekly.sh
sleep 1
./03_plas_rc540_monthly.sh
sleep 1
./04_plas_scims_rpt.sh
sleep 1
./05_plas_mfo900_weekly_timestamped.sh
sleep 1
./06_nats_csv_with_date.sh
sleep 1
./07_plas_weekly_subfolder_example.sh