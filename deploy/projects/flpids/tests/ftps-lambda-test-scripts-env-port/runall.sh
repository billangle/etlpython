#! /bin/sh

#python3 deploy.py --config config/flpidssteam.json --region us-east-1 --project-type flpids
./01_gls_generic_ods.sh
./02_plas_rc540_weekly.sh
./03_plas_rc540_monthly.sh
./04_plas_scims_rpt.sh
./05_plas_mfo900_weekly_timestamped.sh
./06_nats_csv_with_date.sh
./07_plas_weekly_subfolder_example.sh