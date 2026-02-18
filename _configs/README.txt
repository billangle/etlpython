Per POPSUP-7222.

Configuration schema JSONS:
fpac-py-deploy/deploy/projects/nps/SCHEMA_JSON/NPS
fpac-py-deploy/deploy/projects/nps/SCHEMA_JSON/NRRS

These are the json's in the folder SCHEMA_JSON which will need to go to 
 
s3://c108-prod-fpacfsa-landing-zone/dmart/raw/fwadm/SCHEMA_JSON/NPS/
s3://c108-prod-fpacfsa-landing-zone/dmart/raw/fwadm/SCHEMA_JSON/NRRS/

ETL Glues:
NPS_build_archive_table.py --> FSA-PROD-FWADM-NPS_build_archive_table
payable_dim_tlincremental.py --> FSA-PROD-FWADM-payable_dim_tlincremental
payment_summary.py --> FSA-PROD-FWADM-payment_summary
payment_transaction_fact_part3.py --> FSA-PROD-FWADM-payment_transaction_fact_part3
payment_transaction_fact_part2.py --> FSA-PROD-FWADM-payment_transaction_fact_part2
payment_transaction_fact_part1.py --> FSA-PROD-FWADM-payment_transaction_fact_part1
payment_transaction_fact_final.py --> FSA-PROD-FWADM-payment_transaction_fact_final

States:
NPS_build_archive_tables.json --> FSA-PROD-FWADM-NPS_build_archive_tables

