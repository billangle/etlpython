SELECT 
PRPS_PYMT_SIM_ID,
PRV_PRPS_PYMT_SIM_ID,
RCD_REF_PRIM_ID_TYPE_CD,
RCD_REF_SCND_ID_TYPE_CD,
ST_FSA_CD,
CNTY_FSA_CD,
ACCT_PGM_CD,
PAID_CORE_CUST_ID,
PAID_CORE_CUST_ST_CNTY_FSA_CD,
SBSD_PRD_STRT_YR,
PROC_RQST_REF_NBR,
RCD_REF_PRIM_ID,
RCD_REF_SCND_ID,
CMDY_CD,
PYMT_AMT,
PYMT_LMT_IND,
HRCH_PYMT_LMT_IND,
PRST_OPYMT_ATRB_IND,
APP_3_OWNSHP_LVL_LMT_IND,
DEC_PRCS_NBR,
PYMT_ATRB_RQST_RSN_TXT,
PYMT_ATRB_RVRS_DT,
DATA_STAT_CD,
CRE_DT,
CRE_USER_NM,
LAST_CHG_DT,
LAST_CHG_USER_NM,
OVRRD_SBSD_PRD_STRT_YR,
PYMT_PGM_ID,
PYMT_LMT_YR,
SIM_SCHD_END_DT,
ATRB_SIM_PYMT_TYPE_CD,
PGM_ENRL_APP_ID,
OBL_CNFRM_NBR,
CDC_OPER_CD,
''  HASH_DIF,
current_timestamp LOAD_DT,
'sbsd'  DATA_SRC_NM,
'{ETL_START_DATE}' AS CDC_DT
FROM (
SELECT * FROM
(
SELECT PRPS_PYMT_SIM_ID,
PRV_PRPS_PYMT_SIM_ID,
RCD_REF_PRIM_ID_TYPE_CD,
RCD_REF_SCND_ID_TYPE_CD,
ST_FSA_CD,
CNTY_FSA_CD,
ACCT_PGM_CD,
PAID_CORE_CUST_ID,
PAID_CORE_CUST_ST_CNTY_FSA_CD,
SBSD_PRD_STRT_YR,
PROC_RQST_REF_NBR,
RCD_REF_PRIM_ID,
RCD_REF_SCND_ID,
CMDY_CD,
PYMT_AMT,
PYMT_LMT_IND,
HRCH_PYMT_LMT_IND,
PRST_OPYMT_ATRB_IND,
APP_3_OWNSHP_LVL_LMT_IND,
DEC_PRCS_NBR,
PYMT_ATRB_RQST_RSN_TXT,
PYMT_ATRB_RVRS_DT,
DATA_STAT_CD,
CRE_DT,
CRE_USER_NM,
LAST_CHG_DT,
LAST_CHG_USER_NM,
OVRRD_SBSD_PRD_STRT_YR,
PYMT_PGM_ID,
PYMT_LMT_YR,
SIM_SCHD_END_DT,
ATRB_SIM_PYMT_TYPE_CD,
PGM_ENRL_APP_ID,
OBL_CNFRM_NBR,
OP CDC_OPER_CD,
ROW_NUMBER() over ( partition by 
PRPS_PYMT_SIM_ID
order by TBL_PRIORITY ASC, LAST_CHG_DT DESC ) AS row_num_part
FROM
(
SELECT 
proposed_payment_simulation.proposed_payment_simulation_identifier PRPS_PYMT_SIM_ID,
proposed_payment_simulation.previous_proposed_payment_simulation_identifier PRV_PRPS_PYMT_SIM_ID,
proposed_payment_simulation.record_reference_primary_identification_type_code RCD_REF_PRIM_ID_TYPE_CD,
proposed_payment_simulation.record_reference_secondary_identification_type_code RCD_REF_SCND_ID_TYPE_CD,
proposed_payment_simulation.state_fsa_code ST_FSA_CD,
proposed_payment_simulation.county_fsa_code CNTY_FSA_CD,
proposed_payment_simulation.accounting_program_code ACCT_PGM_CD,
Paid_subsidiary_customer.core_customer_identifier PAID_CORE_CUST_ID,
Paid_fsa_county.state_county_fsa_code PAID_CORE_CUST_ST_CNTY_FSA_CD,
proposed_payment_simulation.subsidiary_period_start_year SBSD_PRD_STRT_YR,
proposed_payment_simulation.processing_request_reference_number PROC_RQST_REF_NBR,
proposed_payment_simulation.record_reference_primary_identification RCD_REF_PRIM_ID,
proposed_payment_simulation.record_reference_secondary_identification RCD_REF_SCND_ID,
proposed_payment_simulation.commodity_code CMDY_CD,
proposed_payment_simulation.payment_amount PYMT_AMT,
proposed_payment_simulation.payment_limitation_indicator PYMT_LMT_IND,
proposed_payment_simulation.hierarchical_payment_limitation_indicator HRCH_PYMT_LMT_IND,
proposed_payment_simulation.persist_overpayment_attributions_indicator PRST_OPYMT_ATRB_IND,
proposed_payment_simulation.apply_3_ownership_level_limit_indicator APP_3_OWNSHP_LVL_LMT_IND,
proposed_payment_simulation.decimal_precision_number DEC_PRCS_NBR,
proposed_payment_simulation.payment_attribution_request_reason_text PYMT_ATRB_RQST_RSN_TXT,
proposed_payment_simulation.payment_attribution_reversed_date PYMT_ATRB_RVRS_DT,
proposed_payment_simulation.data_status_code DATA_STAT_CD,
proposed_payment_simulation.creation_date CRE_DT,
proposed_payment_simulation.creation_user_name CRE_USER_NM,
proposed_payment_simulation.last_change_date LAST_CHG_DT,
proposed_payment_simulation.last_change_user_name LAST_CHG_USER_NM,
proposed_payment_simulation.override_subsidiary_period_start_year OVRRD_SBSD_PRD_STRT_YR,
proposed_payment_simulation.limited_payment_program_identifier PYMT_PGM_ID,
proposed_payment_simulation.payment_limitation_year PYMT_LMT_YR,
proposed_payment_simulation.simulation_scheduled_end_date SIM_SCHD_END_DT,
proposed_payment_simulation.attribution_simulation_payment_type_code ATRB_SIM_PYMT_TYPE_CD,
proposed_payment_simulation.application_id PGM_ENRL_APP_ID,
proposed_payment_simulation.obligation_confirmation_number OBL_CNFRM_NBR,
proposed_payment_simulation.OP,
1 AS TBL_PRIORITY
FROM `fsa-{env}-sbsd-cdc`.`proposed_payment_simulation`
LEFT JOIN (SELECT *
           FROM `fsa-{env}-sbsd-cdc`.`payment_attribution_simulation`
           WHERE parent_payment_attribution_simulation_identifier is NULL
          ) UNQ_payment_attribution_simulation
ON (proposed_payment_simulation.proposed_payment_simulation_identifier = UNQ_payment_attribution_simulation.proposed_payment_simulation_identifier)
LEFT JOIN `fsa-{env}-sbsd-cdc`.`paid_subsidiary_customer`
ON (UNQ_payment_attribution_simulation.paid_subsidiary_customer_identifier = Paid_subsidiary_customer.subsidiary_customer_identifier)
LEFT JOIN `fsa-{env}-sbsd-cdc`.`paid_fsa_county`
ON (Paid_subsidiary_customer.fsa_county_identifier = Paid_fsa_county.fsa_county_identifier)
WHERE paid_fsa_county.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND paid_fsa_county.OP <> 'D'

UNION

SELECT 
proposed_payment_simulation.proposed_payment_simulation_identifier PRPS_PYMT_SIM_ID,
proposed_payment_simulation.previous_proposed_payment_simulation_identifier PRV_PRPS_PYMT_SIM_ID,
proposed_payment_simulation.record_reference_primary_identification_type_code RCD_REF_PRIM_ID_TYPE_CD,
proposed_payment_simulation.record_reference_secondary_identification_type_code RCD_REF_SCND_ID_TYPE_CD,
proposed_payment_simulation.state_fsa_code ST_FSA_CD,
proposed_payment_simulation.county_fsa_code CNTY_FSA_CD,
proposed_payment_simulation.accounting_program_code ACCT_PGM_CD,
Paid_subsidiary_customer.core_customer_identifier PAID_CORE_CUST_ID,
Paid_fsa_county.state_county_fsa_code PAID_CORE_CUST_ST_CNTY_FSA_CD,
proposed_payment_simulation.subsidiary_period_start_year SBSD_PRD_STRT_YR,
proposed_payment_simulation.processing_request_reference_number PROC_RQST_REF_NBR,
proposed_payment_simulation.record_reference_primary_identification RCD_REF_PRIM_ID,
proposed_payment_simulation.record_reference_secondary_identification RCD_REF_SCND_ID,
proposed_payment_simulation.commodity_code CMDY_CD,
proposed_payment_simulation.payment_amount PYMT_AMT,
proposed_payment_simulation.payment_limitation_indicator PYMT_LMT_IND,
proposed_payment_simulation.hierarchical_payment_limitation_indicator HRCH_PYMT_LMT_IND,
proposed_payment_simulation.persist_overpayment_attributions_indicator PRST_OPYMT_ATRB_IND,
proposed_payment_simulation.apply_3_ownership_level_limit_indicator APP_3_OWNSHP_LVL_LMT_IND,
proposed_payment_simulation.decimal_precision_number DEC_PRCS_NBR,
proposed_payment_simulation.payment_attribution_request_reason_text PYMT_ATRB_RQST_RSN_TXT,
proposed_payment_simulation.payment_attribution_reversed_date PYMT_ATRB_RVRS_DT,
proposed_payment_simulation.data_status_code DATA_STAT_CD,
proposed_payment_simulation.creation_date CRE_DT,
proposed_payment_simulation.creation_user_name CRE_USER_NM,
proposed_payment_simulation.last_change_date LAST_CHG_DT,
proposed_payment_simulation.last_change_user_name LAST_CHG_USER_NM,
proposed_payment_simulation.override_subsidiary_period_start_year OVRRD_SBSD_PRD_STRT_YR,
proposed_payment_simulation.limited_payment_program_identifier PYMT_PGM_ID,
proposed_payment_simulation.payment_limitation_year PYMT_LMT_YR,
proposed_payment_simulation.simulation_scheduled_end_date SIM_SCHD_END_DT,
proposed_payment_simulation.attribution_simulation_payment_type_code ATRB_SIM_PYMT_TYPE_CD,
proposed_payment_simulation.application_id PGM_ENRL_APP_ID,
proposed_payment_simulation.obligation_confirmation_number OBL_CNFRM_NBR,
proposed_payment_simulation.OP,
2 AS TBL_PRIORITY
FROM 
  ( 
     Select SBSD_PRE.* from (
Select * FROM `fsa-{env}-sbsd-cdc`.`subsidiary_customer`  where  subsidiary_customer.OP ='UN' And cast('{ETL_END_DATE}' as date)  <> cast(current_date as date)
     UNION
     /*same day scenario update happens*/
    Select * FROM `fsa-{env}-sbsd-cdc`.`subsidiary_customer`  where  SBSD_PRE.OP ='UN'
       And cast('{ETL_END_DATE}' as date)  = cast(current_date as date)
     )  SBSD_PRE
  JOIN
   ( Select * from `fsa-{env}-sbsd-cdc`.`subsidiary_customer`   
   ) SBSD_CUST_MAIN
ON  ( SBSD_PRE.subsidiary_customer_identifier = SBSD_CUST_MAIN.subsidiary_customer_identifier 
       And ( SBSD_PRE.fsa_county_identifier <> SBSD_CUST_MAIN.fsa_county_identifier Or SBSD_PRE.core_customer_identifier <> SBSD_CUST_MAIN.core_customer_identifier )
     )
     UNION
   Select * FROM `fsa-{env}-sbsd-cdc`.`subsidiary_customer`  where   subsidiary_customer.OP ='I'
 ) Paid_subsidiary_customer
/* Need to do Inner join untill we reach primary table */
JOIN (SELECT *
      FROM `fsa-{env}-sbsd-cdc`.`payment_attribution_simulation`
      WHERE parent_payment_attribution_simulation_identifier is NULL
     ) UNQ_payment_attribution_simulation
ON (UNQ_payment_attribution_simulation.paid_subsidiary_customer_identifier = Paid_subsidiary_customer.subsidiary_customer_identifier)
JOIN `fsa-{env}-sbsd-cdc`.`proposed_payment_simulation`
ON (proposed_payment_simulation.proposed_payment_simulation_identifier = UNQ_payment_attribution_simulation.proposed_payment_simulation_identifier)
LEFT JOIN `fsa-{env}-sbsd-cdc`.`paid_fsa_county`
ON (Paid_subsidiary_customer.fsa_county_identifier = Paid_fsa_county.fsa_county_identifier)
WHERE proposed_payment_simulation.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND proposed_payment_simulation.OP <> 'D'

UNION
SELECT 
proposed_payment_simulation.proposed_payment_simulation_identifier PRPS_PYMT_SIM_ID,
proposed_payment_simulation.previous_proposed_payment_simulation_identifier PRV_PRPS_PYMT_SIM_ID,
proposed_payment_simulation.record_reference_primary_identification_type_code RCD_REF_PRIM_ID_TYPE_CD,
proposed_payment_simulation.record_reference_secondary_identification_type_code RCD_REF_SCND_ID_TYPE_CD,
proposed_payment_simulation.state_fsa_code ST_FSA_CD,
proposed_payment_simulation.county_fsa_code CNTY_FSA_CD,
proposed_payment_simulation.accounting_program_code ACCT_PGM_CD,
Paid_subsidiary_customer.core_customer_identifier PAID_CORE_CUST_ID,
Paid_fsa_county.state_county_fsa_code PAID_CORE_CUST_ST_CNTY_FSA_CD,
proposed_payment_simulation.subsidiary_period_start_year SBSD_PRD_STRT_YR,
proposed_payment_simulation.processing_request_reference_number PROC_RQST_REF_NBR,
proposed_payment_simulation.record_reference_primary_identification RCD_REF_PRIM_ID,
proposed_payment_simulation.record_reference_secondary_identification RCD_REF_SCND_ID,
proposed_payment_simulation.commodity_code CMDY_CD,
proposed_payment_simulation.payment_amount PYMT_AMT,
proposed_payment_simulation.payment_limitation_indicator PYMT_LMT_IND,
proposed_payment_simulation.hierarchical_payment_limitation_indicator HRCH_PYMT_LMT_IND,
proposed_payment_simulation.persist_overpayment_attributions_indicator PRST_OPYMT_ATRB_IND,
proposed_payment_simulation.apply_3_ownership_level_limit_indicator APP_3_OWNSHP_LVL_LMT_IND,
proposed_payment_simulation.decimal_precision_number DEC_PRCS_NBR,
proposed_payment_simulation.payment_attribution_request_reason_text PYMT_ATRB_RQST_RSN_TXT,
proposed_payment_simulation.payment_attribution_reversed_date PYMT_ATRB_RVRS_DT,
proposed_payment_simulation.data_status_code DATA_STAT_CD,
proposed_payment_simulation.creation_date CRE_DT,
proposed_payment_simulation.creation_user_name CRE_USER_NM,
proposed_payment_simulation.last_change_date LAST_CHG_DT,
proposed_payment_simulation.last_change_user_name LAST_CHG_USER_NM,
proposed_payment_simulation.override_subsidiary_period_start_year OVRRD_SBSD_PRD_STRT_YR,
proposed_payment_simulation.limited_payment_program_identifier PYMT_PGM_ID,
proposed_payment_simulation.payment_limitation_year PYMT_LMT_YR,
proposed_payment_simulation.simulation_scheduled_end_date SIM_SCHD_END_DT,
proposed_payment_simulation.attribution_simulation_payment_type_code ATRB_SIM_PYMT_TYPE_CD,
proposed_payment_simulation.application_id PGM_ENRL_APP_ID,
proposed_payment_simulation.obligation_confirmation_number OBL_CNFRM_NBR,
proposed_payment_simulation.OP,
3 AS TBL_PRIORITY
FROM (  SELECT 
    fsa_county_identifier,
    state_county_fsa_code,
    last_change_date, 
    last_change_user_name,
    Paid_fsa_county.OP 
    FROM (
    SELECT *,ROW_NUMBER() OVER (PARTITION BY (fsa_county_identifier)
            ORDER BY LAST_CHG_DT desc) as rnum 
            FROM `fsa-{env}-sbsd-cdc`.`fsa_county`) as SubQry
    WHERE SubQry.rnum=1
) Paid_fsa_county
/* Need to do Inner join untill we reach primary table */
JOIN  `fsa-{env}-sbsd-cdc`.`paid_subsidiary_customer`
ON (Paid_subsidiary_customer.fsa_county_identifier = Paid_fsa_county.fsa_county_identifier)
JOIN (SELECT *
      FROM `fsa-{env}-sbsd-cdc`.`payment_attribution_simulation`
      WHERE parent_payment_attribution_simulation_identifier is NULL
      ) UNQ_payment_attribution_simulation
ON (UNQ_payment_attribution_simulation.paid_subsidiary_customer_identifier = Paid_subsidiary_customer.subsidiary_customer_identifier)
JOIN `fsa-{env}-sbsd-cdc`.`proposed_payment_simulation`
ON (proposed_payment_simulation.proposed_payment_simulation_identifier = UNQ_payment_attribution_simulation.proposed_payment_simulation_identifier)
WHERE proposed_payment_simulation.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND proposed_payment_simulation.OP <> 'D'

) STG_ALL
) STG_UNQ
WHERE row_num_part = 1
AND (
 (COALESCE(CAST(CRE_DT AS DATE), DATE '1900-01-01') <= CAST('{ETL_START_DATE}'  AS DATE))
    AND
    (COALESCE(CAST(LAST_CHG_DT AS DATE), DATE '1900-01-01') <= CAST('{ETL_START_DATE}'  AS DATE))
)
UNION
SELECT 
proposed_payment_simulation.proposed_payment_simulation_identifier PRPS_PYMT_SIM_ID,
proposed_payment_simulation.previous_proposed_payment_simulation_identifier PRV_PRPS_PYMT_SIM_ID,
proposed_payment_simulation.record_reference_primary_identification_type_code RCD_REF_PRIM_ID_TYPE_CD,
proposed_payment_simulation.record_reference_secondary_identification_type_code RCD_REF_SCND_ID_TYPE_CD,
proposed_payment_simulation.state_fsa_code ST_FSA_CD,
proposed_payment_simulation.county_fsa_code CNTY_FSA_CD,
proposed_payment_simulation.accounting_program_code ACCT_PGM_CD,
NULL PAID_CORE_CUST_ID,
NULL PAID_CORE_CUST_ST_CNTY_FSA_CD,
proposed_payment_simulation.subsidiary_period_start_year SBSD_PRD_STRT_YR,
proposed_payment_simulation.processing_request_reference_number PROC_RQST_REF_NBR,
proposed_payment_simulation.record_reference_primary_identification RCD_REF_PRIM_ID,
proposed_payment_simulation.record_reference_secondary_identification RCD_REF_SCND_ID,
proposed_payment_simulation.commodity_code CMDY_CD,
proposed_payment_simulation.payment_amount PYMT_AMT,
proposed_payment_simulation.payment_limitation_indicator PYMT_LMT_IND,
proposed_payment_simulation.hierarchical_payment_limitation_indicator HRCH_PYMT_LMT_IND,
proposed_payment_simulation.persist_overpayment_attributions_indicator PRST_OPYMT_ATRB_IND,
proposed_payment_simulation.apply_3_ownership_level_limit_indicator APP_3_OWNSHP_LVL_LMT_IND,
proposed_payment_simulation.decimal_precision_number DEC_PRCS_NBR,
proposed_payment_simulation.payment_attribution_request_reason_text PYMT_ATRB_RQST_RSN_TXT,
proposed_payment_simulation.payment_attribution_reversed_date PYMT_ATRB_RVRS_DT,
proposed_payment_simulation.data_status_code DATA_STAT_CD,
proposed_payment_simulation.creation_date CRE_DT,
proposed_payment_simulation.creation_user_name CRE_USER_NM,
proposed_payment_simulation.last_change_date LAST_CHG_DT,
proposed_payment_simulation.last_change_user_name LAST_CHG_USER_NM,
proposed_payment_simulation.override_subsidiary_period_start_year OVRRD_SBSD_PRD_STRT_YR,
proposed_payment_simulation.limited_payment_program_identifier PYMT_PGM_ID,
proposed_payment_simulation.payment_limitation_year PYMT_LMT_YR,
proposed_payment_simulation.simulation_scheduled_end_date SIM_SCHD_END_DT,
proposed_payment_simulation.attribution_simulation_payment_type_code ATRB_SIM_PYMT_TYPE_CD,
proposed_payment_simulation.application_id PGM_ENRL_APP_ID,
proposed_payment_simulation.obligation_confirmation_number OBL_CNFRM_NBR,
'D' OP,
1 AS row_num_part
FROM `fsa-{env}-sbsd-cdc`.`proposed_payment_simulation`
WHERE proposed_payment_simulation.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND proposed_payment_simulation.OP = 'D')