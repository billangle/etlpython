SELECT
ACCT_PGM_CD,
APP_3_OWNSHP_LVL_LMT_IND,
CMDY_CD,
CNTY_FSA_CD,
CRE_DT,
CRE_USER_NM,
DATA_STAT_CD,
DEC_PRCS_NBR,
HRCHL_PYMT_LMT_IND,
LAST_CHG_DT,
LAST_CHG_USER_NM,
OVRRD_SBSD_PRD_STRT_YR,
PAID_CORE_CUST_ID,
PAID_CORE_CUST_ST_CNTY_FSA_CD,
PROC_RQST_REF_NBR,
PRST_OPYMT_ATRB_IND,
PRV_PRPS_PYMT_ACCT_PGM_CD,
PRV_PRPS_PYMT_CNTY_FSA_CD,
PRV_PRPS_PYMT_ID,
PRV_PRPS_PYMT_SBSD_PRD_STRT_YR,
PRV_PRPS_PYMT_ST_FSA_CD,
PYMT_AMT,
PYMT_ATRB_RQST_RSN_TXT,
PYMT_ATRB_RVRS_DT,
PYMT_LMT_IND,
PYMT_LMT_YR,
PYMT_PGM_ID,
RCD_REF_PRIM_ID,
RCD_REF_PRIM_ID_TYPE_CD,
RCD_REF_SCND_ID,
RCD_REF_SCND_ID_TYPE_CD,
SBSD_PRD_STRT_YR,
ST_FSA_CD,
UCHG_ATRB_PRPS_PYMT_LOG_ID,
CDC_OPER_CD
FROM (
Select  ACCT_PGM_CD,
APP_3_OWNSHP_LVL_LMT_IND,
CMDY_CD,
CNTY_FSA_CD,
CRE_DT,
CRE_USER_NM,
DATA_STAT_CD,
DEC_PRCS_NBR,
HRCHL_PYMT_LMT_IND,
LAST_CHG_DT,
LAST_CHG_USER_NM,
OVRRD_SBSD_PRD_STRT_YR,
PAID_CORE_CUST_ID,
PAID_CORE_CUST_ST_CNTY_FSA_CD,
PROC_RQST_REF_NBR,
PRST_OPYMT_ATRB_IND,
PRV_PRPS_PYMT_ACCT_PGM_CD,
PRV_PRPS_PYMT_CNTY_FSA_CD,
PRV_PRPS_PYMT_ID,
PRV_PRPS_PYMT_SBSD_PRD_STRT_YR,
PRV_PRPS_PYMT_ST_FSA_CD,
PYMT_AMT,
PYMT_ATRB_RQST_RSN_TXT,
PYMT_ATRB_RVRS_DT,
PYMT_LMT_IND,
PYMT_LMT_YR,
PYMT_PGM_ID,
RCD_REF_PRIM_ID,
RCD_REF_PRIM_ID_TYPE_CD,
RCD_REF_SCND_ID,
RCD_REF_SCND_ID_TYPE_CD,
SBSD_PRD_STRT_YR,
ST_FSA_CD,
UCHG_ATRB_PRPS_PYMT_LOG_ID,
op as CDC_OPER_CD,
ROW_NUMBER() over ( partition by  UCHG_ATRB_PRPS_PYMT_LOG_ID order by TBL_PRIORITY ASC, LAST_CHG_DT DESC ) AS row_num_part
FROM
(

SELECT 
unchanged_attribution_proposed_payment_log.accounting_program_code ACCT_PGM_CD,
unchanged_attribution_proposed_payment_log.apply_3_ownership_level_limit_indicator APP_3_OWNSHP_LVL_LMT_IND,
unchanged_attribution_proposed_payment_log.commodity_code CMDY_CD,
unchanged_attribution_proposed_payment_log.county_fsa_code CNTY_FSA_CD,
unchanged_attribution_proposed_payment_log.creation_date CRE_DT,
unchanged_attribution_proposed_payment_log.creation_user_name CRE_USER_NM,
unchanged_attribution_proposed_payment_log.data_status_code DATA_STAT_CD,
unchanged_attribution_proposed_payment_log.decimal_precision_number DEC_PRCS_NBR,
unchanged_attribution_proposed_payment_log.hierarchical_payment_limitation_indicator HRCHL_PYMT_LMT_IND,
unchanged_attribution_proposed_payment_log.last_change_date LAST_CHG_DT,
unchanged_attribution_proposed_payment_log.last_change_user_name LAST_CHG_USER_NM,
unchanged_attribution_proposed_payment_log.override_subsidiary_period_start_year OVRRD_SBSD_PRD_STRT_YR,
Paid_subsidiary_customer.core_customer_identifier PAID_CORE_CUST_ID,
paid_fsa_county.state_county_fsa_code PAID_CORE_CUST_ST_CNTY_FSA_CD,
unchanged_attribution_proposed_payment_log.processing_request_reference_number PROC_RQST_REF_NBR,
unchanged_attribution_proposed_payment_log.persist_overpayment_attributions_indicator PRST_OPYMT_ATRB_IND,
proposed_payment.accounting_program_code PRV_PRPS_PYMT_ACCT_PGM_CD,
proposed_payment.county_fsa_code PRV_PRPS_PYMT_CNTY_FSA_CD,
unchanged_attribution_proposed_payment_log.previous_proposed_payment_identifier PRV_PRPS_PYMT_ID,
proposed_payment.subsidiary_period_start_year PRV_PRPS_PYMT_SBSD_PRD_STRT_YR,
proposed_payment.state_fsa_code PRV_PRPS_PYMT_ST_FSA_CD,
unchanged_attribution_proposed_payment_log.payment_amount PYMT_AMT,
unchanged_attribution_proposed_payment_log.payment_attribution_request_reason_text PYMT_ATRB_RQST_RSN_TXT,
unchanged_attribution_proposed_payment_log.payment_attribution_reversed_date PYMT_ATRB_RVRS_DT,
unchanged_attribution_proposed_payment_log.payment_limitation_indicator PYMT_LMT_IND,
unchanged_attribution_proposed_payment_log.payment_limitation_year PYMT_LMT_YR,
unchanged_attribution_proposed_payment_log.limited_payment_program_identifier PYMT_PGM_ID,
unchanged_attribution_proposed_payment_log.record_reference_primary_identification RCD_REF_PRIM_ID,
unchanged_attribution_proposed_payment_log.record_reference_primary_identification_type_code RCD_REF_PRIM_ID_TYPE_CD,
unchanged_attribution_proposed_payment_log.record_reference_secondary_identification RCD_REF_SCND_ID,
unchanged_attribution_proposed_payment_log.record_reference_secondary_identification_type_code RCD_REF_SCND_ID_TYPE_CD,
unchanged_attribution_proposed_payment_log.subsidiary_period_start_year SBSD_PRD_STRT_YR,
unchanged_attribution_proposed_payment_log.state_fsa_code ST_FSA_CD,
unchanged_attribution_proposed_payment_log.unchanged_attribution_proposed_payment_log_identifier UCHG_ATRB_PRPS_PYMT_LOG_ID,
unchanged_attribution_proposed_payment_log.op,
1 AS TBL_PRIORITY
FROM `fsa-{env}-sbsd-cdc`.`unchanged_attribution_proposed_payment_log` 
left join `fsa-{env}-sbsd`.`proposed_payment`
on (unchanged_attribution_proposed_payment_log.previous_proposed_payment_identifier =proposed_payment.proposed_payment_identifier)
Left Join 
(SELECT *
                        FROM
                              (SELECT payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date,
                              row_number() over (partition by proposed_payment_identifier order by last_change_date desc) as rnk  
                              From (
                              Select payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
                              FROM `fsa-{env}-sbsd`.`payment_attribution`
                              WHERE 1 = 1
                              And parent_payment_attribution_identifier is NULL
                             UNION
                               Select payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
                              FROM `fsa-{env}-sbsd`.`payment_attribution_audit`
                              WHERE 1 = 1
                              And parent_payment_attribution_identifier is NULL 
                              ) attr_union
                              ) attr_aud_all
                        WHERE 1 = 1
                        AND rnk = 1
           ) payment_attribution  
on (proposed_payment.proposed_payment_identifier =payment_attribution.proposed_payment_identifier)
LEFT JOIN `fsa-{env}-sbsd`.`subsidiary_customer` paid_subsidiary_customer
ON (payment_attribution.paid_subsidiary_customer_identifier = paid_subsidiary_customer.subsidiary_customer_identifier)
    /*get paid customer fsa state county information*/
LEFT JOIN `fsa-{env}-sbsd`.`fsa_county paid_fsa_county`
ON (paid_subsidiary_customer.fsa_county_identifier = paid_fsa_county.fsa_county_identifier)
WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}' 
AND proposed_payment.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND proposed_payment.op <> 'D'
    
union

SELECT 
unchanged_attribution_proposed_payment_log.accounting_program_code ACCT_PGM_CD,
unchanged_attribution_proposed_payment_log.apply_3_ownership_level_limit_indicator APP_3_OWNSHP_LVL_LMT_IND,
unchanged_attribution_proposed_payment_log.commodity_code CMDY_CD,
unchanged_attribution_proposed_payment_log.county_fsa_code CNTY_FSA_CD,
unchanged_attribution_proposed_payment_log.creation_date CRE_DT,
unchanged_attribution_proposed_payment_log.creation_user_name CRE_USER_NM,
unchanged_attribution_proposed_payment_log.data_status_code DATA_STAT_CD,
unchanged_attribution_proposed_payment_log.decimal_precision_number DEC_PRCS_NBR,
unchanged_attribution_proposed_payment_log.hierarchical_payment_limitation_indicator HRCHL_PYMT_LMT_IND,
unchanged_attribution_proposed_payment_log.last_change_date LAST_CHG_DT,
unchanged_attribution_proposed_payment_log.last_change_user_name LAST_CHG_USER_NM,
unchanged_attribution_proposed_payment_log.override_subsidiary_period_start_year OVRRD_SBSD_PRD_STRT_YR,
Paid_subsidiary_customer.core_customer_identifier PAID_CORE_CUST_ID,
paid_fsa_county.state_county_fsa_code PAID_CORE_CUST_ST_CNTY_FSA_CD,
unchanged_attribution_proposed_payment_log.processing_request_reference_number PROC_RQST_REF_NBR,
unchanged_attribution_proposed_payment_log.persist_overpayment_attributions_indicator PRST_OPYMT_ATRB_IND,
proposed_payment.accounting_program_code PRV_PRPS_PYMT_ACCT_PGM_CD,
proposed_payment.county_fsa_code PRV_PRPS_PYMT_CNTY_FSA_CD,
unchanged_attribution_proposed_payment_log.previous_proposed_payment_identifier PRV_PRPS_PYMT_ID,
proposed_payment.subsidiary_period_start_year PRV_PRPS_PYMT_SBSD_PRD_STRT_YR,
proposed_payment.state_fsa_code PRV_PRPS_PYMT_ST_FSA_CD,
unchanged_attribution_proposed_payment_log.payment_amount PYMT_AMT,
unchanged_attribution_proposed_payment_log.payment_attribution_request_reason_text PYMT_ATRB_RQST_RSN_TXT,
unchanged_attribution_proposed_payment_log.payment_attribution_reversed_date PYMT_ATRB_RVRS_DT,
unchanged_attribution_proposed_payment_log.payment_limitation_indicator PYMT_LMT_IND,
unchanged_attribution_proposed_payment_log.payment_limitation_year PYMT_LMT_YR,
unchanged_attribution_proposed_payment_log.limited_payment_program_identifier PYMT_PGM_ID,
unchanged_attribution_proposed_payment_log.record_reference_primary_identification RCD_REF_PRIM_ID,
unchanged_attribution_proposed_payment_log.record_reference_primary_identification_type_code RCD_REF_PRIM_ID_TYPE_CD,
unchanged_attribution_proposed_payment_log.record_reference_secondary_identification RCD_REF_SCND_ID,
unchanged_attribution_proposed_payment_log.record_reference_secondary_identification_type_code RCD_REF_SCND_ID_TYPE_CD,
unchanged_attribution_proposed_payment_log.subsidiary_period_start_year SBSD_PRD_STRT_YR,
unchanged_attribution_proposed_payment_log.state_fsa_code ST_FSA_CD,
unchanged_attribution_proposed_payment_log.unchanged_attribution_proposed_payment_log_identifier UCHG_ATRB_PRPS_PYMT_LOG_ID,
proposed_payment.op,
2 AS TBL_PRIORITY
FROM `fsa-{env}-sbsd-cdc`.`proposed_payment` 
join `fsa-{env}-sbsd`.`unchanged_attribution_proposed_payment_log`
 on (unchanged_attribution_proposed_payment_log.previous_proposed_payment_identifier = proposed_payment.proposed_payment_identifier)
Left Join
(SELECT *
                        FROM
                              (SELECT payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date,
                              row_number() over (partition by proposed_payment_identifier order by last_change_date desc) as rnk  
                              From (
                              Select payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
                              FROM `fsa-{env}-sbsd`.`payment_attribution`
                              WHERE 1 = 1
                              And parent_payment_attribution_identifier is NULL
                             UNION
                               Select payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
                              FROM `fsa-{env}-sbsd`.`payment_attribution_audit`
                              WHERE 1 = 1
                              And parent_payment_attribution_identifier is NULL 
                              ) attr_union
                              ) attr_aud_all
                        WHERE 1 = 1
                        AND rnk = 1
           ) payment_attribution  
on (proposed_payment.proposed_payment_identifier = payment_attribution.proposed_payment_identifier)
LEFT JOIN `fsa-{env}-sbsd`.`subsidiary_customer` paid_subsidiary_customer
ON (payment_attribution.paid_subsidiary_customer_identifier = paid_subsidiary_customer.subsidiary_customer_identifier)
    /*get paid customer fsa state county information*/
LEFT JOIN `fsa-{env}-sbsd`.`fsa_county paid_fsa_county`
ON paid_subsidiary_customer.fsa_county_identifier = paid_fsa_county.fsa_county_identifier
WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}' 
AND proposed_payment.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND proposed_payment.op <> 'D' 
UNION
SELECT 
unchanged_attribution_proposed_payment_log.accounting_program_code ACCT_PGM_CD,
unchanged_attribution_proposed_payment_log.apply_3_ownership_level_limit_indicator APP_3_OWNSHP_LVL_LMT_IND,
unchanged_attribution_proposed_payment_log.commodity_code CMDY_CD,
unchanged_attribution_proposed_payment_log.county_fsa_code CNTY_FSA_CD,
unchanged_attribution_proposed_payment_log.creation_date CRE_DT,
unchanged_attribution_proposed_payment_log.creation_user_name CRE_USER_NM,
unchanged_attribution_proposed_payment_log.data_status_code DATA_STAT_CD,
unchanged_attribution_proposed_payment_log.decimal_precision_number DEC_PRCS_NBR,
unchanged_attribution_proposed_payment_log.hierarchical_payment_limitation_indicator HRCHL_PYMT_LMT_IND,
unchanged_attribution_proposed_payment_log.last_change_date LAST_CHG_DT,
unchanged_attribution_proposed_payment_log.last_change_user_name LAST_CHG_USER_NM,
unchanged_attribution_proposed_payment_log.override_subsidiary_period_start_year OVRRD_SBSD_PRD_STRT_YR,
Paid_subsidiary_customer.core_customer_identifier PAID_CORE_CUST_ID,
paid_fsa_county.state_county_fsa_code PAID_CORE_CUST_ST_CNTY_FSA_CD,
unchanged_attribution_proposed_payment_log.processing_request_reference_number PROC_RQST_REF_NBR,
unchanged_attribution_proposed_payment_log.persist_overpayment_attributions_indicator PRST_OPYMT_ATRB_IND,
proposed_payment.accounting_program_code PRV_PRPS_PYMT_ACCT_PGM_CD,
proposed_payment.county_fsa_code PRV_PRPS_PYMT_CNTY_FSA_CD,
unchanged_attribution_proposed_payment_log.previous_proposed_payment_identifier PRV_PRPS_PYMT_ID,
proposed_payment.subsidiary_period_start_year PRV_PRPS_PYMT_SBSD_PRD_STRT_YR,
proposed_payment.state_fsa_code PRV_PRPS_PYMT_ST_FSA_CD,
unchanged_attribution_proposed_payment_log.payment_amount PYMT_AMT,
unchanged_attribution_proposed_payment_log.payment_attribution_request_reason_text PYMT_ATRB_RQST_RSN_TXT,
unchanged_attribution_proposed_payment_log.payment_attribution_reversed_date PYMT_ATRB_RVRS_DT,
unchanged_attribution_proposed_payment_log.payment_limitation_indicator PYMT_LMT_IND,
unchanged_attribution_proposed_payment_log.payment_limitation_year PYMT_LMT_YR,
unchanged_attribution_proposed_payment_log.limited_payment_program_identifier PYMT_PGM_ID,
unchanged_attribution_proposed_payment_log.record_reference_primary_identification RCD_REF_PRIM_ID,
unchanged_attribution_proposed_payment_log.record_reference_primary_identification_type_code RCD_REF_PRIM_ID_TYPE_CD,
unchanged_attribution_proposed_payment_log.record_reference_secondary_identification RCD_REF_SCND_ID,
unchanged_attribution_proposed_payment_log.record_reference_secondary_identification_type_code RCD_REF_SCND_ID_TYPE_CD,
unchanged_attribution_proposed_payment_log.subsidiary_period_start_year SBSD_PRD_STRT_YR,
unchanged_attribution_proposed_payment_log.state_fsa_code ST_FSA_CD,
unchanged_attribution_proposed_payment_log.unchanged_attribution_proposed_payment_log_identifier UCHG_ATRB_PRPS_PYMT_LOG_ID,
SBSD_PRE.op,
3 AS TBL_PRIORITY
FROM 
  ( 
    Select SBSD_PRE.* from (
Select * FROM `fsa-{env}-sbsd-cdc`.`subsidiary_customer` WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}' and op ='UN' And DATE '{ETL_END_DATE}'  <> cast(current_date as date)
     UNION
     /*same day scenario update happens*/
    Select * FROM `fsa-{env}-sbsd-cdc`.`subsidiary_customer` WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}' and op ='UN'
       And DATE '{ETL_END_DATE}'  = cast(current_date as date)
     )  SBSD_PRE
  JOIN
   ( Select * from `fsa-{env}-sbsd`.`subsidiary_customer`   
   ) SBSD_CUST_MAIN
ON  ( SBSD_PRE.subsidiary_customer_identifier = SBSD_CUST_MAIN.subsidiary_customer_identifier 
       And ( SBSD_PRE.fsa_county_identifier <> SBSD_CUST_MAIN.fsa_county_identifier Or SBSD_PRE.core_customer_identifier <> SBSD_CUST_MAIN.core_customer_identifier )
     )
     UNION
   Select * FROM `fsa-{env}-sbsd-cdc`.`subsidiary_customer` WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'  and   op ='I'
 ) paid_subsidiary_customer
Join
(SELECT *
                        FROM
                              (SELECT payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date,
                              row_number() over (partition by proposed_payment_identifier order by last_change_date desc) as rnk  
                              From (
                              Select payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
                              FROM `fsa-{env}-sbsd`.`payment_attribution`
                              WHERE 1 = 1
                              And parent_payment_attribution_identifier is NULL
                             UNION
                               Select payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
                              FROM `fsa-{env}-sbsd`.`payment_attribution_audit`
                              WHERE 1 = 1
                              And parent_payment_attribution_identifier is NULL 
                              ) attr_union
                              ) attr_aud_all
                        WHERE 1 = 1
                        AND rnk = 1
           ) payment_attribution  
 ON (payment_attribution.paid_subsidiary_customer_identifier = paid_subsidiary_customer.subsidiary_customer_identifier)         
Join `fsa-{env}-sbsd`.`proposed_payment`
on (proposed_payment.proposed_payment_identifier =payment_attribution.proposed_payment_identifier)
join `fsa-{env}-sbsd`.`unchanged_attribution_proposed_payment_log`
on (unchanged_attribution_proposed_payment_log.previous_proposed_payment_identifier = proposed_payment.proposed_payment_identifier)
    /*get paid customer fsa state county information*/
LEFT JOIN `fsa-{env}-sbsd`.`fsa_county paid_fsa_county`
ON (paid_subsidiary_customer.fsa_county_identifier = paid_fsa_county.fsa_county_identifier)
WHERE SBSD_PRE.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND SBSD_PRE.op <> 'D'

union

SELECT 
unchanged_attribution_proposed_payment_log.accounting_program_code ACCT_PGM_CD,
unchanged_attribution_proposed_payment_log.apply_3_ownership_level_limit_indicator APP_3_OWNSHP_LVL_LMT_IND,
unchanged_attribution_proposed_payment_log.commodity_code CMDY_CD,
unchanged_attribution_proposed_payment_log.county_fsa_code CNTY_FSA_CD,
unchanged_attribution_proposed_payment_log.creation_date CRE_DT,
unchanged_attribution_proposed_payment_log.creation_user_name CRE_USER_NM,
unchanged_attribution_proposed_payment_log.data_status_code DATA_STAT_CD,
unchanged_attribution_proposed_payment_log.decimal_precision_number DEC_PRCS_NBR,
unchanged_attribution_proposed_payment_log.hierarchical_payment_limitation_indicator HRCHL_PYMT_LMT_IND,
unchanged_attribution_proposed_payment_log.last_change_date LAST_CHG_DT,
unchanged_attribution_proposed_payment_log.last_change_user_name LAST_CHG_USER_NM,
unchanged_attribution_proposed_payment_log.override_subsidiary_period_start_year OVRRD_SBSD_PRD_STRT_YR,
Paid_subsidiary_customer.core_customer_identifier PAID_CORE_CUST_ID,
paid_fsa_county.state_county_fsa_code PAID_CORE_CUST_ST_CNTY_FSA_CD,
unchanged_attribution_proposed_payment_log.processing_request_reference_number PROC_RQST_REF_NBR,
unchanged_attribution_proposed_payment_log.persist_overpayment_attributions_indicator PRST_OPYMT_ATRB_IND,
proposed_payment.accounting_program_code PRV_PRPS_PYMT_ACCT_PGM_CD,
proposed_payment.county_fsa_code PRV_PRPS_PYMT_CNTY_FSA_CD,
unchanged_attribution_proposed_payment_log.previous_proposed_payment_identifier PRV_PRPS_PYMT_ID,
proposed_payment.subsidiary_period_start_year PRV_PRPS_PYMT_SBSD_PRD_STRT_YR,
proposed_payment.state_fsa_code PRV_PRPS_PYMT_ST_FSA_CD,
unchanged_attribution_proposed_payment_log.payment_amount PYMT_AMT,
unchanged_attribution_proposed_payment_log.payment_attribution_request_reason_text PYMT_ATRB_RQST_RSN_TXT,
unchanged_attribution_proposed_payment_log.payment_attribution_reversed_date PYMT_ATRB_RVRS_DT,
unchanged_attribution_proposed_payment_log.payment_limitation_indicator PYMT_LMT_IND,
unchanged_attribution_proposed_payment_log.payment_limitation_year PYMT_LMT_YR,
unchanged_attribution_proposed_payment_log.limited_payment_program_identifier PYMT_PGM_ID,
unchanged_attribution_proposed_payment_log.record_reference_primary_identification RCD_REF_PRIM_ID,
unchanged_attribution_proposed_payment_log.record_reference_primary_identification_type_code RCD_REF_PRIM_ID_TYPE_CD,
unchanged_attribution_proposed_payment_log.record_reference_secondary_identification RCD_REF_SCND_ID,
unchanged_attribution_proposed_payment_log.record_reference_secondary_identification_type_code RCD_REF_SCND_ID_TYPE_CD,
unchanged_attribution_proposed_payment_log.subsidiary_period_start_year SBSD_PRD_STRT_YR,
unchanged_attribution_proposed_payment_log.state_fsa_code ST_FSA_CD,
unchanged_attribution_proposed_payment_log.unchanged_attribution_proposed_payment_log_identifier UCHG_ATRB_PRPS_PYMT_LOG_ID,
paid_fsa_county.op,
4 AS TBL_PRIORITY
FROM (  SELECT 
    fsa_county_identifier,
    state_county_fsa_code,
    last_change_date, 
    last_change_user_name,
    paid_fsa_county.op 
    FROM (
    SELECT *,ROW_NUMBER() OVER (PARTITION BY fsa_county_identifier  order by LAST_CHG_DT desc) as rnum
            FROM `fsa-{env}-sbsd`.`fsa_county` WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}') as SubQry
    WHERE SubQry.rnum=1
) paid_fsa_county 
JOIN `fsa-{env}-sbsd`.`subsidiary_customer` paid_subsidiary_customer
ON (paid_subsidiary_customer.fsa_county_identifier = paid_fsa_county.fsa_county_identifier)
Join (SELECT *
                        FROM
                              (SELECT payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date,
                              row_number() over (partition by proposed_payment_identifier order by last_change_date desc) as rnk  
                              From (
                              Select payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
                              FROM `fsa-{env}-sbsd`.`payment_attribution`
                              WHERE 1 = 1
                              And parent_payment_attribution_identifier is NULL
                             UNION
                               Select payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
                              FROM `fsa-{env}-sbsd`.`payment_attribution_audit`
                              WHERE 1 = 1
                              And parent_payment_attribution_identifier is NULL 
                              ) attr_union
                              ) attr_aud_all
                        WHERE 1 = 1
                        AND rnk = 1
           ) payment_attribution  
ON (payment_attribution.paid_subsidiary_customer_identifier = paid_subsidiary_customer.subsidiary_customer_identifier)
Join `fsa-{env}-sbsd`.`proposed_payment`
on (proposed_payment.proposed_payment_identifier =payment_attribution.proposed_payment_identifier)
join `fsa-{env}-sbsd`.`unchanged_attribution_proposed_payment_log`
on (unchanged_attribution_proposed_payment_log.previous_proposed_payment_identifier = proposed_payment.proposed_payment_identifier)

    /*get paid customer fsa state county information*/
WHERE paid_fsa_county.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND paid_fsa_county.op <> 'D'

) STG_ALL
) STG_UNQ
where row_num_part = 1
AND (
 (COALESCE(CAST(CRE_DT AS DATE), DATE '1900-01-01') <= CAST('{ETL_START_DATE}'  AS DATE))
    AND
    (COALESCE(CAST(LAST_CHG_DT AS DATE), DATE '1900-01-01') <= CAST('{ETL_START_DATE}'  AS DATE))
)

UNION

SELECT 
unchanged_attribution_proposed_payment_log.accounting_program_code ACCT_PGM_CD,
unchanged_attribution_proposed_payment_log.apply_3_ownership_level_limit_indicator APP_3_OWNSHP_LVL_LMT_IND,
unchanged_attribution_proposed_payment_log.commodity_code CMDY_CD,
unchanged_attribution_proposed_payment_log.county_fsa_code CNTY_FSA_CD,
unchanged_attribution_proposed_payment_log.creation_date CRE_DT,
unchanged_attribution_proposed_payment_log.creation_user_name CRE_USER_NM,
unchanged_attribution_proposed_payment_log.data_status_code DATA_STAT_CD,
unchanged_attribution_proposed_payment_log.decimal_precision_number DEC_PRCS_NBR,
unchanged_attribution_proposed_payment_log.hierarchical_payment_limitation_indicator HRCHL_PYMT_LMT_IND,
unchanged_attribution_proposed_payment_log.last_change_date LAST_CHG_DT,
unchanged_attribution_proposed_payment_log.last_change_user_name LAST_CHG_USER_NM,
unchanged_attribution_proposed_payment_log.override_subsidiary_period_start_year OVRRD_SBSD_PRD_STRT_YR,
NULL PAID_CORE_CUST_ID,
NULL PAID_CORE_CUST_ST_CNTY_FSA_CD,
unchanged_attribution_proposed_payment_log.processing_request_reference_number PROC_RQST_REF_NBR,
unchanged_attribution_proposed_payment_log.persist_overpayment_attributions_indicator PRST_OPYMT_ATRB_IND,
NULL PRV_PRPS_PYMT_ACCT_PGM_CD,
NULL PRV_PRPS_PYMT_CNTY_FSA_CD,
unchanged_attribution_proposed_payment_log.previous_proposed_payment_identifier PRV_PRPS_PYMT_ID,
NULL PRV_PRPS_PYMT_SBSD_PRD_STRT_YR,
NULL PRV_PRPS_PYMT_ST_FSA_CD,
unchanged_attribution_proposed_payment_log.payment_amount PYMT_AMT,
unchanged_attribution_proposed_payment_log.payment_attribution_request_reason_text PYMT_ATRB_RQST_RSN_TXT,
unchanged_attribution_proposed_payment_log.payment_attribution_reversed_date PYMT_ATRB_RVRS_DT,
unchanged_attribution_proposed_payment_log.payment_limitation_indicator PYMT_LMT_IND,
unchanged_attribution_proposed_payment_log.payment_limitation_year PYMT_LMT_YR,
unchanged_attribution_proposed_payment_log.limited_payment_program_identifier PYMT_PGM_ID,
unchanged_attribution_proposed_payment_log.record_reference_primary_identification RCD_REF_PRIM_ID,
unchanged_attribution_proposed_payment_log.record_reference_primary_identification_type_code RCD_REF_PRIM_ID_TYPE_CD,
unchanged_attribution_proposed_payment_log.record_reference_primary_identification_type_code RCD_REF_PRIM_ID_TYPE_CD,
unchanged_attribution_proposed_payment_log.record_reference_secondary_identification RCD_REF_SCND_ID,
unchanged_attribution_proposed_payment_log.record_reference_secondary_identification_type_code RCD_REF_SCND_ID_TYPE_CD,
unchanged_attribution_proposed_payment_log.subsidiary_period_start_year SBSD_PRD_STRT_YR,
unchanged_attribution_proposed_payment_log.state_fsa_code ST_FSA_CD,
unchanged_attribution_proposed_payment_log.unchanged_attribution_proposed_payment_log_identifier UCHG_ATRB_PRPS_PYMT_LOG_ID,
'D' OP,
1 AS row_num_part
FROM `fsa-{env}-sbsd-cdc`.`unchanged_attribution_proposed_payment_log` 
WHERE unchanged_attribution_proposed_payment_log.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}' 
AND unchanged_attribution_proposed_payment_log.op = 'D'