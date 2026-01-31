SELECT 
PRPS_PYMT_ID,
PRV_PRPS_PYMT_ID,
ACCT_PGM_CD,
APP_3_OWNSHP_LVL_LMT_IND,
CMDY_CD,
CNTY_FSA_CD,
CRE_DT,
CRE_USER_NM,
DATA_STAT_CD,
DEC_PRCS_NBR,
HRCH_PYMT_LMT_IND,
LAST_CHG_DT,
LAST_CHG_USER_NM,
OBL_CNFRM_NBR,
OVRRD_SBSD_PRD_STRT_YR,
PAID_CORE_CUST_ID,
PAID_CORE_CUST_ST_CNTY_FSA_CD,
PROC_RQST_REF_NBR,
PRST_OPYMT_ATRB_IND,
PRV_PRPS_PYMT_ACCT_PGM_CD,
PRV_PRPS_PYMT_CNTY_FSA_CD,
PRV_PRPS_PYMT_CORE_CUST_ID,
PRV_PRPS_PYMT_SBSD_PRD_STRT_YR,
PRV_PRPS_PYMT_ST_FSA_CD,
PRV_PYMT_CUST_ST_CNTY_FSA_CD,
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
CDC_OPER_CD,
''  HASH_DIF,
current_timestamp LOAD_DT,
'CNSV'  DATA_SRC_NM,
'{ETL_START_DATE}' AS CDC_DT 
FROM (
SELECT * FROM
(
SELECT 
PRPS_PYMT_ID,
PRV_PRPS_PYMT_ID,
ACCT_PGM_CD,
APP_3_OWNSHP_LVL_LMT_IND,
CMDY_CD,
CNTY_FSA_CD,
CRE_DT,
CRE_USER_NM,
DATA_STAT_CD,
DEC_PRCS_NBR,
HRCH_PYMT_LMT_IND,
LAST_CHG_DT,
LAST_CHG_USER_NM,
OBL_CNFRM_NBR,
OVRRD_SBSD_PRD_STRT_YR,
PAID_CORE_CUST_ID,
PAID_CORE_CUST_ST_CNTY_FSA_CD,
PROC_RQST_REF_NBR,
PRST_OPYMT_ATRB_IND,
PRV_PRPS_PYMT_ACCT_PGM_CD,
PRV_PRPS_PYMT_CNTY_FSA_CD,
PRV_PRPS_PYMT_CORE_CUST_ID,
PRV_PRPS_PYMT_SBSD_PRD_STRT_YR,
PRV_PRPS_PYMT_ST_FSA_CD,
PRV_PYMT_CUST_ST_CNTY_FSA_CD,
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
OP CDC_OPER_CD,
ROW_NUMBER() over ( partition by PRPS_PYMT_ID order by TBL_PRIORITY ASC, LAST_CHG_DT DESC ) AS row_num_part
FROM
(
Select 
proposed_payment.proposed_payment_identifier PRPS_PYMT_ID,
proposed_payment.previous_proposed_payment_identifier PRV_PRPS_PYMT_ID,
proposed_payment.accounting_program_code ACCT_PGM_CD,
proposed_payment.apply_3_ownership_level_limit_indicator APP_3_OWNSHP_LVL_LMT_IND,
proposed_payment.commodity_code CMDY_CD,
proposed_payment.county_fsa_code CNTY_FSA_CD,
proposed_payment.creation_date CRE_DT,
proposed_payment.creation_user_name CRE_USER_NM,
proposed_payment.data_status_code DATA_STAT_CD,
proposed_payment.decimal_precision_number DEC_PRCS_NBR,
proposed_payment.hierarchical_payment_limitation_indicator HRCH_PYMT_LMT_IND,
proposed_payment.last_change_date LAST_CHG_DT,
proposed_payment.last_change_user_name LAST_CHG_USER_NM,
proposed_payment.obligation_confirmation_number OBL_CNFRM_NBR,
proposed_payment.override_subsidiary_period_start_year OVRRD_SBSD_PRD_STRT_YR,
Paid_subsidiary_customer.core_customer_identifier PAID_CORE_CUST_ID,
Paid_fsa_county.state_county_fsa_code PAID_CORE_CUST_ST_CNTY_FSA_CD,
proposed_payment.processing_request_reference_number PROC_RQST_REF_NBR,
proposed_payment.persist_overpayment_attributions_indicator PRST_OPYMT_ATRB_IND,
PRVS_proposed_payment.accounting_program_code PRV_PRPS_PYMT_ACCT_PGM_CD,
PRVS_proposed_payment.county_fsa_code PRV_PRPS_PYMT_CNTY_FSA_CD,
PRVS_PRPS_PYMT_subsidiary_customer.core_customer_identifier PRV_PRPS_PYMT_CORE_CUST_ID,
PRVS_proposed_payment.subsidiary_period_start_year PRV_PRPS_PYMT_SBSD_PRD_STRT_YR,
PRVS_proposed_payment.state_fsa_code PRV_PRPS_PYMT_ST_FSA_CD,
PRVS_PRPS_PYMT_fsa_county.state_county_fsa_code PRV_PYMT_CUST_ST_CNTY_FSA_CD,
proposed_payment.payment_amount PYMT_AMT,
proposed_payment.payment_attribution_request_reason_text PYMT_ATRB_RQST_RSN_TXT,
proposed_payment.payment_attribution_reversed_date PYMT_ATRB_RVRS_DT,
proposed_payment.payment_limitation_indicator PYMT_LMT_IND,
proposed_payment.payment_limitation_year PYMT_LMT_YR,
proposed_payment.limited_payment_program_identifier PYMT_PGM_ID,
proposed_payment.record_reference_primary_identification RCD_REF_PRIM_ID,
proposed_payment.record_reference_primary_identification_type_code RCD_REF_PRIM_ID_TYPE_CD,
proposed_payment.record_reference_secondary_identification RCD_REF_SCND_ID,
proposed_payment.record_reference_secondary_identification_type_code RCD_REF_SCND_ID_TYPE_CD,
proposed_payment.subsidiary_period_start_year SBSD_PRD_STRT_YR,
proposed_payment.state_fsa_code ST_FSA_CD,
proposed_payment.OP,
1 AS TBL_PRIORITY
From `fsa-{env}-sbsd-cdc`.`proposed_payment`
Left join `fsa-{env}-sbsd-cdc`.`PRVS_proposed_payment`
ON (proposed_payment.previous_proposed_payment_identifier = PRVS_proposed_payment.proposed_payment_identifier)
LEFT JOIN 
            (SELECT *
                            FROM
                                  (SELECT payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date,
                                  row_number() over (partition by proposed_payment_identifier order by last_change_date desc) as rnk  
                                  From (
                                  Select payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
                                  FROM `fsa-{env}-sbsd-cdc`.`payment_attribution` 
                                  WHERE 1 = 1
                                  And parent_payment_attribution_identifier is NULL
                                 UNION
                                   Select payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
                                  FROM `fsa-{env}-sbsd-cdc`.`payment_attribution`_audit 
                                  WHERE 1 = 1
                                  And parent_payment_attribution_identifier is NULL 
                                  ) attr_union
                                  ) attr_aud_all
                            WHERE 1 = 1
                            AND rnk = 1
           ) payment_attribution
On (proposed_payment.proposed_payment_identifier = payment_attribution.proposed_payment_identifier)
Left join `fsa-{env}-sbsd-cdc`.`subsidiary_customer`
On (payment_attribution.paid_subsidiary_customer_identifier =Paid_subsidiary_customer.subsidiary_customer_identifier)
  LEFT JOIN 
            (SELECT *
                            FROM
                                  (SELECT payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date,
                                  row_number() over (partition by proposed_payment_identifier order by last_change_date desc) as rnk  
                                  From (
                                  Select payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
                                  FROM `fsa-{env}-sbsd-cdc`.`payment_attribution` 
                                  WHERE 1 = 1
                                  And parent_payment_attribution_identifier is NULL
                                 UNION
                                   Select payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
                                  FROM `fsa-{env}-sbsd-cdc`.`payment_attribution`_audit 
                                  WHERE 1 = 1
                                  And parent_payment_attribution_identifier is NULL 
                                  ) attr_union
                                  ) attr_aud_all
                            WHERE 1 = 1
                            AND rnk = 1
              ) PRVS_payment_attribution
On (proposed_payment.Previous_proposed_payment_identifier = PRVS_payment_attribution.proposed_payment_identifier)
Left Join `fsa-{env}-sbsd-cdc`.`subsidiary_customer`
On  (PRVS_payment_attribution.paid_subsidiary_customer_identifier = PRVS_PRPS_PYMT_subsidiary_customer.subsidiary_customer_identifier)
Left Join `fsa-{env}-sbsd-cdc`.`fsa_county`
On (Paid_subsidiary_customer.fsa_county_identifier =Paid_fsa_county.fsa_county_identifier)
Left join `fsa-{env}-sbsd-cdc`.`fsa_county`
On (PRVS_PRPS_PYMT_subsidiary_customer.fsa_county_identifier =  PRVS_PRPS_PYMT_fsa_county.fsa_county_identifier)
WHERE proposed_payment.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
  AND proposed_payment.OP <> 'D'
  /* And proposed_payment.proposed_payment_identifier =7372790 */

 Union

Select 
proposed_payment.proposed_payment_identifier PRPS_PYMT_ID,
proposed_payment.previous_proposed_payment_identifier PRV_PRPS_PYMT_ID,
proposed_payment.accounting_program_code ACCT_PGM_CD,
proposed_payment.apply_3_ownership_level_limit_indicator APP_3_OWNSHP_LVL_LMT_IND,
proposed_payment.commodity_code CMDY_CD,
proposed_payment.county_fsa_code CNTY_FSA_CD,
proposed_payment.creation_date CRE_DT,
proposed_payment.creation_user_name CRE_USER_NM,
proposed_payment.data_status_code DATA_STAT_CD,
proposed_payment.decimal_precision_number DEC_PRCS_NBR,
proposed_payment.hierarchical_payment_limitation_indicator HRCH_PYMT_LMT_IND,
proposed_payment.last_change_date LAST_CHG_DT,
proposed_payment.last_change_user_name LAST_CHG_USER_NM,
proposed_payment.obligation_confirmation_number OBL_CNFRM_NBR,
proposed_payment.override_subsidiary_period_start_year OVRRD_SBSD_PRD_STRT_YR,
Paid_subsidiary_customer.core_customer_identifier PAID_CORE_CUST_ID,
Paid_fsa_county.state_county_fsa_code PAID_CORE_CUST_ST_CNTY_FSA_CD,
proposed_payment.processing_request_reference_number PROC_RQST_REF_NBR,
proposed_payment.persist_overpayment_attributions_indicator PRST_OPYMT_ATRB_IND,
PRVS_proposed_payment.accounting_program_code PRV_PRPS_PYMT_ACCT_PGM_CD,
PRVS_proposed_payment.county_fsa_code PRV_PRPS_PYMT_CNTY_FSA_CD,
PRVS_PRPS_PYMT_subsidiary_customer.core_customer_identifier PRV_PRPS_PYMT_CORE_CUST_ID,
PRVS_proposed_payment.subsidiary_period_start_year PRV_PRPS_PYMT_SBSD_PRD_STRT_YR,
PRVS_proposed_payment.state_fsa_code PRV_PRPS_PYMT_ST_FSA_CD,
PRVS_PRPS_PYMT_fsa_county.state_county_fsa_code PRV_PYMT_CUST_ST_CNTY_FSA_CD,
proposed_payment.payment_amount PYMT_AMT,
proposed_payment.payment_attribution_request_reason_text PYMT_ATRB_RQST_RSN_TXT,
proposed_payment.payment_attribution_reversed_date PYMT_ATRB_RVRS_DT,
proposed_payment.payment_limitation_indicator PYMT_LMT_IND,
proposed_payment.payment_limitation_year PYMT_LMT_YR,
proposed_payment.limited_payment_program_identifier PYMT_PGM_ID,
proposed_payment.record_reference_primary_identification RCD_REF_PRIM_ID,
proposed_payment.record_reference_primary_identification_type_code RCD_REF_PRIM_ID_TYPE_CD,
proposed_payment.record_reference_secondary_identification RCD_REF_SCND_ID,
proposed_payment.record_reference_secondary_identification_type_code RCD_REF_SCND_ID_TYPE_CD,
proposed_payment.subsidiary_period_start_year SBSD_PRD_STRT_YR,
proposed_payment.state_fsa_code ST_FSA_CD,
proposed_payment.OP,
2 AS TBL_PRIORITY
From  
 ( 
   Select SBSD_PRE.* from (
Select * FROM `fsa-{env}-sbsd-cdc`.`subsidiary_customer`     where  subsidiary_customer.OP ='UN' And cast('{ETL_END_DATE}' as date)  <> cast(current_date as date)
     UNION
     /*same day scenario update happens*/
    Select * FROM `fsa-{env}-sbsd-cdc`.`subsidiary_customer` where  subsidiary_customer.OP ='UN'
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
join 
 (SELECT *
                   FROM
                         (SELECT payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date,
                         row_number() over (partition by proposed_payment_identifier order by last_change_date desc) as rnk  
                         From (
                         Select payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
                         FROM `fsa-{env}-sbsd-cdc`.`payment_attribution` 
                         WHERE 1 = 1
                         And parent_payment_attribution_identifier is NULL
                        UNION
                          Select payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
                         FROM `fsa-{env}-sbsd-cdc`.`payment_attribution`_audit 
                         WHERE 1 = 1
                         And parent_payment_attribution_identifier is NULL 
                         ) attr_union
                         ) attr_aud_all
                   WHERE 1 = 1
                   AND rnk = 1
           ) payment_attribution  
 On (payment_attribution.paid_subsidiary_customer_identifier =Paid_subsidiary_customer.subsidiary_customer_identifier)
  JOIN `fsa-{env}-sbsd-cdc`.`proposed_payment`
  On (proposed_payment.proposed_payment_identifier = payment_attribution.proposed_payment_identifier)
  Left  join `fsa-{env}-sbsd-cdc`.`PRVS_proposed_payment`
ON (proposed_payment.previous_proposed_payment_identifier = PRVS_proposed_payment.proposed_payment_identifier) 
LEFT JOIN 
         (SELECT *
                         FROM
                               (SELECT payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date,
                               row_number() over (partition by proposed_payment_identifier order by last_change_date desc) as rnk  
                               From (
                               Select payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
                               FROM `fsa-{env}-sbsd-cdc`.`payment_attribution` 
                               WHERE 1 = 1
                               And parent_payment_attribution_identifier is NULL
                              UNION
                                Select payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
                               FROM `fsa-{env}-sbsd-cdc`.`payment_attribution`_audit 
                               WHERE 1 = 1
                               And parent_payment_attribution_identifier is NULL 
                               ) attr_union
                               ) attr_aud_all
                         WHERE 1 = 1
                         AND rnk = 1
         ) PRVS_payment_attribution
On (proposed_payment.Previous_proposed_payment_identifier = PRVS_payment_attribution.proposed_payment_identifier)
  Left Join `fsa-{env}-sbsd-cdc`.`subsidiary_customer`
  On  (PRVS_payment_attribution.paid_subsidiary_customer_identifier = PRVS_PRPS_PYMT_subsidiary_customer.subsidiary_customer_identifier) 
 Left Join `fsa-{env}-sbsd-cdc`.`fsa_county`
On (Paid_subsidiary_customer.fsa_county_identifier =Paid_fsa_county.fsa_county_identifier)
Left join `fsa-{env}-sbsd-cdc`.`fsa_county`
On (PRVS_PRPS_PYMT_subsidiary_customer.fsa_county_identifier =  PRVS_PRPS_PYMT_fsa_county.fsa_county_identifier)
WHERE proposed_payment.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND proposed_payment.OP <> 'D'
/* And proposed_payment.proposed_payment_identifier =7372790 */

 Union

 Select 
proposed_payment.proposed_payment_identifier PRPS_PYMT_ID,
proposed_payment.previous_proposed_payment_identifier PRV_PRPS_PYMT_ID,
proposed_payment.accounting_program_code ACCT_PGM_CD,
proposed_payment.apply_3_ownership_level_limit_indicator APP_3_OWNSHP_LVL_LMT_IND,
proposed_payment.commodity_code CMDY_CD,
proposed_payment.county_fsa_code CNTY_FSA_CD,
proposed_payment.creation_date CRE_DT,
proposed_payment.creation_user_name CRE_USER_NM,
proposed_payment.data_status_code DATA_STAT_CD,
proposed_payment.decimal_precision_number DEC_PRCS_NBR,
proposed_payment.hierarchical_payment_limitation_indicator HRCH_PYMT_LMT_IND,
proposed_payment.last_change_date LAST_CHG_DT,
proposed_payment.last_change_user_name LAST_CHG_USER_NM,
proposed_payment.obligation_confirmation_number OBL_CNFRM_NBR,
proposed_payment.override_subsidiary_period_start_year OVRRD_SBSD_PRD_STRT_YR,
Paid_subsidiary_customer.core_customer_identifier PAID_CORE_CUST_ID,
Paid_fsa_county.state_county_fsa_code PAID_CORE_CUST_ST_CNTY_FSA_CD,
proposed_payment.processing_request_reference_number PROC_RQST_REF_NBR,
proposed_payment.persist_overpayment_attributions_indicator PRST_OPYMT_ATRB_IND,
PRVS_proposed_payment.accounting_program_code PRV_PRPS_PYMT_ACCT_PGM_CD,
PRVS_proposed_payment.county_fsa_code PRV_PRPS_PYMT_CNTY_FSA_CD,
PRVS_PRPS_PYMT_subsidiary_customer.core_customer_identifier PRV_PRPS_PYMT_CORE_CUST_ID,
PRVS_proposed_payment.subsidiary_period_start_year PRV_PRPS_PYMT_SBSD_PRD_STRT_YR,
PRVS_proposed_payment.state_fsa_code PRV_PRPS_PYMT_ST_FSA_CD,
PRVS_PRPS_PYMT_fsa_county.state_county_fsa_code PRV_PYMT_CUST_ST_CNTY_FSA_CD,
proposed_payment.payment_amount PYMT_AMT,
proposed_payment.payment_attribution_request_reason_text PYMT_ATRB_RQST_RSN_TXT,
proposed_payment.payment_attribution_reversed_date PYMT_ATRB_RVRS_DT,
proposed_payment.payment_limitation_indicator PYMT_LMT_IND,
proposed_payment.payment_limitation_year PYMT_LMT_YR,
proposed_payment.limited_payment_program_identifier PYMT_PGM_ID,
proposed_payment.record_reference_primary_identification RCD_REF_PRIM_ID,
proposed_payment.record_reference_primary_identification_type_code RCD_REF_PRIM_ID_TYPE_CD,
proposed_payment.record_reference_secondary_identification RCD_REF_SCND_ID,
proposed_payment.record_reference_secondary_identification_type_code RCD_REF_SCND_ID_TYPE_CD,
proposed_payment.subsidiary_period_start_year SBSD_PRD_STRT_YR,
proposed_payment.state_fsa_code ST_FSA_CD,
Paid_fsa_county.OP,
3 AS TBL_PRIORITY
From (   SELECT 
            fsa_county_identifier,
            state_county_fsa_code,
            last_change_date, 
            last_change_user_name,
            Paid_fsa_county.OP 
            FROM (
            SELECT *,ROW_NUMBER() OVER (PARTITION BY fsa_county_identifier
                        ORDER BY LAST_CHG_DT desc) as rnum 
                        FROM `fsa-{env}-sbsd-cdc`.`fsa_county`) as SubQry
            WHERE SubQry.rnum=1
) Paid_fsa_county
  /* Need to do Inner join untill we reach primary table */
  Join `fsa-{env}-sbsd-cdc`.`subsidiary_customer`
On (Paid_subsidiary_customer.fsa_county_identifier =Paid_fsa_county.fsa_county_identifier)
  join 
        (SELECT *
                        FROM
                              (SELECT payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date,
                              row_number() over (partition by proposed_payment_identifier order by last_change_date desc) as rnk  
                              From (
                              Select payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
                              FROM `fsa-{env}-sbsd-cdc`.`payment_attribution` 
                              WHERE 1 = 1
                              And parent_payment_attribution_identifier is NULL
                             UNION
                               Select payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
                              FROM `fsa-{env}-sbsd-cdc`.`payment_attribution`_audit 
                              WHERE 1 = 1
                              And parent_payment_attribution_identifier is NULL 
                              ) attr_union
                              ) attr_aud_all
                        WHERE 1 = 1
                        AND rnk = 1
           ) payment_attribution  
 On (payment_attribution.paid_subsidiary_customer_identifier =Paid_subsidiary_customer.subsidiary_customer_identifier)
    JOIN `fsa-{env}-sbsd-cdc`.`proposed_payment`
  On (proposed_payment.proposed_payment_identifier = payment_attribution.proposed_payment_identifier)
   Left join `fsa-{env}-sbsd-cdc`.`proposed_payment`
ON (proposed_payment.previous_proposed_payment_identifier = PRVS_proposed_payment.proposed_payment_identifier)   
LEFT JOIN 
            (SELECT *
                            FROM
                                  (SELECT payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date,
                                  row_number() over (partition by proposed_payment_identifier order by last_change_date desc) as rnk  
                                  From (
                                  Select payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
                                  FROM `fsa-{env}-sbsd-cdc`.`payment_attribution` 
                                  WHERE 1 = 1
                                  And parent_payment_attribution_identifier is NULL
                                 UNION
                                   Select payment_attribution_identifier,proposed_payment_identifier, subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
                                  FROM `fsa-{env}-sbsd-cdc`.`payment_attribution`_audit 
                                  WHERE 1 = 1
                                  And parent_payment_attribution_identifier is NULL 
                                  ) attr_union
                                  ) attr_aud_all
                            WHERE 1 = 1
                            AND rnk = 1
           ) PRVS_payment_attribution
On (proposed_payment.Previous_proposed_payment_identifier = PRVS_payment_attribution.proposed_payment_identifier)
   Left Join `fsa-{env}-sbsd-cdc`.`subsidiary_customer`
  On  (PRVS_payment_attribution.paid_subsidiary_customer_identifier = PRVS_PRPS_PYMT_subsidiary_customer.subsidiary_customer_identifier) 
  Left join `fsa-{env}-sbsd-cdc`.`fsa_county`
  On (PRVS_PRPS_PYMT_subsidiary_customer.fsa_county_identifier =  PRVS_PRPS_PYMT_fsa_county.fsa_county_identifier)
WHERE PRVS_PRPS_PYMT_subsidiary_customer.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND PRVS_PRPS_PYMT_subsidiary_customer.OP <> 'D'
/* And proposed_payment.proposed_payment_identifier =7372790 */

 ) STG_ALL
) STG_UNQ
WHERE row_num_part = 1
AND (
 (COALESCE(CAST(CRE_DT AS DATE), DATE '1900-01-01') <= CAST('{ETL_START_DATE}'  AS DATE))
    AND
    (COALESCE(CAST(LAST_CHG_DT AS DATE), DATE '1900-01-01') <= CAST('{ETL_START_DATE}'  AS DATE))
)
UNION
Select 
proposed_payment.proposed_payment_identifier PRPS_PYMT_ID,
proposed_payment.previous_proposed_payment_identifier PRV_PRPS_PYMT_ID,
proposed_payment.accounting_program_code ACCT_PGM_CD,
proposed_payment.apply_3_ownership_level_limit_indicator APP_3_OWNSHP_LVL_LMT_IND,
proposed_payment.commodity_code CMDY_CD,
proposed_payment.county_fsa_code CNTY_FSA_CD,
proposed_payment.creation_date CRE_DT,
proposed_payment.creation_user_name CRE_USER_NM,
proposed_payment.data_status_code DATA_STAT_CD,
proposed_payment.decimal_precision_number DEC_PRCS_NBR,
proposed_payment.hierarchical_payment_limitation_indicator HRCH_PYMT_LMT_IND,
proposed_payment.last_change_date LAST_CHG_DT,
proposed_payment.last_change_user_name LAST_CHG_USER_NM,
proposed_payment.obligation_confirmation_number OBL_CNFRM_NBR,
proposed_payment.override_subsidiary_period_start_year OVRRD_SBSD_PRD_STRT_YR,
NULL PAID_CORE_CUST_ID,
NULL PAID_CORE_CUST_ST_CNTY_FSA_CD,
proposed_payment.processing_request_reference_number PROC_RQST_REF_NBR,
proposed_payment.persist_overpayment_attributions_indicator PRST_OPYMT_ATRB_IND,
NULL PRV_PRPS_PYMT_ACCT_PGM_CD,
NULL PRV_PRPS_PYMT_CNTY_FSA_CD,
NULL PRV_PRPS_PYMT_CORE_CUST_ID,
NULL PRV_PRPS_PYMT_SBSD_PRD_STRT_YR,
NULL PRV_PRPS_PYMT_ST_FSA_CD,
NULL PRV_PYMT_CUST_ST_CNTY_FSA_CD,
proposed_payment.payment_amount PYMT_AMT,
proposed_payment.payment_attribution_request_reason_text PYMT_ATRB_RQST_RSN_TXT,
proposed_payment.payment_attribution_reversed_date PYMT_ATRB_RVRS_DT,
proposed_payment.payment_limitation_indicator PYMT_LMT_IND,
proposed_payment.payment_limitation_year PYMT_LMT_YR,
proposed_payment.limited_payment_program_identifier PYMT_PGM_ID,
proposed_payment.record_reference_primary_identification RCD_REF_PRIM_ID,
proposed_payment.record_reference_primary_identification_type_code RCD_REF_PRIM_ID_TYPE_CD,
proposed_payment.record_reference_secondary_identification RCD_REF_SCND_ID,
proposed_payment.record_reference_secondary_identification_type_code RCD_REF_SCND_ID_TYPE_CD,
proposed_payment.subsidiary_period_start_year SBSD_PRD_STRT_YR,
proposed_payment.state_fsa_code ST_FSA_CD,
'D' OP,
1 AS row_num_part
FROM `fsa-{env}-sbsd-cdc`.`proposed_payment`
WHERE proposed_payment.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND proposed_payment.OP = 'D')