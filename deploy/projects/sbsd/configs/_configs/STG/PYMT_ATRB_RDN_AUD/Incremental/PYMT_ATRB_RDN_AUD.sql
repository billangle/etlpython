SELECT PYMT_ATRB_RDN_ID,
PYMT_ATRB_ID,
PRPS_PYMT_ID,
ST_FSA_CD,
CNTY_FSA_CD,
ACCT_PGM_CD,
SBSD_PRD_STRT_YR,
MBR_CORE_CUST_ID,
MBR_CORE_CUST_ST_CNTY_FSA_CD,
PAID_CORE_CUST_ID,
PAID_CORE_CUST_ST_CNTY_FSA_CD,
PYMT_ATRB_RDN_TYPE_CD,
PYMT_ATRB_RDN_AMT,
TOT_PYMT_ATRB_RDN_AMT,
DATA_STAT_CD,
CRE_DT,
CRE_USER_NM,
LAST_CHG_DT,
LAST_CHG_USER_NM,
DATA_ACTN_CD,
DATA_ACTN_DT,
CDC_OPER_CD
FROM (
SELECT * FROM
(
SELECT PYMT_ATRB_RDN_ID,
PYMT_ATRB_ID,
PRPS_PYMT_ID,
ST_FSA_CD,
CNTY_FSA_CD,
ACCT_PGM_CD,
SBSD_PRD_STRT_YR,
MBR_CORE_CUST_ID,
MBR_CORE_CUST_ST_CNTY_FSA_CD,
PAID_CORE_CUST_ID,
PAID_CORE_CUST_ST_CNTY_FSA_CD,
PYMT_ATRB_RDN_TYPE_CD,
PYMT_ATRB_RDN_AMT,
TOT_PYMT_ATRB_RDN_AMT,
DATA_STAT_CD,
CRE_DT,
CRE_USER_NM,
LAST_CHG_DT,
LAST_CHG_USER_NM,
DATA_ACTN_CD,
DATA_ACTN_DT,
OP CDC_OPER_CD,
''  HASH_DIF,
current_timestamp LOAD_DT,
'CNSV'  DATA_SRC_NM,
'{ETL_START_DATE}' AS CDC_DT,
ROW_NUMBER() over ( partition by 
PYMT_ATRB_RDN_ID, 
PYMT_ATRB_ID, 
PRPS_PYMT_ID,
DATA_ACTN_DT
order by TBL_PRIORITY ASC, LAST_CHG_DT DESC) AS row_num_part
FROM
(
SELECT 
NET_CHANGES.PYMT_ATRB_RDN_ID,
NET_CHANGES.PYMT_ATRB_ID,
NET_CHANGES.PRPS_PYMT_ID,
NET_CHANGES.ST_FSA_CD,
NET_CHANGES.CNTY_FSA_CD,
NET_CHANGES.ACCT_PGM_CD,
NET_CHANGES.SBSD_PRD_STRT_YR,
NET_CHANGES.MBR_CORE_CUST_ID,
NET_CHANGES.MBR_CORE_CUST_ST_CNTY_FSA_CD,
NET_CHANGES.PAID_CORE_CUST_ID,
NET_CHANGES.PAID_CORE_CUST_ST_CNTY_FSA_CD,
NET_CHANGES.PYMT_ATRB_RDN_TYPE_CD,
NET_CHANGES.PYMT_ATRB_RDN_AMT,
NET_CHANGES.TOT_PYMT_ATRB_RDN_AMT,
NET_CHANGES.DATA_STAT_CD,
NET_CHANGES.CRE_DT,
NET_CHANGES.CRE_USER_NM,
NET_CHANGES.LAST_CHG_DT,
NET_CHANGES.LAST_CHG_USER_NM,
NET_CHANGES.DATA_ACTN_CD,
NET_CHANGES.DATA_ACTN_DT,
NET_CHANGES.OP,
NET_CHANGES.TBL_PRIORITY
FROM ( 
SELECT 
payment_attribution_reduction_audit.payment_attribution_reduction_identifier PYMT_ATRB_RDN_ID,
payment_attribution_reduction_audit.payment_attribution_identifier PYMT_ATRB_ID,
payment_attribution_reduction_audit.proposed_payment_identifier PRPS_PYMT_ID,
proposed_payment.state_fsa_code ST_FSA_CD,
proposed_payment.county_fsa_code CNTY_FSA_CD,
proposed_payment.accounting_program_code ACCT_PGM_CD,
proposed_payment.subsidiary_period_start_year SBSD_PRD_STRT_YR,
subsidiary_customer.core_customer_identifier MBR_CORE_CUST_ID,
fsa_county.state_county_fsa_code MBR_CORE_CUST_ST_CNTY_FSA_CD,
paid_subsidiary_customer.core_customer_identifier PAID_CORE_CUST_ID,
paid_fsa_county.state_county_fsa_code PAID_CORE_CUST_ST_CNTY_FSA_CD,
payment_attribution_reduction_audit.payment_attribution_reduction_type_code PYMT_ATRB_RDN_TYPE_CD,
payment_attribution_reduction_audit.payment_attribution_reduction_amount PYMT_ATRB_RDN_AMT,
payment_attribution_reduction_audit.total_payment_attribution_reduction_amount TOT_PYMT_ATRB_RDN_AMT,
payment_attribution_reduction_audit.data_status_code DATA_STAT_CD,
payment_attribution_reduction_audit.creation_date CRE_DT,
payment_attribution_reduction_audit.creation_user_name CRE_USER_NM,
payment_attribution_reduction_audit.last_change_date LAST_CHG_DT,
payment_attribution_reduction_audit.last_change_user_name LAST_CHG_USER_NM,
payment_attribution_reduction_audit.data_action_code DATA_ACTN_CD,
payment_attribution_reduction_audit.data_action_date DATA_ACTN_DT,
payment_attribution_reduction_audit.OP,
1 AS TBL_PRIORITY,
ROW_NUMBER() over ( partition by 
payment_attribution_reduction_audit.payment_attribution_reduction_identifier, 
payment_attribution_reduction_audit.payment_attribution_identifier, 
payment_attribution_reduction_audit.proposed_payment_identifier,
payment_attribution_reduction_audit.data_action_date
order by LAST_CHG_DT DESC  ) AS row_num_part
FROM `fsa-{env}-cnsv-cdc`.`payment_attribution_reduction_audit`
LEFT JOIN `fsa-{env}-cnsv`.`proposed_payment`
ON (payment_attribution_reduction_audit.proposed_payment_identifier = proposed_payment.proposed_payment_identifier)
LEFT JOIN 
    /* Join from Reduction Audit to (Attribution Audit and Attribution Live union table) (as some ids can be in live table) based on Attribution ID
       and select any one record (as the Member ID and Paid ID will be same on all the records) for a given Attribution ID irrespective of Proposed payment ID
    */   
(SELECT *
 FROM
     (SELECT *,row_number() over (partition by payment_attribution_identifier order by last_change_date desc) as rnk  
      FROM
          (SELECT payment_attribution_identifier,subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
           FROM `fsa-{env}-cnsv`.`payment_attribution`
           UNION
           SELECT payment_attribution_identifier,subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
           FROM `fsa-{env}-cnsv`.`payment_attribution_audit`
          ) attr_union
     )attr_all
      WHERE 1 = 1
      AND rnk = 1
) payment_attribution_union_unique
ON (payment_attribution_reduction_audit.payment_attribution_identifier = payment_attribution_union_unique.payment_attribution_identifier)
LEFT JOIN `fsa-{env}-cnsv`.`subsidiary_customer`
ON (payment_attribution_union_unique.subsidiary_customer_identifier = subsidiary_customer.subsidiary_customer_identifier)
/*get paid customer information*/
LEFT JOIN `fsa-{env}-cnsv`.`subsidiary_customer` paid_subsidiary_customer
ON (payment_attribution_union_unique.paid_subsidiary_customer_identifier = paid_subsidiary_customer.subsidiary_customer_identifier)
LEFT JOIN `fsa-{env}-cnsv`.`fsa_county`
ON (subsidiary_customer.fsa_county_identifier = fsa_county.fsa_county_identifier)
/*get paid customer fsa state county information*/
LEFT JOIN `fsa-{env}-cnsv`.`fsa_county` paid_fsa_county
ON (paid_subsidiary_customer.fsa_county_identifier = paid_fsa_county.fsa_county_identifier)
WHERE payment_attribution_union_unique.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND payment_attribution_union_unique.OP <> 'D'
) NET_CHANGES
WHERE NET_CHANGES.row_num_part = 1
UNION
SELECT 
payment_attribution_reduction_audit.payment_attribution_reduction_identifier PYMT_ATRB_RDN_ID,
payment_attribution_reduction_audit.payment_attribution_identifier PYMT_ATRB_ID,
payment_attribution_reduction_audit.proposed_payment_identifier PRPS_PYMT_ID,
proposed_payment.state_fsa_code ST_FSA_CD,
proposed_payment.county_fsa_code CNTY_FSA_CD,
proposed_payment.accounting_program_code ACCT_PGM_CD,
proposed_payment.subsidiary_period_start_year SBSD_PRD_STRT_YR,
subsidiary_customer.core_customer_identifier MBR_CORE_CUST_ID,
fsa_county.state_county_fsa_code MBR_CORE_CUST_ST_CNTY_FSA_CD,
paid_subsidiary_customer.core_customer_identifier PAID_CORE_CUST_ID,
paid_fsa_county.state_county_fsa_code PAID_CORE_CUST_ST_CNTY_FSA_CD,
payment_attribution_reduction_audit.payment_attribution_reduction_type_code PYMT_ATRB_RDN_TYPE_CD,
payment_attribution_reduction_audit.payment_attribution_reduction_amount PYMT_ATRB_RDN_AMT,
payment_attribution_reduction_audit.total_payment_attribution_reduction_amount TOT_PYMT_ATRB_RDN_AMT,
payment_attribution_reduction_audit.data_status_code DATA_STAT_CD,
payment_attribution_reduction_audit.creation_date CRE_DT,
payment_attribution_reduction_audit.creation_user_name CRE_USER_NM,
payment_attribution_reduction_audit.last_change_date LAST_CHG_DT,
payment_attribution_reduction_audit.last_change_user_name LAST_CHG_USER_NM,
payment_attribution_reduction_audit.data_action_code DATA_ACTN_CD,
payment_attribution_reduction_audit.data_action_date DATA_ACTN_DT,
proposed_payment.OP,
2 AS TBL_PRIORITY
FROM `fsa-{env}-cnsv`.`proposed_payment`
JOIN `fsa-{env}-cnsv`.`payment_attribution_reduction_audit`
ON (proposed_payment.proposed_payment_identifier = payment_attribution_reduction_audit.proposed_payment_identifier)
LEFT JOIN 
    /* Join from Reduction Audit to (Attribution Audit and Attribution Live union table) (as some ids can be in live table) based on Attribution ID
       and select any one record (as the Member ID and Paid ID will be same on all the records) for a given Attribution ID irrespective of Proposed payment ID
    */   
(SELECT *
 FROM
     (SELECT *,row_number() over (partition by payment_attribution_identifier order by last_change_date desc) as rnk  
      FROM
          (SELECT payment_attribution_identifier,subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
           FROM `fsa-{env}-cnsv`.`payment_attribution`
           UNION
           SELECT payment_attribution_identifier,subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
           FROM `fsa-{env}-cnsv`.`payment_attribution_audit`
          ) attr_union
     )attr_all
      WHERE 1 = 1
      AND rnk = 1
) payment_attribution_union_unique
ON (payment_attribution_reduction_audit.payment_attribution_identifier = payment_attribution_union_unique.payment_attribution_identifier)
LEFT JOIN `fsa-{env}-cnsv`.`subsidiary_customer`
ON (payment_attribution_union_unique.subsidiary_customer_identifier = subsidiary_customer.subsidiary_customer_identifier)
/*get paid customer information*/
LEFT JOIN `fsa-{env}-cnsv`.`subsidiary_customer` paid_subsidiary_customer
ON (payment_attribution_union_unique.paid_subsidiary_customer_identifier = paid_subsidiary_customer.subsidiary_customer_identifier)
LEFT JOIN `fsa-{env}-cnsv`.`fsa_county`
ON (subsidiary_customer.fsa_county_identifier = fsa_county.fsa_county_identifier)
/*get paid customer fsa state county information*/
LEFT JOIN `fsa-{env}-cnsv`.`fsa_county` paid_fsa_county
ON (paid_subsidiary_customer.fsa_county_identifier = paid_fsa_county.fsa_county_identifier)
WHERE payment_attribution_union_unique.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND payment_attribution_union_unique.OP <> 'D'
UNION
SELECT 
payment_attribution_reduction_audit.payment_attribution_reduction_identifier PYMT_ATRB_RDN_ID,
payment_attribution_reduction_audit.payment_attribution_identifier PYMT_ATRB_ID,
payment_attribution_reduction_audit.proposed_payment_identifier PRPS_PYMT_ID,
proposed_payment.state_fsa_code ST_FSA_CD,
proposed_payment.county_fsa_code CNTY_FSA_CD,
proposed_payment.accounting_program_code ACCT_PGM_CD,
proposed_payment.subsidiary_period_start_year SBSD_PRD_STRT_YR,
subsidiary_customer.core_customer_identifier MBR_CORE_CUST_ID,
fsa_county.state_county_fsa_code MBR_CORE_CUST_ST_CNTY_FSA_CD,
paid_subsidiary_customer.core_customer_identifier PAID_CORE_CUST_ID,
paid_fsa_county.state_county_fsa_code PAID_CORE_CUST_ST_CNTY_FSA_CD,
payment_attribution_reduction_audit.payment_attribution_reduction_type_code PYMT_ATRB_RDN_TYPE_CD,
payment_attribution_reduction_audit.payment_attribution_reduction_amount PYMT_ATRB_RDN_AMT,
payment_attribution_reduction_audit.total_payment_attribution_reduction_amount TOT_PYMT_ATRB_RDN_AMT,
payment_attribution_reduction_audit.data_status_code DATA_STAT_CD,
payment_attribution_reduction_audit.creation_date CRE_DT,
payment_attribution_reduction_audit.creation_user_name CRE_USER_NM,
payment_attribution_reduction_audit.last_change_date LAST_CHG_DT,
payment_attribution_reduction_audit.last_change_user_name LAST_CHG_USER_NM,
payment_attribution_reduction_audit.data_action_code DATA_ACTN_CD,
payment_attribution_reduction_audit.data_action_date DATA_ACTN_DT,
proposed_payment.OP,
3 AS TBL_PRIORITY
FROM 
  ( 
  Select SBSD_PRE.* from (
Select * FROM `fsa-{env}-cnsv-cdc`.`subsidiary_customer`     where  subsidiary_customer.OP ='UN' And cast('{ETL_START_DATE}' as date)  <> cast(current_date as date)
     UNION
     /*same day scenario update happens*/
    Select * FROM `fsa-{env}-cnsv-cdc`.`subsidiary_customer`  where  subsidiary_customer.OP ='UN'
       And cast('{ETL_END_DATE}' as date)  = cast(current_date as date)
     )  SBSD_PRE
  JOIN
   ( Select * from `fsa-{env}-cnsv`.`subsidiary_customer`   
   ) SBSD_CUST_MAIN
ON  ( SBSD_PRE.subsidiary_customer_identifier = SBSD_CUST_MAIN.subsidiary_customer_identifier 
       And ( SBSD_PRE.fsa_county_identifier <> SBSD_CUST_MAIN.fsa_county_identifier Or SBSD_PRE.core_customer_identifier <> SBSD_CUST_MAIN.core_customer_identifier )
     )
     UNION
   Select * FROM `fsa-{env}-cnsv-cdc`.`subsidiary_customer`  where   subsidiary_customer.OP ='I'
 ) subsidiary_customer
/*We need to use inner joins until we reach the point of Driving table*/
LEFT JOIN 
    /* Join from Reduction Audit to (Attribution Audit and Attribution Live union table) (as some ids can be in live table) based on Attribution ID
       and select any one record (as the Member ID and Paid ID will be same on all the records) for a given Attribution ID irrespective of Proposed payment ID
    */   
(SELECT *
 FROM
     (SELECT *,row_number() over (partition by payment_attribution_identifier order by last_change_date desc) as rnk  
      FROM
          (SELECT payment_attribution_identifier,subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
           FROM `fsa-{env}-cnsv`.`payment_attribution`
           UNION
           SELECT payment_attribution_identifier,subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
           FROM `fsa-{env}-cnsv`.`payment_attribution_audit`
          ) attr_union
     )attr_all
      WHERE 1 = 1
      AND rnk = 1
) payment_attribution_union_unique
ON (subsidiary_customer.subsidiary_customer_identifier = payment_attribution_union_unique.subsidiary_customer_identifier)
JOIN `fsa-{env}-cnsv`.`payment_attribution_reduction_audit`
ON (payment_attribution_union_unique.payment_attribution_identifier = payment_attribution_reduction_audit.payment_attribution_identifier)
LEFT JOIN `fsa-{env}-cnsv`.`proposed_payment`
ON (payment_attribution_reduction_audit.proposed_payment_identifier = proposed_payment.proposed_payment_identifier)
/*get paid customer information*/
LEFT JOIN `fsa-{env}-cnsv`.`subsidiary_customer` paid_subsidiary_customer
ON (payment_attribution_union_unique.paid_subsidiary_customer_identifier = paid_subsidiary_customer.subsidiary_customer_identifier)
LEFT JOIN `fsa-{env}-cnsv`.`fsa_county`
ON (subsidiary_customer.fsa_county_identifier = fsa_county.fsa_county_identifier)
/*get paid customer fsa state county information*/
LEFT JOIN `fsa-{env}-cnsv`.`fsa_county` paid_fsa_county
ON (paid_subsidiary_customer.fsa_county_identifier = paid_fsa_county.fsa_county_identifier)
WHERE subsidiary_customer.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND subsidiary_customer.OP <> 'D'
UNION
SELECT 
payment_attribution_reduction_audit.payment_attribution_reduction_identifier PYMT_ATRB_RDN_ID,
payment_attribution_reduction_audit.payment_attribution_identifier PYMT_ATRB_ID,
payment_attribution_reduction_audit.proposed_payment_identifier PRPS_PYMT_ID,
proposed_payment.state_fsa_code ST_FSA_CD,
proposed_payment.county_fsa_code CNTY_FSA_CD,
proposed_payment.accounting_program_code ACCT_PGM_CD,
proposed_payment.subsidiary_period_start_year SBSD_PRD_STRT_YR,
subsidiary_customer.core_customer_identifier MBR_CORE_CUST_ID,
fsa_county.state_county_fsa_code MBR_CORE_CUST_ST_CNTY_FSA_CD,
paid_subsidiary_customer.core_customer_identifier PAID_CORE_CUST_ID,
paid_fsa_county.state_county_fsa_code PAID_CORE_CUST_ST_CNTY_FSA_CD,
payment_attribution_reduction_audit.payment_attribution_reduction_type_code PYMT_ATRB_RDN_TYPE_CD,
payment_attribution_reduction_audit.payment_attribution_reduction_amount PYMT_ATRB_RDN_AMT,
payment_attribution_reduction_audit.total_payment_attribution_reduction_amount TOT_PYMT_ATRB_RDN_AMT,
payment_attribution_reduction_audit.data_status_code DATA_STAT_CD,
payment_attribution_reduction_audit.creation_date CRE_DT,
payment_attribution_reduction_audit.creation_user_name CRE_USER_NM,
payment_attribution_reduction_audit.last_change_date LAST_CHG_DT,
payment_attribution_reduction_audit.last_change_user_name LAST_CHG_USER_NM,
payment_attribution_reduction_audit.data_action_code DATA_ACTN_CD,
payment_attribution_reduction_audit.data_action_date DATA_ACTN_DT,
fsa_county.OP,
4 AS TBL_PRIORITY
FROM (  SELECT 
    fsa_county_identifier,
    state_county_fsa_code,
    last_change_date, 
    last_change_user_name,
    fsa_county.OP 
    FROM (
    SELECT *,ROW_NUMBER() OVER (PARTITION BY fsa_county_identifier
            ORDER BY LAST_CHG_DT desc) as rnum 
            FROM `fsa-{env}-cnsv-cdc`.`fsa_county`) as SubQry
    WHERE SubQry.rnum=1
) fsa_county
JOIN `fsa-{env}-cnsv`.`subsidiary_customer`
ON (fsa_county.fsa_county_identifier = subsidiary_customer.fsa_county_identifier)
/*We need to use inner joins until we reach the point of Driving table*/
LEFT JOIN 
    /* Join from Reduction Audit to (Attribution Audit and Attribution Live union table) (as some ids can be in live table) based on Attribution ID
       and select any one record (as the Member ID and Paid ID will be same on all the records) for a given Attribution ID irrespective of Proposed payment ID
    */   
(SELECT *
 FROM
     (SELECT *,row_number() over (partition by payment_attribution_identifier order by last_change_date desc) as rnk  
      FROM
          (SELECT payment_attribution_identifier,subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
           FROM `fsa-{env}-cnsv`.`payment_attribution`
           UNION
           SELECT payment_attribution_identifier,subsidiary_customer_identifier,paid_subsidiary_customer_identifier,last_change_date
           FROM `fsa-{env}-cnsv`.`payment_attribution_audit`
          ) attr_union
     )attr_all
      WHERE 1 = 1
      AND rnk = 1
) payment_attribution_union_unique
ON (subsidiary_customer.subsidiary_customer_identifier = payment_attribution_union_unique.subsidiary_customer_identifier)
JOIN `fsa-{env}-cnsv`.`payment_attribution_reduction_audit`
ON (payment_attribution_union_unique.payment_attribution_identifier = payment_attribution_reduction_audit.payment_attribution_identifier)
LEFT JOIN `fsa-{env}-cnsv`.`proposed_payment`
ON (payment_attribution_reduction_audit.proposed_payment_identifier = proposed_payment.proposed_payment_identifier)
/*get paid customer information*/
LEFT JOIN `fsa-{env}-cnsv`.`subsidiary_customer` paid_subsidiary_customer
ON (payment_attribution_union_unique.paid_subsidiary_customer_identifier = paid_subsidiary_customer.subsidiary_customer_identifier)
/*get paid customer fsa state county information*/
LEFT JOIN `fsa-{env}-cnsv`.`fsa_county` paid_fsa_county
ON (paid_subsidiary_customer.fsa_county_identifier = paid_fsa_county.fsa_county_identifier)
WHERE attr_all.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND attr_all.OP <> 'D'
UNION
SELECT 
DELETE_NET.PYMT_ATRB_RDN_ID,
DELETE_NET.PYMT_ATRB_ID,
DELETE_NET.PRPS_PYMT_ID,
DELETE_NET.ST_FSA_CD,
DELETE_NET.CNTY_FSA_CD,
DELETE_NET.ACCT_PGM_CD,
DELETE_NET.SBSD_PRD_STRT_YR,
DELETE_NET.MBR_CORE_CUST_ID,
DELETE_NET.MBR_CORE_CUST_ST_CNTY_FSA_CD,
DELETE_NET.PAID_CORE_CUST_ID,
DELETE_NET.PAID_CORE_CUST_ST_CNTY_FSA_CD,
DELETE_NET.PYMT_ATRB_RDN_TYPE_CD,
DELETE_NET.PYMT_ATRB_RDN_AMT,
DELETE_NET.TOT_PYMT_ATRB_RDN_AMT,
DELETE_NET.DATA_STAT_CD,
DELETE_NET.CRE_DT,
DELETE_NET.CRE_USER_NM,
DELETE_NET.LAST_CHG_DT,
DELETE_NET.LAST_CHG_USER_NM,
DELETE_NET.DATA_ACTN_CD,
DELETE_NET.DATA_ACTN_DT,
DELETE_NET.CDC_OPER_CD,
DELETE_NET.TBL_PRIORITY
FROM (
SELECT
payment_attribution_reduction_audit.payment_attribution_reduction_identifier PYMT_ATRB_RDN_ID,
payment_attribution_reduction_audit.payment_attribution_identifier PYMT_ATRB_ID,
payment_attribution_reduction_audit.proposed_payment_identifier PRPS_PYMT_ID,
NULL ST_FSA_CD,
NULL CNTY_FSA_CD,
NULL ACCT_PGM_CD,
NULL SBSD_PRD_STRT_YR,
NULL MBR_CORE_CUST_ID,
NULL MBR_CORE_CUST_ST_CNTY_FSA_CD,
NULL PAID_CORE_CUST_ID,
NULL PAID_CORE_CUST_ST_CNTY_FSA_CD,
payment_attribution_reduction_audit.payment_attribution_reduction_type_code PYMT_ATRB_RDN_TYPE_CD,
payment_attribution_reduction_audit.payment_attribution_reduction_amount PYMT_ATRB_RDN_AMT,
payment_attribution_reduction_audit.total_payment_attribution_reduction_amount TOT_PYMT_ATRB_RDN_AMT,
payment_attribution_reduction_audit.data_status_code DATA_STAT_CD,
payment_attribution_reduction_audit.creation_date CRE_DT,
payment_attribution_reduction_audit.creation_user_name CRE_USER_NM,
payment_attribution_reduction_audit.last_change_date LAST_CHG_DT,
payment_attribution_reduction_audit.last_change_user_name LAST_CHG_USER_NM,
payment_attribution_reduction_audit.data_action_code DATA_ACTN_CD,
payment_attribution_reduction_audit.data_action_date DATA_ACTN_DT,
'D' OP,
1 AS TBL_PRIORITY,
ROW_NUMBER() over ( partition by 
payment_attribution_reduction_audit.payment_attribution_reduction_identifier, 
payment_attribution_reduction_audit.payment_attribution_identifier, 
payment_attribution_reduction_audit.proposed_payment_identifier,
payment_attribution_reduction_audit.data_action_date
order by LAST_CHG_DT DESC, payment_attribution_reduction_audit.__CDC_SEQVAL DESC ) AS row_num_part
FROM `fsa-{env}-cnsv-cdc`.`payment_attribution_reduction_audit`
WHERE payment_attribution_reduction_audit.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND payment_attribution_reduction_audit.OP = 'D'
)DELETE_NET
WHERE DELETE_NET.row_num_part = 1
) STG_ALL
) STG_UNQ
WHERE row_num_part = 1
AND (
 (COALESCE(CAST(CRE_DT AS DATE), DATE '1900-01-01') <= CAST('{ETL_START_DATE}'  AS DATE))
    AND
    (COALESCE(CAST(LAST_CHG_DT AS DATE), DATE '1900-01-01') <= CAST('{ETL_START_DATE}'  AS DATE))
)
)