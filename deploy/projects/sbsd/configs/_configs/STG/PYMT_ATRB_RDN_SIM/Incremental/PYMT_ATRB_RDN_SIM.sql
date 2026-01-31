SELECT PYMT_ATRB_RDN_SIM_ID,
PYMT_ATRB_SIM_ID,
PRPS_PYMT_SIM_ID,
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
CDC_OPER_CD
FROM (
SELECT * FROM
(
SELECT PYMT_ATRB_RDN_SIM_ID,
PYMT_ATRB_SIM_ID,
PRPS_PYMT_SIM_ID,
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
OP CDC_OPER_CD,
ROW_NUMBER() over ( partition by 
PYMT_ATRB_RDN_SIM_ID,PYMT_ATRB_SIM_ID,PRPS_PYMT_SIM_ID
order by TBL_PRIORITY ASC, LAST_CHG_DT DESC ) AS row_num_part
FROM
(
SELECT 
payment_attribution_reduction_simulation.payment_attribution_reduction_simulation_identifier PYMT_ATRB_RDN_SIM_ID,
payment_attribution_reduction_simulation.payment_attribution_simulation_identifier PYMT_ATRB_SIM_ID,
payment_attribution_reduction_simulation.proposed_payment_simulation_identifier PRPS_PYMT_SIM_ID,
proposed_payment_simulation.state_fsa_code ST_FSA_CD,
proposed_payment_simulation.county_fsa_code CNTY_FSA_CD,
proposed_payment_simulation.accounting_program_code ACCT_PGM_CD,
proposed_payment_simulation.subsidiary_period_start_year SBSD_PRD_STRT_YR,
subsidiary_customer.core_customer_identifier MBR_CORE_CUST_ID,
paid_subsidiary_customer.core_customer_identifier PAID_CORE_CUST_ID,
fsa_county.state_county_fsa_code MBR_CORE_CUST_ST_CNTY_FSA_CD,
paid_fsa_county.state_county_fsa_code PAID_CORE_CUST_ST_CNTY_FSA_CD,
payment_attribution_reduction_simulation.payment_attribution_reduction_type_code PYMT_ATRB_RDN_TYPE_CD,
payment_attribution_reduction_simulation.payment_attribution_reduction_amount PYMT_ATRB_RDN_AMT,
payment_attribution_reduction_simulation.total_payment_attribution_reduction_amount TOT_PYMT_ATRB_RDN_AMT,
payment_attribution_reduction_simulation.data_status_code DATA_STAT_CD,
payment_attribution_reduction_simulation.creation_date CRE_DT,
payment_attribution_reduction_simulation.creation_user_name CRE_USER_NM,
payment_attribution_reduction_simulation.last_change_date LAST_CHG_DT,
payment_attribution_reduction_simulation.last_change_user_name LAST_CHG_USER_NM,
payment_attribution_reduction_simulation.OP,
1 AS TBL_PRIORITY
/*We need to use fn_all changes to capture all different proposed payment IDs for a given attribution ID for a given CDC date*/
FROM `fsa-{env}-sbsd-cdc`.`payment_attribution_reduction_simulation`
LEFT JOIN `fsa-{env}-sbsd-cdc`.`proposed_payment_simulation`
    ON (payment_attribution_reduction_simulation.proposed_payment_simulation_identifier = proposed_payment_simulation.proposed_payment_simulation_identifier)
    LEFT JOIN `fsa-{env}-sbsd-cdc`.`payment_attribution_simulation`
    ON (payment_attribution_reduction_simulation.payment_attribution_simulation_identifier = payment_attribution_simulation.payment_attribution_simulation_identifier)
    LEFT JOIN `fsa-{env}-sbsd-cdc`.`subsidiary_customer`
    ON (payment_attribution_simulation.subsidiary_customer_identifier = subsidiary_customer.subsidiary_customer_identifier)
    /*get paid customer information*/
    LEFT JOIN `fsa-{env}-sbsd-cdc`.`subsidiary_customer` paid_subsidiary_customer
    ON (payment_attribution_simulation.paid_subsidiary_customer_identifier = paid_subsidiary_customer.subsidiary_customer_identifier)
    LEFT JOIN `fsa-{env}-sbsd`.`fsa_county`
    ON (subsidiary_customer.fsa_county_identifier = fsa_county.fsa_county_identifier)
    /*get paid customer fsa state county information*/
    LEFT JOIN `fsa-{env}-sbsd`.`fsa_county` paid_fsa_county
    ON (paid_subsidiary_customer.fsa_county_identifier = paid_fsa_county.fsa_county_identifier)
WHERE payment_attribution_reduction_simulation.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND payment_attribution_reduction_simulation.OP <> 'D'

UNION

SELECT 
payment_attribution_reduction_simulation.payment_attribution_reduction_simulation_identifier PYMT_ATRB_RDN_SIM_ID,
payment_attribution_reduction_simulation.payment_attribution_simulation_identifier PYMT_ATRB_SIM_ID,
payment_attribution_reduction_simulation.proposed_payment_simulation_identifier PRPS_PYMT_SIM_ID,
proposed_payment_simulation.state_fsa_code ST_FSA_CD,
proposed_payment_simulation.county_fsa_code CNTY_FSA_CD,
proposed_payment_simulation.accounting_program_code ACCT_PGM_CD,
proposed_payment_simulation.subsidiary_period_start_year SBSD_PRD_STRT_YR,
subsidiary_customer.core_customer_identifier MBR_CORE_CUST_ID,
paid_subsidiary_customer.core_customer_identifier PAID_CORE_CUST_ID,
fsa_county.state_county_fsa_code MBR_CORE_CUST_ST_CNTY_FSA_CD,
paid_fsa_county.state_county_fsa_code PAID_CORE_CUST_ST_CNTY_FSA_CD,
payment_attribution_reduction_simulation.payment_attribution_reduction_type_code PYMT_ATRB_RDN_TYPE_CD,
payment_attribution_reduction_simulation.payment_attribution_reduction_amount PYMT_ATRB_RDN_AMT,
payment_attribution_reduction_simulation.total_payment_attribution_reduction_amount TOT_PYMT_ATRB_RDN_AMT,
payment_attribution_reduction_simulation.data_status_code DATA_STAT_CD,
payment_attribution_reduction_simulation.creation_date CRE_DT,
payment_attribution_reduction_simulation.creation_user_name CRE_USER_NM,
payment_attribution_reduction_simulation.last_change_date LAST_CHG_DT,
payment_attribution_reduction_simulation.last_change_user_name LAST_CHG_USER_NM,
proposed_payment_simulation.OP,
2 AS TBL_PRIORITY
FROM `fsa-{env}-sbsd-cdc`.`proposed_payment_simulation`
JOIN `fsa-{env}-sbsd-cdc`.`payment_attribution_reduction_simulation`
ON (proposed_payment_simulation.proposed_payment_simulation_identifier = payment_attribution_reduction_simulation.proposed_payment_simulation_identifier)
LEFT JOIN `fsa-{env}-sbsd-cdc`.`payment_attribution_simulation`
ON (payment_attribution_reduction_simulation.payment_attribution_simulation_identifier = payment_attribution_simulation.payment_attribution_simulation_identifier)
LEFT JOIN `fsa-{env}-sbsd-cdc`.`subsidiary_customer`
ON (payment_attribution_simulation.subsidiary_customer_identifier = subsidiary_customer.subsidiary_customer_identifier)
/*get paid customer information*/
LEFT JOIN `fsa-{env}-sbsd-cdc`.`subsidiary_customer` paid_subsidiary_customer
ON (payment_attribution_simulation.paid_subsidiary_customer_identifier = paid_subsidiary_customer.subsidiary_customer_identifier)
LEFT JOIN `fsa-{env}-sbsd`.`fsa_county`
ON (subsidiary_customer.fsa_county_identifier = fsa_county.fsa_county_identifier)
/*get paid customer fsa state county information*/
LEFT JOIN `fsa-{env}-sbsd`.`fsa_county` paid_fsa_county
ON (paid_subsidiary_customer.fsa_county_identifier = paid_fsa_county.fsa_county_identifier)
WHERE proposed_payment_simulation.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND proposed_payment_simulation.OP <> 'D'

UNION

SELECT 
payment_attribution_reduction_simulation.payment_attribution_reduction_simulation_identifier PYMT_ATRB_RDN_SIM_ID,
payment_attribution_reduction_simulation.payment_attribution_simulation_identifier PYMT_ATRB_SIM_ID,
payment_attribution_reduction_simulation.proposed_payment_simulation_identifier PRPS_PYMT_SIM_ID,
proposed_payment_simulation.state_fsa_code ST_FSA_CD,
proposed_payment_simulation.county_fsa_code CNTY_FSA_CD,
proposed_payment_simulation.accounting_program_code ACCT_PGM_CD,
proposed_payment_simulation.subsidiary_period_start_year SBSD_PRD_STRT_YR,
subsidiary_customer.core_customer_identifier MBR_CORE_CUST_ID,
paid_subsidiary_customer.core_customer_identifier PAID_CORE_CUST_ID,
fsa_county.state_county_fsa_code MBR_CORE_CUST_ST_CNTY_FSA_CD,
paid_fsa_county.state_county_fsa_code PAID_CORE_CUST_ST_CNTY_FSA_CD,
payment_attribution_reduction_simulation.payment_attribution_reduction_type_code PYMT_ATRB_RDN_TYPE_CD,
payment_attribution_reduction_simulation.payment_attribution_reduction_amount PYMT_ATRB_RDN_AMT,
payment_attribution_reduction_simulation.total_payment_attribution_reduction_amount TOT_PYMT_ATRB_RDN_AMT,
payment_attribution_reduction_simulation.data_status_code DATA_STAT_CD,
payment_attribution_reduction_simulation.creation_date CRE_DT,
payment_attribution_reduction_simulation.creation_user_name CRE_USER_NM,
payment_attribution_reduction_simulation.last_change_date LAST_CHG_DT,
payment_attribution_reduction_simulation.last_change_user_name LAST_CHG_USER_NM,
subsidiary_customer.OP,
3 AS TBL_PRIORITY
FROM 
  ( 
Select SBSD_PRE.* from (
Select * FROM `fsa-{env}-sbsd-cdc`.`subsidiary_customer`  where  subsidiary_customer.OP ='UN' And cast('{ETL_END_DATE}' as date)  <> cast(current_date as date)
     UNION
     /*same day scenario update happens*/
    Select * FROM `fsa-{env}-sbsd-cdc`.`subsidiary_customer`)  where  subsidiary_customer.OP ='UN'
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
 ) subsidiary_customer
/*We need to use inner joins until we reach the point of Driving table*/
JOIN `fsa-{env}-sbsd`.`payment_attribution_simulation`
ON (subsidiary_customer.subsidiary_customer_identifier = payment_attribution_simulation.subsidiary_customer_identifier
    )
JOIN `fsa-{env}-sbsd`.`payment_attribution_reduction_simulation`
ON (payment_attribution_simulation.payment_attribution_simulation_identifier = payment_attribution_reduction_simulation.payment_attribution_simulation_identifier)
LEFT JOIN `fsa-{env}-sbsd`.`proposed_payment_simulation`
ON (payment_attribution_reduction_simulation.proposed_payment_simulation_identifier = proposed_payment_simulation.proposed_payment_simulation_identifier)
/*get paid customer information*/
LEFT JOIN `fsa-{env}-sbsd-cdc`.`subsidiary_customer` paid_subsidiary_customer
ON (payment_attribution_simulation.paid_subsidiary_customer_identifier = paid_subsidiary_customer.subsidiary_customer_identifier)
LEFT JOIN `fsa-{env}-sbsd`.`fsa_county`
ON (subsidiary_customer.fsa_county_identifier = fsa_county.fsa_county_identifier)
/*get paid customer fsa state county information*/
LEFT JOIN `fsa-{env}-sbsd`.`fsa_county` paid_fsa_county
ON (paid_subsidiary_customer.fsa_county_identifier = paid_fsa_county.fsa_county_identifier)
WHERE SBSD_PRE.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND SBSD_PRE.OP <> 'D'


UNION

SELECT 
payment_attribution_reduction_simulation.payment_attribution_reduction_simulation_identifier PYMT_ATRB_RDN_SIM_ID,
payment_attribution_reduction_simulation.payment_attribution_simulation_identifier PYMT_ATRB_SIM_ID,
payment_attribution_reduction_simulation.proposed_payment_simulation_identifier PRPS_PYMT_SIM_ID,
proposed_payment_simulation.state_fsa_code ST_FSA_CD,
proposed_payment_simulation.county_fsa_code CNTY_FSA_CD,
proposed_payment_simulation.accounting_program_code ACCT_PGM_CD,
proposed_payment_simulation.subsidiary_period_start_year SBSD_PRD_STRT_YR,
subsidiary_customer.core_customer_identifier MBR_CORE_CUST_ID,
paid_subsidiary_customer.core_customer_identifier PAID_CORE_CUST_ID,
fsa_county.state_county_fsa_code MBR_CORE_CUST_ST_CNTY_FSA_CD,
paid_fsa_county.state_county_fsa_code PAID_CORE_CUST_ST_CNTY_FSA_CD,
payment_attribution_reduction_simulation.payment_attribution_reduction_type_code PYMT_ATRB_RDN_TYPE_CD,
payment_attribution_reduction_simulation.payment_attribution_reduction_amount PYMT_ATRB_RDN_AMT,
payment_attribution_reduction_simulation.total_payment_attribution_reduction_amount TOT_PYMT_ATRB_RDN_AMT,
payment_attribution_reduction_simulation.data_status_code DATA_STAT_CD,
payment_attribution_reduction_simulation.creation_date CRE_DT,
payment_attribution_reduction_simulation.creation_user_name CRE_USER_NM,
payment_attribution_reduction_simulation.last_change_date LAST_CHG_DT,
payment_attribution_reduction_simulation.last_change_user_name LAST_CHG_USER_NM,
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
            FROM `fsa-{env}-sbsd-cdc`.`fsa_county` as SubQry
    WHERE SubQry.rnum=1
) fsa_county
JOIN `fsa-{env}-sbsd-cdc`.`subsidiary_customer`
ON (fsa_county.fsa_county_identifier = subsidiary_customer.fsa_county_identifier)
/*We need to use inner joins until we reach the point of Driving table*/
JOIN `fsa-{env}-sbsd-cdc`.`payment_attribution_simulation`
ON (subsidiary_customer.subsidiary_customer_identifier = payment_attribution_simulation.subsidiary_customer_identifier
    )
JOIN `fsa-{env}-sbsd-cdc`.`payment_attribution_reduction_simulation`
ON (payment_attribution_simulation.payment_attribution_simulation_identifier = payment_attribution_reduction_simulation.payment_attribution_simulation_identifier)
LEFT JOIN `fsa-{env}-sbsd-cdc`.`proposed_payment_simulation`
ON (payment_attribution_reduction_simulation.proposed_payment_simulation_identifier = proposed_payment_simulation.proposed_payment_simulation_identifier)
/*get paid customer information*/
LEFT JOIN `fsa-{env}-sbsd-cdc`.`subsidiary_customer` paid_subsidiary_customer
ON (payment_attribution_simulation.paid_subsidiary_customer_identifier = paid_subsidiary_customer.subsidiary_customer_identifier)
/*get paid customer fsa state county information*/
LEFT JOIN `fsa-{env}-sbsd`.`fsa_county` paid_fsa_county
ON (paid_subsidiary_customer.fsa_county_identifier = paid_fsa_county.fsa_county_identifier)
WHERE fsa_county.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND fsa_county.OP <> 'D'

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
payment_attribution_reduction_simulation.payment_attribution_reduction_simulation_identifier PYMT_ATRB_RDN_SIM_ID,
payment_attribution_reduction_simulation.payment_attribution_simulation_identifier PYMT_ATRB_SIM_ID,
payment_attribution_reduction_simulation.proposed_payment_simulation_identifier PRPS_PYMT_SIM_ID,
NULL ST_FSA_CD,
NULL CNTY_FSA_CD,
NULL ACCT_PGM_CD,
NULL SBSD_PRD_STRT_YR,
NULL MBR_CORE_CUST_ID,
NULL PAID_CORE_CUST_ID,
NULL MBR_CORE_CUST_ST_CNTY_FSA_CD,
NULL PAID_CORE_CUST_ST_CNTY_FSA_CD,
payment_attribution_reduction_simulation.payment_attribution_reduction_type_code PYMT_ATRB_RDN_TYPE_CD,
payment_attribution_reduction_simulation.payment_attribution_reduction_amount PYMT_ATRB_RDN_AMT,
payment_attribution_reduction_simulation.total_payment_attribution_reduction_amount TOT_PYMT_ATRB_RDN_AMT,
payment_attribution_reduction_simulation.data_status_code DATA_STAT_CD,
payment_attribution_reduction_simulation.creation_date CRE_DT,
payment_attribution_reduction_simulation.creation_user_name CRE_USER_NM,
payment_attribution_reduction_simulation.last_change_date LAST_CHG_DT,
payment_attribution_reduction_simulation.last_change_user_name LAST_CHG_USER_NM,
'D' OP,
1 AS row_num_part
FROM `fsa-{env}-sbsd-cdc`.`payment_attribution_reduction_simulation`
WHERE payment_attribution_reduction_simulation.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND payment_attribution_reduction_simulation.OP = 'D')