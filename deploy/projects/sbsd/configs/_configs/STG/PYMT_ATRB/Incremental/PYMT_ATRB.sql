SELECT 
PYMT_ATRB_ID,
PRPS_PYMT_ID,
ACCT_PGM_CD,
AGI_ELG_IND,
AGI_ERR_REF_CD,
BUS_TYPE_CD,
CASH_RENT_CPLD_FCTR,
CMB_PTY_PYMT_SHR_PCT,
MBR_CTRB_ELG_IND,
MBR_CTRB_SHR_PCT,
MBR_SHR_PCT,
NET_PYMT_AMT,
OWNSHP_HRCH_LVL_SHR_PCT,
PAID_PTY_SHR_PCT,
PAID_SBSD_CUST_ID,
PGM_INELG_RSN_TYPE_CD,
PMIT_ENTY_ELG_IND,
PRNT_PYMT_ATRB_ID,
SBSD_PRD_STRT_YR,
PYMT_ATRB_AMT,
PYMT_ATRB_BUS_CAT_CD,
PYMT_ATRB_SHR_PCT,
PYMT_LMT_YR,
PYMT_PGM_ID,
PYMT_PTY_RLT_CD,
RCD_REF_PRIM_ID,
RCD_REF_PRIM_ID_TYPE_CD,
RCD_REF_SCND_ID,
RCD_REF_SCND_ID_TYPE_CD,
SBSD_CUST_ID,
SBST_CHG_ELG_IND,
TAX_ID_TYPE_CD,
TOT_NET_PYMT_AMT,
TOT_PYMT_ATRB_AMT,
CRE_DT,
CRE_USER_NM,
DATA_STAT_CD,
FGN_PRSN_ELG_IND,
LAST_CHG_DT,
LAST_CHG_USER_NM,
MBR_CORE_CUST_ST_CNTY_FSA_CD,
PRNT_ATRB_CUST_ST_CNTY_FSA_CD,
PAID_CORE_CUST_ST_CNTY_FSA_CD,
MBR_CORE_CUST_ID,
PRNT_PYMT_ATRB_CORE_CUST_ID,
PAID_CORE_CUST_ID,
ST_FSA_CD,
CNTY_FSA_CD,
PRPS_PYMT_ACCT_PGM_CD,
PRPS_PYMT_SBSD_PRD_STRT_YR,
PRNT_PYMT_ATRB_ACCT_PGM_CD,
PRNT_PYMT_ATRB_CNTY_FSA_CD,
PRNT_PYMT_ATRB_ST_FSA_CD,
PRNT_SBSD_PRD_STRT_YR,
CDC_OPER_CD,
''  HASH_DIF,
current_timestamp LOAD_DT,
'CNSV'  DATA_SRC_NM,
'{ETL_START_DATE}' AS CDC_DT
FROM (
SELECT * FROM
(
SELECT DISTINCT PYMT_ATRB_ID,
PRPS_PYMT_ID,
ACCT_PGM_CD,
AGI_ELG_IND,
AGI_ERR_REF_CD,
BUS_TYPE_CD,
CASH_RENT_CPLD_FCTR,
CMB_PTY_PYMT_SHR_PCT,
MBR_CTRB_ELG_IND,
MBR_CTRB_SHR_PCT,
MBR_SHR_PCT,
NET_PYMT_AMT,
OWNSHP_HRCH_LVL_SHR_PCT,
PAID_PTY_SHR_PCT,
PAID_SBSD_CUST_ID,
PGM_INELG_RSN_TYPE_CD,
PMIT_ENTY_ELG_IND,
PRNT_PYMT_ATRB_ID,
SBSD_PRD_STRT_YR,
PYMT_ATRB_AMT,
PYMT_ATRB_BUS_CAT_CD,
PYMT_ATRB_SHR_PCT,
PYMT_LMT_YR,
PYMT_PGM_ID,
PYMT_PTY_RLT_CD,
RCD_REF_PRIM_ID,
RCD_REF_PRIM_ID_TYPE_CD,
RCD_REF_SCND_ID,
RCD_REF_SCND_ID_TYPE_CD,
SBSD_CUST_ID,
SBST_CHG_ELG_IND,
TAX_ID_TYPE_CD,
TOT_NET_PYMT_AMT,
TOT_PYMT_ATRB_AMT,
CRE_DT,
CRE_USER_NM,
DATA_STAT_CD,
FGN_PRSN_ELG_IND,
LAST_CHG_DT,
LAST_CHG_USER_NM,
MBR_CORE_CUST_ST_CNTY_FSA_CD,
PRNT_ATRB_CUST_ST_CNTY_FSA_CD,
PAID_CORE_CUST_ST_CNTY_FSA_CD,
MBR_CORE_CUST_ID,
PRNT_PYMT_ATRB_CORE_CUST_ID,
PAID_CORE_CUST_ID,
ST_FSA_CD,
CNTY_FSA_CD,
PRPS_PYMT_ACCT_PGM_CD,
PRPS_PYMT_SBSD_PRD_STRT_YR,
PRNT_PYMT_ATRB_ACCT_PGM_CD,
PRNT_PYMT_ATRB_CNTY_FSA_CD,
PRNT_PYMT_ATRB_ST_FSA_CD,
PRNT_SBSD_PRD_STRT_YR,
OP CDC_OPER_CD,
ROW_NUMBER() over ( partition by 
PYMT_ATRB_ID
order by TBL_PRIORITY ASC, LAST_CHG_DT DESC ) AS row_num_part
FROM
(
SELECT 
PAYMENT_ATTRIBUTION.payment_attribution_identifier PYMT_ATRB_ID,
PAYMENT_ATTRIBUTION.proposed_payment_identifier PRPS_PYMT_ID,
PAYMENT_ATTRIBUTION.accounting_program_code ACCT_PGM_CD,
PAYMENT_ATTRIBUTION.agi_eligible_indicator AGI_ELG_IND,
PAYMENT_ATTRIBUTION.agi_error_reference_code AGI_ERR_REF_CD,
PAYMENT_ATTRIBUTION.business_type_code BUS_TYPE_CD,
PAYMENT_ATTRIBUTION.cash_rent_cropland_factor CASH_RENT_CPLD_FCTR,
PAYMENT_ATTRIBUTION.combined_party_payment_share_percentage CMB_PTY_PYMT_SHR_PCT,
PAYMENT_ATTRIBUTION.member_contribution_eligible_indicator MBR_CTRB_ELG_IND,
PAYMENT_ATTRIBUTION.member_contribution_share_percentage MBR_CTRB_SHR_PCT,
PAYMENT_ATTRIBUTION.member_share_percentage MBR_SHR_PCT,
PAYMENT_ATTRIBUTION.net_payment_amount NET_PYMT_AMT,
PAYMENT_ATTRIBUTION.ownership_hierarchy_level_share_percentage OWNSHP_HRCH_LVL_SHR_PCT,
PAYMENT_ATTRIBUTION.paid_party_share_percentage PAID_PTY_SHR_PCT,
PAYMENT_ATTRIBUTION.paid_subsidiary_customer_identifier PAID_SBSD_CUST_ID,
PAYMENT_ATTRIBUTION.program_ineligibility_reason_type_code PGM_INELG_RSN_TYPE_CD,
PAYMENT_ATTRIBUTION.permitted_entity_eligible_indicator PMIT_ENTY_ELG_IND,
PAYMENT_ATTRIBUTION.parent_payment_attribution_identifier PRNT_PYMT_ATRB_ID,
PAYMENT_ATTRIBUTION.subsidiary_period_start_year SBSD_PRD_STRT_YR,
PAYMENT_ATTRIBUTION.payment_attribution_amount PYMT_ATRB_AMT,
PAYMENT_ATTRIBUTION.payment_attribution_business_category_code PYMT_ATRB_BUS_CAT_CD,
PAYMENT_ATTRIBUTION.payment_attribution_share_percentage PYMT_ATRB_SHR_PCT,
PAYMENT_ATTRIBUTION.payment_limitation_year PYMT_LMT_YR,
PAYMENT_ATTRIBUTION.limited_payment_program_identifier PYMT_PGM_ID,
PAYMENT_ATTRIBUTION.payment_party_relationship_code PYMT_PTY_RLT_CD,
PAYMENT_ATTRIBUTION.record_reference_primary_identification RCD_REF_PRIM_ID,
PAYMENT_ATTRIBUTION.record_reference_primary_identification_type_code RCD_REF_PRIM_ID_TYPE_CD,
PAYMENT_ATTRIBUTION.record_reference_secondary_identification RCD_REF_SCND_ID,
PAYMENT_ATTRIBUTION.record_reference_secondary_identification_type_code RCD_REF_SCND_ID_TYPE_CD,
PAYMENT_ATTRIBUTION.subsidiary_customer_identifier SBSD_CUST_ID,
PAYMENT_ATTRIBUTION.substantive_change_eligible_indicator SBST_CHG_ELG_IND,
PAYMENT_ATTRIBUTION.tax_identification_type_code TAX_ID_TYPE_CD,
PAYMENT_ATTRIBUTION.total_net_payment_amount TOT_NET_PYMT_AMT,
PAYMENT_ATTRIBUTION.total_payment_attribution_amount TOT_PYMT_ATRB_AMT,
PAYMENT_ATTRIBUTION.creation_date CRE_DT,
PAYMENT_ATTRIBUTION.creation_user_name CRE_USER_NM,
PAYMENT_ATTRIBUTION.data_status_code DATA_STAT_CD,
PAYMENT_ATTRIBUTION.foreign_person_eligible_indicator FGN_PRSN_ELG_IND,
PAYMENT_ATTRIBUTION.last_change_date LAST_CHG_DT,
PAYMENT_ATTRIBUTION.last_change_user_name LAST_CHG_USER_NM,
fsa_county.state_county_fsa_code MBR_CORE_CUST_ST_CNTY_FSA_CD,
parent_fsa_county.state_county_fsa_code PRNT_ATRB_CUST_ST_CNTY_FSA_CD,
paid_fsa_county.state_county_fsa_code PAID_CORE_CUST_ST_CNTY_FSA_CD,
subsidiary_customer.core_customer_identifier MBR_CORE_CUST_ID,
parent_subsidiary_customer.core_customer_identifier PRNT_PYMT_ATRB_CORE_CUST_ID,
paid_subsidiary_customer.core_customer_identifier PAID_CORE_CUST_ID,
proposed_payment.state_fsa_code ST_FSA_CD,
proposed_payment.county_fsa_code CNTY_FSA_CD,
proposed_payment.accounting_program_code PRPS_PYMT_ACCT_PGM_CD,
proposed_payment.subsidiary_period_start_year PRPS_PYMT_SBSD_PRD_STRT_YR,
PRNT_proposed_payment.accounting_program_code PRNT_PYMT_ATRB_ACCT_PGM_CD,
PRNT_proposed_payment.county_fsa_code PRNT_PYMT_ATRB_CNTY_FSA_CD,
PRNT_proposed_payment.state_fsa_code PRNT_PYMT_ATRB_ST_FSA_CD,
PRNT_proposed_payment.subsidiary_period_start_year PRNT_SBSD_PRD_STRT_YR,
PAYMENT_ATTRIBUTION.OP,
1 AS TBL_PRIORITY
from `fsa-{env}-sbsd-cdc`.`PAYMENT_ATTRIBUTION`
       LEFT JOIN `fsa-{env}-sbsd-cdc`.`PRNT_PAYMENT_ATTRIBUTION`
       on (PAYMENT_ATTRIBUTION.parent_payment_attribution_identifier = PRNT_PAYMENT_ATTRIBUTION.payment_attribution_identifier) 
       LEFT JOIN `fsa-{env}-sbsd-cdc`.`subsidiary_customer`
       ON (PAYMENT_ATTRIBUTION.subsidiary_customer_identifier = subsidiary_customer.subsidiary_customer_identifier)
       Left join `fsa-{env}-sbsd-cdc`.`fsa_county`
       On (subsidiary_customer.fsa_county_identifier =fsa_county.fsa_county_identifier)
             /*join to subsidary customer again to get parent values*/
       LEFT JOIN `fsa-{env}-sbsd-cdc`.`subsidiary_customer` parent_subsidiary_customer
       ON (PRNT_PAYMENT_ATTRIBUTION.subsidiary_customer_identifier = parent_subsidiary_customer.subsidiary_customer_identifier)
            /*join to fsa county again to get parent values*/
        left join `fsa-{env}-sbsd-cdc`.`fsa_county` parent_fsa_county
        on (parent_subsidiary_customer.fsa_county_identifier=parent_fsa_county.fsa_county_identifier)
   /*get paid customer information*/
       LEFT JOIN `fsa-{env}-sbsd-cdc`.`subsidiary_customer` paid_subsidiary_customer
       ON (PAYMENT_ATTRIBUTION.paid_subsidiary_customer_identifier = paid_subsidiary_customer.subsidiary_customer_identifier)
             /*get paid fsa county information*/
        left join `fsa-{env}-sbsd-cdc`.`fsa_county` paid_fsa_county
        on (paid_subsidiary_customer.fsa_county_identifier = paid_fsa_county.fsa_county_identifier)
       Left join `fsa-{env}-sbsd-cdc`.`PROPOSED_PAYMENT` 
        ON (PAYMENT_ATTRIBUTION.proposed_payment_identifier =proposed_payment.proposed_payment_identifier)
        /*join to Proposed Payment again to get parent values*/
             Left join `fsa-{env}-sbsd-cdc`.`PROPOSED_PAYMENT`  PRNT_PROPOSED_PAYMENT
            ON (PRNT_payment_attribution.proposed_payment_identifier =PRNT_PROPOSED_PAYMENT.proposed_payment_identifier)
WHERE SBSD_CUST_MAIN.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND SBSD_CUST_MAIN.OP <> 'D'

UNION
SELECT 
PAYMENT_ATTRIBUTION.payment_attribution_identifier PYMT_ATRB_ID,
PAYMENT_ATTRIBUTION.proposed_payment_identifier PRPS_PYMT_ID,
PAYMENT_ATTRIBUTION.accounting_program_code ACCT_PGM_CD,
PAYMENT_ATTRIBUTION.agi_eligible_indicator AGI_ELG_IND,
PAYMENT_ATTRIBUTION.agi_error_reference_code AGI_ERR_REF_CD,
PAYMENT_ATTRIBUTION.business_type_code BUS_TYPE_CD,
PAYMENT_ATTRIBUTION.cash_rent_cropland_factor CASH_RENT_CPLD_FCTR,
PAYMENT_ATTRIBUTION.combined_party_payment_share_percentage CMB_PTY_PYMT_SHR_PCT,
PAYMENT_ATTRIBUTION.member_contribution_eligible_indicator MBR_CTRB_ELG_IND,
PAYMENT_ATTRIBUTION.member_contribution_share_percentage MBR_CTRB_SHR_PCT,
PAYMENT_ATTRIBUTION.member_share_percentage MBR_SHR_PCT,
PAYMENT_ATTRIBUTION.net_payment_amount NET_PYMT_AMT,
PAYMENT_ATTRIBUTION.ownership_hierarchy_level_share_percentage OWNSHP_HRCH_LVL_SHR_PCT,
PAYMENT_ATTRIBUTION.paid_party_share_percentage PAID_PTY_SHR_PCT,
PAYMENT_ATTRIBUTION.paid_subsidiary_customer_identifier PAID_SBSD_CUST_ID,
PAYMENT_ATTRIBUTION.program_ineligibility_reason_type_code PGM_INELG_RSN_TYPE_CD,
PAYMENT_ATTRIBUTION.permitted_entity_eligible_indicator PMIT_ENTY_ELG_IND,
PAYMENT_ATTRIBUTION.parent_payment_attribution_identifier PRNT_PYMT_ATRB_ID,
PAYMENT_ATTRIBUTION.subsidiary_period_start_year SBSD_PRD_STRT_YR,
PAYMENT_ATTRIBUTION.payment_attribution_amount PYMT_ATRB_AMT,
PAYMENT_ATTRIBUTION.payment_attribution_business_category_code PYMT_ATRB_BUS_CAT_CD,
PAYMENT_ATTRIBUTION.payment_attribution_share_percentage PYMT_ATRB_SHR_PCT,
PAYMENT_ATTRIBUTION.payment_limitation_year PYMT_LMT_YR,
PAYMENT_ATTRIBUTION.limited_payment_program_identifier PYMT_PGM_ID,
PAYMENT_ATTRIBUTION.payment_party_relationship_code PYMT_PTY_RLT_CD,
PAYMENT_ATTRIBUTION.record_reference_primary_identification RCD_REF_PRIM_ID,
PAYMENT_ATTRIBUTION.record_reference_primary_identification_type_code RCD_REF_PRIM_ID_TYPE_CD,
PAYMENT_ATTRIBUTION.record_reference_secondary_identification RCD_REF_SCND_ID,
PAYMENT_ATTRIBUTION.record_reference_secondary_identification_type_code RCD_REF_SCND_ID_TYPE_CD,
PAYMENT_ATTRIBUTION.subsidiary_customer_identifier SBSD_CUST_ID,
PAYMENT_ATTRIBUTION.substantive_change_eligible_indicator SBST_CHG_ELG_IND,
PAYMENT_ATTRIBUTION.tax_identification_type_code TAX_ID_TYPE_CD,
PAYMENT_ATTRIBUTION.total_net_payment_amount TOT_NET_PYMT_AMT,
PAYMENT_ATTRIBUTION.total_payment_attribution_amount TOT_PYMT_ATRB_AMT,
PAYMENT_ATTRIBUTION.creation_date CRE_DT,
PAYMENT_ATTRIBUTION.creation_user_name CRE_USER_NM,
PAYMENT_ATTRIBUTION.data_status_code DATA_STAT_CD,
PAYMENT_ATTRIBUTION.foreign_person_eligible_indicator FGN_PRSN_ELG_IND,
PAYMENT_ATTRIBUTION.last_change_date LAST_CHG_DT,
PAYMENT_ATTRIBUTION.last_change_user_name LAST_CHG_USER_NM,
fsa_county.state_county_fsa_code MBR_CORE_CUST_ST_CNTY_FSA_CD,
parent_fsa_county.state_county_fsa_code PRNT_ATRB_CUST_ST_CNTY_FSA_CD,
paid_fsa_county.state_county_fsa_code PAID_CORE_CUST_ST_CNTY_FSA_CD,
subsidiary_customer.core_customer_identifier MBR_CORE_CUST_ID,
parent_subsidiary_customer.core_customer_identifier PRNT_PYMT_ATRB_CORE_CUST_ID,
paid_subsidiary_customer.core_customer_identifier PAID_CORE_CUST_ID,
proposed_payment.state_fsa_code ST_FSA_CD,
proposed_payment.county_fsa_code CNTY_FSA_CD,
proposed_payment.accounting_program_code PRPS_PYMT_ACCT_PGM_CD,
proposed_payment.subsidiary_period_start_year PRPS_PYMT_SBSD_PRD_STRT_YR,
PRNT_proposed_payment.accounting_program_code PRNT_PYMT_ATRB_ACCT_PGM_CD,
PRNT_proposed_payment.county_fsa_code PRNT_PYMT_ATRB_CNTY_FSA_CD,
PRNT_proposed_payment.state_fsa_code PRNT_PYMT_ATRB_ST_FSA_CD,
PRNT_proposed_payment.subsidiary_period_start_year PRNT_SBSD_PRD_STRT_YR,
SBSD_CUST_MAIN.OP,
2 AS TBL_PRIORITY
from
  ( 
Select SBSD_PRE.* from (
Select * FROM `fsa-{env}-sbsd-cdc`.`SBSD_CUST_MAIN`     where  SBSD_CUST_MAIN.OP ='UN' And cast('{ETL_END_DATE}' as date)  <> cast(current_date as date)
     UNION
     /*same day scenario update happens*/
    Select * FROM `fsa-{env}-sbsd-cdc`.`SBSD_CUST_MAIN` where  SBSD_CUST_MAIN.OP ='UN'
       And cast('{ETL_END_DATE}' as date)  = cast(current_date as date)
     )  SBSD_PRE
  JOIN
   ( Select * from `fsa-{env}-sbsd-cdc`.`subsidiary_customer`   
   ) SBSD_CUST_MAIN
ON  ( SBSD_PRE.subsidiary_customer_identifier = SBSD_CUST_MAIN.subsidiary_customer_identifier 
       And ( SBSD_PRE.fsa_county_identifier <> SBSD_CUST_MAIN.fsa_county_identifier Or SBSD_PRE.core_customer_identifier <> SBSD_CUST_MAIN.core_customer_identifier )
     )
     UNION
   Select * FROM `fsa-{env}-sbsd-cdc`.`SBSD_CUST_MAIN`  where   SBSD_CUST_MAIN.OP ='I'
 ) subsidiary_customer
       JOIN    `fsa-{env}-sbsd-cdc`.`PRNT_PAYMENT_ATTRIBUTION`
       ON (PAYMENT_ATTRIBUTION.subsidiary_customer_identifier = subsidiary_customer.subsidiary_customer_identifier)
       LEFT JOIN `fsa-{env}-cnsv`.`PRNT_PAYMENT_ATTRIBUTION`
       on (PAYMENT_ATTRIBUTION.parent_payment_attribution_identifier = PRNT_PAYMENT_ATTRIBUTION.payment_attribution_identifier)
        Left join `fsa-{env}-sbsd-cdc`.`fsa_county` fsa_county
       On (subsidiary_customer.fsa_county_identifier =fsa_county.fsa_county_identifier)
             /*join to subsidary customer again to get parent values*/
       LEFT JOIN `fsa-{env}-sbsd-cdc`.`subsidiary_customer` parent_subsidiary_customer
       ON (PRNT_PAYMENT_ATTRIBUTION.subsidiary_customer_identifier = parent_subsidiary_customer.subsidiary_customer_identifier)
            /*join to fsa county again to get parent values*/
        left join `fsa-{env}-sbsd-cdc`.`fsa_county` parent_fsa_county
        on (parent_subsidiary_customer.fsa_county_identifier=parent_fsa_county.fsa_county_identifier)
   /*get paid customer information*/
       LEFT JOIN `fsa-{env}-sbsd-cdc`.`subsidiary_customer` paid_subsidiary_customer
       ON (PAYMENT_ATTRIBUTION.paid_subsidiary_customer_identifier = paid_subsidiary_customer.subsidiary_customer_identifier)
             /*get paid fsa county information*/
        left join `fsa-{env}-sbsd-cdc`.`fsa_county` paid_fsa_county
        on (paid_subsidiary_customer.fsa_county_identifier = paid_fsa_county.fsa_county_identifier)
       Left join `fsa-{env}-sbsd-cdc`.`PROPOSED_PAYMENT` 
        ON (PAYMENT_ATTRIBUTION.proposed_payment_identifier =proposed_payment.proposed_payment_identifier)
        /*join to Proposed Payment again to get parent values*/
             Left join `fsa-{env}-sbsd-cdc`.`PROPOSED_PAYMENT`  PRNT_PROPOSED_PAYMENT
            ON (PRNT_payment_attribution.proposed_payment_identifier =PRNT_PROPOSED_PAYMENT.proposed_payment_identifier)
WHERE SBSD_CUST_MAIN.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND SBSD_CUST_MAIN.OP <> 'D'

UNION
SELECT 
PAYMENT_ATTRIBUTION.payment_attribution_identifier PYMT_ATRB_ID,
PAYMENT_ATTRIBUTION.proposed_payment_identifier PRPS_PYMT_ID,
PAYMENT_ATTRIBUTION.accounting_program_code ACCT_PGM_CD,
PAYMENT_ATTRIBUTION.agi_eligible_indicator AGI_ELG_IND,
PAYMENT_ATTRIBUTION.agi_error_reference_code AGI_ERR_REF_CD,
PAYMENT_ATTRIBUTION.business_type_code BUS_TYPE_CD,
PAYMENT_ATTRIBUTION.cash_rent_cropland_factor CASH_RENT_CPLD_FCTR,
PAYMENT_ATTRIBUTION.combined_party_payment_share_percentage CMB_PTY_PYMT_SHR_PCT,
PAYMENT_ATTRIBUTION.member_contribution_eligible_indicator MBR_CTRB_ELG_IND,
PAYMENT_ATTRIBUTION.member_contribution_share_percentage MBR_CTRB_SHR_PCT,
PAYMENT_ATTRIBUTION.member_share_percentage MBR_SHR_PCT,
PAYMENT_ATTRIBUTION.net_payment_amount NET_PYMT_AMT,
PAYMENT_ATTRIBUTION.ownership_hierarchy_level_share_percentage OWNSHP_HRCH_LVL_SHR_PCT,
PAYMENT_ATTRIBUTION.paid_party_share_percentage PAID_PTY_SHR_PCT,
PAYMENT_ATTRIBUTION.paid_subsidiary_customer_identifier PAID_SBSD_CUST_ID,
PAYMENT_ATTRIBUTION.program_ineligibility_reason_type_code PGM_INELG_RSN_TYPE_CD,
PAYMENT_ATTRIBUTION.permitted_entity_eligible_indicator PMIT_ENTY_ELG_IND,
PAYMENT_ATTRIBUTION.parent_payment_attribution_identifier PRNT_PYMT_ATRB_ID,
PAYMENT_ATTRIBUTION.subsidiary_period_start_year SBSD_PRD_STRT_YR,
PAYMENT_ATTRIBUTION.payment_attribution_amount PYMT_ATRB_AMT,
PAYMENT_ATTRIBUTION.payment_attribution_business_category_code PYMT_ATRB_BUS_CAT_CD,
PAYMENT_ATTRIBUTION.payment_attribution_share_percentage PYMT_ATRB_SHR_PCT,
PAYMENT_ATTRIBUTION.payment_limitation_year PYMT_LMT_YR,
PAYMENT_ATTRIBUTION.limited_payment_program_identifier PYMT_PGM_ID,
PAYMENT_ATTRIBUTION.payment_party_relationship_code PYMT_PTY_RLT_CD,
PAYMENT_ATTRIBUTION.record_reference_primary_identification RCD_REF_PRIM_ID,
PAYMENT_ATTRIBUTION.record_reference_primary_identification_type_code RCD_REF_PRIM_ID_TYPE_CD,
PAYMENT_ATTRIBUTION.record_reference_secondary_identification RCD_REF_SCND_ID,
PAYMENT_ATTRIBUTION.record_reference_secondary_identification_type_code RCD_REF_SCND_ID_TYPE_CD,
PAYMENT_ATTRIBUTION.subsidiary_customer_identifier SBSD_CUST_ID,
PAYMENT_ATTRIBUTION.substantive_change_eligible_indicator SBST_CHG_ELG_IND,
PAYMENT_ATTRIBUTION.tax_identification_type_code TAX_ID_TYPE_CD,
PAYMENT_ATTRIBUTION.total_net_payment_amount TOT_NET_PYMT_AMT,
PAYMENT_ATTRIBUTION.total_payment_attribution_amount TOT_PYMT_ATRB_AMT,
PAYMENT_ATTRIBUTION.creation_date CRE_DT,
PAYMENT_ATTRIBUTION.creation_user_name CRE_USER_NM,
PAYMENT_ATTRIBUTION.data_status_code DATA_STAT_CD,
PAYMENT_ATTRIBUTION.foreign_person_eligible_indicator FGN_PRSN_ELG_IND,
PAYMENT_ATTRIBUTION.last_change_date LAST_CHG_DT,
PAYMENT_ATTRIBUTION.last_change_user_name LAST_CHG_USER_NM,
fsa_county.state_county_fsa_code MBR_CORE_CUST_ST_CNTY_FSA_CD,
parent_fsa_county.state_county_fsa_code PRNT_ATRB_CUST_ST_CNTY_FSA_CD,
paid_fsa_county.state_county_fsa_code PAID_CORE_CUST_ST_CNTY_FSA_CD,
subsidiary_customer.core_customer_identifier MBR_CORE_CUST_ID,
parent_subsidiary_customer.core_customer_identifier PRNT_PYMT_ATRB_CORE_CUST_ID,
paid_subsidiary_customer.core_customer_identifier PAID_CORE_CUST_ID,
proposed_payment.state_fsa_code ST_FSA_CD,
proposed_payment.county_fsa_code CNTY_FSA_CD,
proposed_payment.accounting_program_code PRPS_PYMT_ACCT_PGM_CD,
proposed_payment.subsidiary_period_start_year PRPS_PYMT_SBSD_PRD_STRT_YR,
PRNT_proposed_payment.accounting_program_code PRNT_PYMT_ATRB_ACCT_PGM_CD,
PRNT_proposed_payment.county_fsa_code PRNT_PYMT_ATRB_CNTY_FSA_CD,
PRNT_proposed_payment.state_fsa_code PRNT_PYMT_ATRB_ST_FSA_CD,
PRNT_proposed_payment.subsidiary_period_start_year PRNT_SBSD_PRD_STRT_YR,
subsidiary_customer.OP,
3 AS TBL_PRIORITY
from  (  SELECT 
    fsa_county_identifier,
    state_county_fsa_code,
    last_change_date, 
    last_change_user_name,
    subsidiary_customer.OP 
    FROM (
    SELECT *,ROW_NUMBER() OVER (PARTITION BY fsa_county_identifier
            ORDER BY LAST_CHG_DT desc) as rnum 
            FROM `fsa-{env}-sbsd-cdc`.`fsa_county` fsa_county) as SubQry
    WHERE SubQry.rnum=1
) fsa_county
     /* Need to inner join untill we reach Primary Table */
      join `fsa-{env}-sbsd-cdc`.`subsidiary_customer`
     On (subsidiary_customer.fsa_county_identifier =fsa_county.fsa_county_identifier)
      JOIN    `fsa-{env}-sbsd-cdc`.`PRNT_PAYMENT_ATTRIBUTION`
          ON (PAYMENT_ATTRIBUTION.subsidiary_customer_identifier = subsidiary_customer.subsidiary_customer_identifier)
       LEFT JOIN `fsa-{env}-cnsv`.`PRNT_PAYMENT_ATTRIBUTION`
       on (PAYMENT_ATTRIBUTION.parent_payment_attribution_identifier = PRNT_PAYMENT_ATTRIBUTION.payment_attribution_identifier)
                 /*join to subsidary customer again to get parent values*/
       LEFT JOIN `fsa-{env}-sbsd-cdc`.`subsidiary_customer` parent_subsidiary_customer
       ON (PRNT_PAYMENT_ATTRIBUTION.subsidiary_customer_identifier = parent_subsidiary_customer.subsidiary_customer_identifier)
            /*join to fsa county again to get parent values*/
        left join `fsa-{env}-sbsd-cdc`.`fsa_county` parent_fsa_county
        on (parent_subsidiary_customer.fsa_county_identifier=parent_fsa_county.fsa_county_identifier)
   /*get paid customer information*/
       LEFT JOIN `fsa-{env}-sbsd-cdc`.`subsidiary_customer` paid_subsidiary_customer
       ON (PAYMENT_ATTRIBUTION.paid_subsidiary_customer_identifier = paid_subsidiary_customer.subsidiary_customer_identifier)
             /*get paid fsa county information*/
        left join `fsa-{env}-sbsd-cdc`.`fsa_county` paid_fsa_county
        on (paid_subsidiary_customer.fsa_county_identifier = paid_fsa_county.fsa_county_identifier)
       Left join `fsa-{env}-sbsd-cdc`.`PROPOSED_PAYMENT` 
        ON (PAYMENT_ATTRIBUTION.proposed_payment_identifier =proposed_payment.proposed_payment_identifier)
        /*join to Proposed Payment again to get parent values*/
             Left join `fsa-{env}-sbsd-cdc`.`PROPOSED_PAYMENT`  PRNT_PROPOSED_PAYMENT
            ON (PRNT_payment_attribution.proposed_payment_identifier =PRNT_PROPOSED_PAYMENT.proposed_payment_identifier)
WHERE subsidiary_customer.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND subsidiary_customer.OP <> 'D'

UNION
SELECT 
PAYMENT_ATTRIBUTION.payment_attribution_identifier PYMT_ATRB_ID,
PAYMENT_ATTRIBUTION.proposed_payment_identifier PRPS_PYMT_ID,
PAYMENT_ATTRIBUTION.accounting_program_code ACCT_PGM_CD,
PAYMENT_ATTRIBUTION.agi_eligible_indicator AGI_ELG_IND,
PAYMENT_ATTRIBUTION.agi_error_reference_code AGI_ERR_REF_CD,
PAYMENT_ATTRIBUTION.business_type_code BUS_TYPE_CD,
PAYMENT_ATTRIBUTION.cash_rent_cropland_factor CASH_RENT_CPLD_FCTR,
PAYMENT_ATTRIBUTION.combined_party_payment_share_percentage CMB_PTY_PYMT_SHR_PCT,
PAYMENT_ATTRIBUTION.member_contribution_eligible_indicator MBR_CTRB_ELG_IND,
PAYMENT_ATTRIBUTION.member_contribution_share_percentage MBR_CTRB_SHR_PCT,
PAYMENT_ATTRIBUTION.member_share_percentage MBR_SHR_PCT,
PAYMENT_ATTRIBUTION.net_payment_amount NET_PYMT_AMT,
PAYMENT_ATTRIBUTION.ownership_hierarchy_level_share_percentage OWNSHP_HRCH_LVL_SHR_PCT,
PAYMENT_ATTRIBUTION.paid_party_share_percentage PAID_PTY_SHR_PCT,
PAYMENT_ATTRIBUTION.paid_subsidiary_customer_identifier PAID_SBSD_CUST_ID,
PAYMENT_ATTRIBUTION.program_ineligibility_reason_type_code PGM_INELG_RSN_TYPE_CD,
PAYMENT_ATTRIBUTION.permitted_entity_eligible_indicator PMIT_ENTY_ELG_IND,
PAYMENT_ATTRIBUTION.parent_payment_attribution_identifier PRNT_PYMT_ATRB_ID,
PAYMENT_ATTRIBUTION.subsidiary_period_start_year SBSD_PRD_STRT_YR,
PAYMENT_ATTRIBUTION.payment_attribution_amount PYMT_ATRB_AMT,
PAYMENT_ATTRIBUTION.payment_attribution_business_category_code PYMT_ATRB_BUS_CAT_CD,
PAYMENT_ATTRIBUTION.payment_attribution_share_percentage PYMT_ATRB_SHR_PCT,
PAYMENT_ATTRIBUTION.payment_limitation_year PYMT_LMT_YR,
PAYMENT_ATTRIBUTION.limited_payment_program_identifier PYMT_PGM_ID,
PAYMENT_ATTRIBUTION.payment_party_relationship_code PYMT_PTY_RLT_CD,
PAYMENT_ATTRIBUTION.record_reference_primary_identification RCD_REF_PRIM_ID,
PAYMENT_ATTRIBUTION.record_reference_primary_identification_type_code RCD_REF_PRIM_ID_TYPE_CD,
PAYMENT_ATTRIBUTION.record_reference_secondary_identification RCD_REF_SCND_ID,
PAYMENT_ATTRIBUTION.record_reference_secondary_identification_type_code RCD_REF_SCND_ID_TYPE_CD,
PAYMENT_ATTRIBUTION.subsidiary_customer_identifier SBSD_CUST_ID,
PAYMENT_ATTRIBUTION.substantive_change_eligible_indicator SBST_CHG_ELG_IND,
PAYMENT_ATTRIBUTION.tax_identification_type_code TAX_ID_TYPE_CD,
PAYMENT_ATTRIBUTION.total_net_payment_amount TOT_NET_PYMT_AMT,
PAYMENT_ATTRIBUTION.total_payment_attribution_amount TOT_PYMT_ATRB_AMT,
PAYMENT_ATTRIBUTION.creation_date CRE_DT,
PAYMENT_ATTRIBUTION.creation_user_name CRE_USER_NM,
PAYMENT_ATTRIBUTION.data_status_code DATA_STAT_CD,
PAYMENT_ATTRIBUTION.foreign_person_eligible_indicator FGN_PRSN_ELG_IND,
PAYMENT_ATTRIBUTION.last_change_date LAST_CHG_DT,
PAYMENT_ATTRIBUTION.last_change_user_name LAST_CHG_USER_NM,
fsa_county.state_county_fsa_code MBR_CORE_CUST_ST_CNTY_FSA_CD,
parent_fsa_county.state_county_fsa_code PRNT_ATRB_CUST_ST_CNTY_FSA_CD,
paid_fsa_county.state_county_fsa_code PAID_CORE_CUST_ST_CNTY_FSA_CD,
subsidiary_customer.core_customer_identifier MBR_CORE_CUST_ID,
parent_subsidiary_customer.core_customer_identifier PRNT_PYMT_ATRB_CORE_CUST_ID,
paid_subsidiary_customer.core_customer_identifier PAID_CORE_CUST_ID,
proposed_payment.state_fsa_code ST_FSA_CD,
proposed_payment.county_fsa_code CNTY_FSA_CD,
proposed_payment.accounting_program_code PRPS_PYMT_ACCT_PGM_CD,
proposed_payment.subsidiary_period_start_year PRPS_PYMT_SBSD_PRD_STRT_YR,
PRNT_proposed_payment.accounting_program_code PRNT_PYMT_ATRB_ACCT_PGM_CD,
PRNT_proposed_payment.county_fsa_code PRNT_PYMT_ATRB_CNTY_FSA_CD,
PRNT_proposed_payment.state_fsa_code PRNT_PYMT_ATRB_ST_FSA_CD,
PRNT_proposed_payment.subsidiary_period_start_year PRNT_SBSD_PRD_STRT_YR,
PROPOSED_PAYMENT.OP,
4 AS TBL_PRIORITY
from `fsa-{env}-sbsd-cdc`.`PROPOSED_PAYMENT`
       JOIN    `fsa-{env}-sbsd-cdc`.`PRNT_PAYMENT_ATTRIBUTION`
       ON (PAYMENT_ATTRIBUTION.proposed_payment_identifier =proposed_payment.proposed_payment_identifier)
        /*join to PAYMENT_ATTRIBUTION again to get parent values*/
       LEFT JOIN `fsa-{env}-cnsv`.`PRNT_PAYMENT_ATTRIBUTION`
       on (PAYMENT_ATTRIBUTION.parent_payment_attribution_identifier = PRNT_PAYMENT_ATTRIBUTION.payment_attribution_identifier) 
       Left join `fsa-{env}-sbsd-cdc`.`subsidiary_customer`
       ON (PAYMENT_ATTRIBUTION.subsidiary_customer_identifier = subsidiary_customer.subsidiary_customer_identifier)
       Left join    `fsa-{env}-sbsd-cdc`.`fsa_county`
       On (subsidiary_customer.fsa_county_identifier =fsa_county.fsa_county_identifier)
                     /*join to subsidary customer again to get parent values*/
       LEFT JOIN `fsa-{env}-sbsd-cdc`.`subsidiary_customer` parent_subsidiary_customer
       ON (PRNT_PAYMENT_ATTRIBUTION.subsidiary_customer_identifier = parent_subsidiary_customer.subsidiary_customer_identifier)
            /*join to fsa county again to get parent values*/
        left join `fsa-{env}-sbsd-cdc`.`fsa_county` parent_fsa_county
        on (parent_subsidiary_customer.fsa_county_identifier=parent_fsa_county.fsa_county_identifier)
       /*get paid customer information*/
       LEFT JOIN `fsa-{env}-sbsd-cdc`.`subsidiary_customer` paid_subsidiary_customer
       ON (PAYMENT_ATTRIBUTION.paid_subsidiary_customer_identifier = paid_subsidiary_customer.subsidiary_customer_identifier)
             /*get paid fsa county information*/
        left join `fsa-{env}-sbsd-cdc`.`fsa_county` paid_fsa_county
        on (paid_subsidiary_customer.fsa_county_identifier = paid_fsa_county.fsa_county_identifier)
                  /*join to Proposed Payment again to get parent values*/
          Left join `fsa-{env}-sbsd-cdc`.`PROPOSED_PAYMENT`  PRNT_PROPOSED_PAYMENT
       ON (PRNT_payment_attribution.proposed_payment_identifier =PRNT_PROPOSED_PAYMENT.proposed_payment_identifier)
WHERE PAYMENT_ATTRIBUTION.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND PAYMENT_ATTRIBUTION <> 'D'

) STG_ALL
) STG_UNQ
WHERE row_num_part = 1
AND (
 (COALESCE(CAST(CRE_DT AS DATE), DATE '1900-01-01') <= CAST('{ETL_START_DATE}'  AS DATE))
    AND
    (COALESCE(CAST(LAST_CHG_DT AS DATE), DATE '1900-01-01') <= CAST('{ETL_START_DATE}'  AS DATE))
)
UNION
SELECT DISTINCT
PAYMENT_ATTRIBUTION.payment_attribution_identifier PYMT_ATRB_ID,
PAYMENT_ATTRIBUTION.proposed_payment_identifier PRPS_PYMT_ID,
PAYMENT_ATTRIBUTION.accounting_program_code ACCT_PGM_CD,
PAYMENT_ATTRIBUTION.agi_eligible_indicator AGI_ELG_IND,
PAYMENT_ATTRIBUTION.agi_error_reference_code AGI_ERR_REF_CD,
PAYMENT_ATTRIBUTION.business_type_code BUS_TYPE_CD,
PAYMENT_ATTRIBUTION.cash_rent_cropland_factor CASH_RENT_CPLD_FCTR,
PAYMENT_ATTRIBUTION.combined_party_payment_share_percentage CMB_PTY_PYMT_SHR_PCT,
PAYMENT_ATTRIBUTION.member_contribution_eligible_indicator MBR_CTRB_ELG_IND,
PAYMENT_ATTRIBUTION.member_contribution_share_percentage MBR_CTRB_SHR_PCT,
PAYMENT_ATTRIBUTION.member_share_percentage MBR_SHR_PCT,
PAYMENT_ATTRIBUTION.net_payment_amount NET_PYMT_AMT,
PAYMENT_ATTRIBUTION.ownership_hierarchy_level_share_percentage OWNSHP_HRCH_LVL_SHR_PCT,
PAYMENT_ATTRIBUTION.paid_party_share_percentage PAID_PTY_SHR_PCT,
PAYMENT_ATTRIBUTION.paid_subsidiary_customer_identifier PAID_SBSD_CUST_ID,
PAYMENT_ATTRIBUTION.program_ineligibility_reason_type_code PGM_INELG_RSN_TYPE_CD,
PAYMENT_ATTRIBUTION.permitted_entity_eligible_indicator PMIT_ENTY_ELG_IND,
PAYMENT_ATTRIBUTION.parent_payment_attribution_identifier PRNT_PYMT_ATRB_ID,
PAYMENT_ATTRIBUTION.subsidiary_period_start_year SBSD_PRD_STRT_YR,
PAYMENT_ATTRIBUTION.payment_attribution_amount PYMT_ATRB_AMT,
PAYMENT_ATTRIBUTION.payment_attribution_business_category_code PYMT_ATRB_BUS_CAT_CD,
PAYMENT_ATTRIBUTION.payment_attribution_share_percentage PYMT_ATRB_SHR_PCT,
PAYMENT_ATTRIBUTION.payment_limitation_year PYMT_LMT_YR,
PAYMENT_ATTRIBUTION.limited_payment_program_identifier PYMT_PGM_ID,
PAYMENT_ATTRIBUTION.payment_party_relationship_code PYMT_PTY_RLT_CD,
PAYMENT_ATTRIBUTION.record_reference_primary_identification RCD_REF_PRIM_ID,
PAYMENT_ATTRIBUTION.record_reference_primary_identification_type_code RCD_REF_PRIM_ID_TYPE_CD,
PAYMENT_ATTRIBUTION.record_reference_secondary_identification RCD_REF_SCND_ID,
PAYMENT_ATTRIBUTION.record_reference_secondary_identification_type_code RCD_REF_SCND_ID_TYPE_CD,
PAYMENT_ATTRIBUTION.subsidiary_customer_identifier SBSD_CUST_ID,
PAYMENT_ATTRIBUTION.substantive_change_eligible_indicator SBST_CHG_ELG_IND,
PAYMENT_ATTRIBUTION.tax_identification_type_code TAX_ID_TYPE_CD,
PAYMENT_ATTRIBUTION.total_net_payment_amount TOT_NET_PYMT_AMT,
PAYMENT_ATTRIBUTION.total_payment_attribution_amount TOT_PYMT_ATRB_AMT,
PAYMENT_ATTRIBUTION.creation_date CRE_DT,
PAYMENT_ATTRIBUTION.creation_user_name CRE_USER_NM,
PAYMENT_ATTRIBUTION.data_status_code DATA_STAT_CD,
PAYMENT_ATTRIBUTION.foreign_person_eligible_indicator FGN_PRSN_ELG_IND,
PAYMENT_ATTRIBUTION.last_change_date LAST_CHG_DT,
PAYMENT_ATTRIBUTION.last_change_user_name LAST_CHG_USER_NM,
NULL MBR_CORE_CUST_ST_CNTY_FSA_CD,
NULL PRNT_ATRB_CUST_ST_CNTY_FSA_CD,
NULL PAID_CORE_CUST_ST_CNTY_FSA_CD,
NULL MBR_CORE_CUST_ID,
NULL PRNT_PYMT_ATRB_CORE_CUST_ID,
NULL PAID_CORE_CUST_ID,
NULL ST_FSA_CD,
NULL CNTY_FSA_CD,
NULL PRPS_PYMT_ACCT_PGM_CD,
NULL PRPS_PYMT_SBSD_PRD_STRT_YR,
NULL PRNT_PYMT_ATRB_ACCT_PGM_CD,
NULL PRNT_PYMT_ATRB_CNTY_FSA_CD,
NULL PRNT_PYMT_ATRB_ST_FSA_CD,
NULL PRNT_SBSD_PRD_STRT_YR,
'D' OP,
1 AS row_num_part
FROM  `fsa-{env}-sbsd-cdc`.`PAYMENT_ATTRIBUTION`
WHERE PAYMENT_ATTRIBUTION.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND PAYMENT_ATTRIBUTION.OP = 'D')