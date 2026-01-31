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
DATA_ACTN_CD,
DATA_ACTN_DT,
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
SELECT PYMT_ATRB_ID,
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
DATA_ACTN_CD,
DATA_ACTN_DT,
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
ROW_NUMBER() over ( partition by PYMT_ATRB_ID,PRPS_PYMT_ID,DATA_ACTN_DT
order by TBL_PRIORITY ASC, LAST_CHG_DT DESC) AS row_num_part
FROM
(
SELECT 
NET_CHANGES.PYMT_ATRB_ID,
NET_CHANGES.PRPS_PYMT_ID,
NET_CHANGES.ACCT_PGM_CD,
NET_CHANGES.AGI_ELG_IND,
NET_CHANGES.AGI_ERR_REF_CD,
NET_CHANGES.BUS_TYPE_CD,
NET_CHANGES.CASH_RENT_CPLD_FCTR,
NET_CHANGES.CMB_PTY_PYMT_SHR_PCT,
NET_CHANGES.MBR_CTRB_ELG_IND,
NET_CHANGES.MBR_CTRB_SHR_PCT,
NET_CHANGES.MBR_SHR_PCT,
NET_CHANGES.NET_PYMT_AMT,
NET_CHANGES.OWNSHP_HRCH_LVL_SHR_PCT,
NET_CHANGES.PAID_PTY_SHR_PCT,
NET_CHANGES.PAID_SBSD_CUST_ID,
NET_CHANGES.PGM_INELG_RSN_TYPE_CD,
NET_CHANGES.PMIT_ENTY_ELG_IND,
NET_CHANGES.PRNT_PYMT_ATRB_ID,
NET_CHANGES.SBSD_PRD_STRT_YR,
NET_CHANGES.PYMT_ATRB_AMT,
NET_CHANGES.PYMT_ATRB_BUS_CAT_CD,
NET_CHANGES.PYMT_ATRB_SHR_PCT,
NET_CHANGES.PYMT_LMT_YR,
NET_CHANGES.PYMT_PGM_ID,
NET_CHANGES.PYMT_PTY_RLT_CD,
NET_CHANGES.RCD_REF_PRIM_ID,
NET_CHANGES.RCD_REF_PRIM_ID_TYPE_CD,
NET_CHANGES.RCD_REF_SCND_ID,
NET_CHANGES.RCD_REF_SCND_ID_TYPE_CD,
NET_CHANGES.SBSD_CUST_ID,
NET_CHANGES.SBST_CHG_ELG_IND,
NET_CHANGES.TAX_ID_TYPE_CD,
NET_CHANGES.TOT_NET_PYMT_AMT,
NET_CHANGES.TOT_PYMT_ATRB_AMT,
NET_CHANGES.CRE_DT,
NET_CHANGES.CRE_USER_NM,
NET_CHANGES.DATA_STAT_CD,
NET_CHANGES.FGN_PRSN_ELG_IND,
NET_CHANGES.LAST_CHG_DT,
NET_CHANGES.LAST_CHG_USER_NM,
NET_CHANGES.DATA_ACTN_CD,
NET_CHANGES.DATA_ACTN_DT,
NET_CHANGES.MBR_CORE_CUST_ST_CNTY_FSA_CD,
NET_CHANGES.PRNT_ATRB_CUST_ST_CNTY_FSA_CD,
NET_CHANGES.PAID_CORE_CUST_ST_CNTY_FSA_CD,
NET_CHANGES.MBR_CORE_CUST_ID,
NET_CHANGES.PRNT_PYMT_ATRB_CORE_CUST_ID,
NET_CHANGES.PAID_CORE_CUST_ID,
NET_CHANGES.ST_FSA_CD,
NET_CHANGES.CNTY_FSA_CD,
NET_CHANGES.PRPS_PYMT_ACCT_PGM_CD,
NET_CHANGES.PRPS_PYMT_SBSD_PRD_STRT_YR,
NET_CHANGES.PRNT_PYMT_ATRB_ACCT_PGM_CD,
NET_CHANGES.PRNT_PYMT_ATRB_CNTY_FSA_CD,
NET_CHANGES.PRNT_PYMT_ATRB_ST_FSA_CD,
NET_CHANGES.PRNT_SBSD_PRD_STRT_YR,
NET_CHANGES.__CDC_OPERATION,
NET_CHANGES.TBL_PRIORITY,
NET_CHANGES.__CDC_STARTLSN,
NET_CHANGES.__CDC_SEQVAL
FROM ( 
SELECT 
PAYMENT_ATTRIBUTION_AUDIT.payment_attribution_identifier PYMT_ATRB_ID,
PAYMENT_ATTRIBUTION_AUDIT.proposed_payment_identifier PRPS_PYMT_ID,
PAYMENT_ATTRIBUTION_AUDIT.accounting_program_code ACCT_PGM_CD,
PAYMENT_ATTRIBUTION_AUDIT.agi_eligible_indicator AGI_ELG_IND,
PAYMENT_ATTRIBUTION_AUDIT.agi_error_reference_code AGI_ERR_REF_CD,
PAYMENT_ATTRIBUTION_AUDIT.business_type_code BUS_TYPE_CD,
PAYMENT_ATTRIBUTION_AUDIT.cash_rent_cropland_factor CASH_RENT_CPLD_FCTR,
PAYMENT_ATTRIBUTION_AUDIT.combined_party_payment_share_percentage CMB_PTY_PYMT_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.member_contribution_eligible_indicator MBR_CTRB_ELG_IND,
PAYMENT_ATTRIBUTION_AUDIT.member_contribution_share_percentage MBR_CTRB_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.member_share_percentage MBR_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.net_payment_amount NET_PYMT_AMT,
PAYMENT_ATTRIBUTION_AUDIT.ownership_hierarchy_level_share_percentage OWNSHP_HRCH_LVL_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.paid_party_share_percentage PAID_PTY_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.paid_subsidiary_customer_identifier PAID_SBSD_CUST_ID,
PAYMENT_ATTRIBUTION_AUDIT.program_ineligibility_reason_type_code PGM_INELG_RSN_TYPE_CD,
PAYMENT_ATTRIBUTION_AUDIT.permitted_entity_eligible_indicator PMIT_ENTY_ELG_IND,
PAYMENT_ATTRIBUTION_AUDIT.parent_payment_attribution_identifier PRNT_PYMT_ATRB_ID,
PAYMENT_ATTRIBUTION_AUDIT.subsidiary_period_start_year  SBSD_PRD_STRT_YR,
PAYMENT_ATTRIBUTION_AUDIT.payment_attribution_amount PYMT_ATRB_AMT,
PAYMENT_ATTRIBUTION_AUDIT.payment_attribution_business_category_code PYMT_ATRB_BUS_CAT_CD,
PAYMENT_ATTRIBUTION_AUDIT.payment_attribution_share_percentage PYMT_ATRB_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.payment_limitation_year PYMT_LMT_YR,
PAYMENT_ATTRIBUTION_AUDIT.limited_payment_program_identifier PYMT_PGM_ID,
PAYMENT_ATTRIBUTION_AUDIT.payment_party_relationship_code PYMT_PTY_RLT_CD,
PAYMENT_ATTRIBUTION_AUDIT.record_reference_primary_identification RCD_REF_PRIM_ID,
PAYMENT_ATTRIBUTION_AUDIT.record_reference_primary_identification_type_code   RCD_REF_PRIM_ID_TYPE_CD,
PAYMENT_ATTRIBUTION_AUDIT.record_reference_secondary_identification RCD_REF_SCND_ID,
PAYMENT_ATTRIBUTION_AUDIT.record_reference_secondary_identification_type_code RCD_REF_SCND_ID_TYPE_CD,
PAYMENT_ATTRIBUTION_AUDIT.subsidiary_customer_identifier SBSD_CUST_ID,
PAYMENT_ATTRIBUTION_AUDIT.substantive_change_eligible_indicator SBST_CHG_ELG_IND,
PAYMENT_ATTRIBUTION_AUDIT.tax_identification_type_code  TAX_ID_TYPE_CD,
PAYMENT_ATTRIBUTION_AUDIT.total_net_payment_amount TOT_NET_PYMT_AMT,
PAYMENT_ATTRIBUTION_AUDIT.total_payment_attribution_amount TOT_PYMT_ATRB_AMT,
PAYMENT_ATTRIBUTION_AUDIT.creation_date CRE_DT,
PAYMENT_ATTRIBUTION_AUDIT.creation_user_name CRE_USER_NM,
PAYMENT_ATTRIBUTION_AUDIT.data_status_code DATA_STAT_CD,
PAYMENT_ATTRIBUTION_AUDIT.foreign_person_eligible_indicator FGN_PRSN_ELG_IND,
PAYMENT_ATTRIBUTION_AUDIT.last_change_date LAST_CHG_DT,
PAYMENT_ATTRIBUTION_AUDIT.last_change_user_name LAST_CHG_USER_NM,
PAYMENT_ATTRIBUTION_AUDIT.data_action_code DATA_ACTN_CD,
PAYMENT_ATTRIBUTION_AUDIT.data_action_date DATA_ACTN_DT,
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
PRNT_proposed_payment.subsidiary_period_start_year  PRNT_SBSD_PRD_STRT_YR,
PAYMENT_ATTRIBUTION_AUDIT.OP,
1 AS TBL_PRIORITY,
ROW_NUMBER() over ( partition by PAYMENT_ATTRIBUTION_AUDIT.payment_attribution_identifier,
PAYMENT_ATTRIBUTION_AUDIT.proposed_payment_identifier,
PAYMENT_ATTRIBUTION_AUDIT.data_action_date 
ORDER BY LAST_CHG_DT DESC
) AS row_num_part
From `fsa-{env}-cnsv-cdc`.`PAYMENT_ATTRIBUTION_AUDIT`
       LEFT JOIN  
   (
    Select * From  `fsa-{env}-cnsv`.`PAYMENT_ATTRIBUTION_AUDIT`  where cast(Date,PAYMENT_ATTRIBUTION_AUDIT.data_action_date) <='{ETL_END_DATE}'
  ) PRNT_PAYMENT_ATTRIBUTION_AUDIT
       on (PAYMENT_ATTRIBUTION_AUDIT.parent_payment_attribution_identifier = PRNT_PAYMENT_ATTRIBUTION_AUDIT.payment_attribution_identifier)
       And (PAYMENT_ATTRIBUTION_AUDIT.proposed_payment_identifier = PRNT_PAYMENT_ATTRIBUTION_AUDIT.proposed_payment_identifier)   
       LEFT JOIN `fsa-{env}-cnsv`.`subsidiary_customer`
       ON (PAYMENT_ATTRIBUTION_AUDIT.subsidiary_customer_identifier = subsidiary_customer.subsidiary_customer_identifier)
       Left join `fsa-{env}-cnsv`.`fsa_county`
       On (subsidiary_customer.fsa_county_identifier =fsa_county.fsa_county_identifier)
             /*join to subsidary customer again to get parent values*/
       LEFT JOIN `fsa-{env}-cnsv`.`subsidiary_customer` parent_subsidiary_customer
       ON (PRNT_PAYMENT_ATTRIBUTION_AUDIT.subsidiary_customer_identifier = parent_subsidiary_customer.subsidiary_customer_identifier)
            /*join to fsa county again to get parent values*/
        left join `fsa-{env}-cnsv`.`fsa_county` parent_fsa_county
        on (parent_subsidiary_customer.fsa_county_identifier=parent_fsa_county.fsa_county_identifier)
         /*get paid customer information*/
       LEFT JOIN `fsa-{env}-cnsv`.`subsidiary_customer` paid_subsidiary_customer
       ON (PAYMENT_ATTRIBUTION_AUDIT.paid_subsidiary_customer_identifier = paid_subsidiary_customer.subsidiary_customer_identifier)
             /*get paid fsa county information*/
        left join `fsa-{env}-cnsv`.`fsa_county` paid_fsa_county
        on (paid_subsidiary_customer.fsa_county_identifier = paid_fsa_county.fsa_county_identifier)
       Left join `fsa-{env}-cnsv`.`PROPOSED_PAYMENT` 
        ON (PAYMENT_ATTRIBUTION_AUDIT.proposed_payment_identifier =proposed_payment.proposed_payment_identifier)
        /*join to Proposed Payment again to get parent values*/
             Left join `fsa-{env}-cnsv`.`PROPOSED_PAYMENT`  PRNT_PROPOSED_PAYMENT
            ON (PRNT_PAYMENT_ATTRIBUTION_AUDIT.proposed_payment_identifier =PRNT_PROPOSED_PAYMENT.proposed_payment_identifier)
WHERE PAYMENT_ATTRIBUTION_AUDIT.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND PAYMENT_ATTRIBUTION_AUDIT.OP <> 'D'
) NET_CHANGES
WHERE NET_CHANGES.row_num_part = 1

UNION

SELECT 
PAYMENT_ATTRIBUTION_AUDIT.payment_attribution_identifier PYMT_ATRB_ID,
PAYMENT_ATTRIBUTION_AUDIT.proposed_payment_identifier PRPS_PYMT_ID,
PAYMENT_ATTRIBUTION_AUDIT.accounting_program_code ACCT_PGM_CD,
PAYMENT_ATTRIBUTION_AUDIT.agi_eligible_indicator AGI_ELG_IND,
PAYMENT_ATTRIBUTION_AUDIT.agi_error_reference_code AGI_ERR_REF_CD,
PAYMENT_ATTRIBUTION_AUDIT.business_type_code BUS_TYPE_CD,
PAYMENT_ATTRIBUTION_AUDIT.cash_rent_cropland_factor CASH_RENT_CPLD_FCTR,
PAYMENT_ATTRIBUTION_AUDIT.combined_party_payment_share_percentage CMB_PTY_PYMT_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.member_contribution_eligible_indicator MBR_CTRB_ELG_IND,
PAYMENT_ATTRIBUTION_AUDIT.member_contribution_share_percentage MBR_CTRB_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.member_share_percentage MBR_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.net_payment_amount NET_PYMT_AMT,
PAYMENT_ATTRIBUTION_AUDIT.ownership_hierarchy_level_share_percentage OWNSHP_HRCH_LVL_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.paid_party_share_percentage PAID_PTY_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.paid_subsidiary_customer_identifier PAID_SBSD_CUST_ID,
PAYMENT_ATTRIBUTION_AUDIT.program_ineligibility_reason_type_code PGM_INELG_RSN_TYPE_CD,
PAYMENT_ATTRIBUTION_AUDIT.permitted_entity_eligible_indicator PMIT_ENTY_ELG_IND,
PAYMENT_ATTRIBUTION_AUDIT.parent_payment_attribution_identifier PRNT_PYMT_ATRB_ID,
PAYMENT_ATTRIBUTION_AUDIT.subsidiary_period_start_year  SBSD_PRD_STRT_YR,
PAYMENT_ATTRIBUTION_AUDIT.payment_attribution_amount PYMT_ATRB_AMT,
PAYMENT_ATTRIBUTION_AUDIT.payment_attribution_business_category_code PYMT_ATRB_BUS_CAT_CD,
PAYMENT_ATTRIBUTION_AUDIT.payment_attribution_share_percentage PYMT_ATRB_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.payment_limitation_year PYMT_LMT_YR,
PAYMENT_ATTRIBUTION_AUDIT.limited_payment_program_identifier PYMT_PGM_ID,
PAYMENT_ATTRIBUTION_AUDIT.payment_party_relationship_code PYMT_PTY_RLT_CD,
PAYMENT_ATTRIBUTION_AUDIT.record_reference_primary_identification RCD_REF_PRIM_ID,
PAYMENT_ATTRIBUTION_AUDIT.record_reference_primary_identification_type_code   RCD_REF_PRIM_ID_TYPE_CD,
PAYMENT_ATTRIBUTION_AUDIT.record_reference_secondary_identification RCD_REF_SCND_ID,
PAYMENT_ATTRIBUTION_AUDIT.record_reference_secondary_identification_type_code RCD_REF_SCND_ID_TYPE_CD,
PAYMENT_ATTRIBUTION_AUDIT.subsidiary_customer_identifier SBSD_CUST_ID,
PAYMENT_ATTRIBUTION_AUDIT.substantive_change_eligible_indicator SBST_CHG_ELG_IND,
PAYMENT_ATTRIBUTION_AUDIT.tax_identification_type_code  TAX_ID_TYPE_CD,
PAYMENT_ATTRIBUTION_AUDIT.total_net_payment_amount TOT_NET_PYMT_AMT,
PAYMENT_ATTRIBUTION_AUDIT.total_payment_attribution_amount TOT_PYMT_ATRB_AMT,
PAYMENT_ATTRIBUTION_AUDIT.creation_date CRE_DT,
PAYMENT_ATTRIBUTION_AUDIT.creation_user_name CRE_USER_NM,
PAYMENT_ATTRIBUTION_AUDIT.data_status_code DATA_STAT_CD,
PAYMENT_ATTRIBUTION_AUDIT.foreign_person_eligible_indicator FGN_PRSN_ELG_IND,
PAYMENT_ATTRIBUTION_AUDIT.last_change_date LAST_CHG_DT,
PAYMENT_ATTRIBUTION_AUDIT.last_change_user_name LAST_CHG_USER_NM,
PAYMENT_ATTRIBUTION_AUDIT.data_action_code DATA_ACTN_CD,
PAYMENT_ATTRIBUTION_AUDIT.data_action_date DATA_ACTN_DT,
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
PRNT_proposed_payment.subsidiary_period_start_year  PRNT_SBSD_PRD_STRT_YR,
subsidiary_customer.OP,
2 AS TBL_PRIORITY
from 
  ( 
    Select SBSD_PRE.* from (
Select * FROM `fsa-{env}-cnsv`.`subsidiary_customer` where  subsidiary_customer.OP ='UN' And cast('{ETL_START_DATE}' as date)  <> cast(current_date as date)
     UNION
     /*same day scenario update happens*/
    Select * FROM  `fsa-{env}-cnsv`.`subsidiary_customer`  where  subsidiary_customer.OP ='UN'
       And (COALESCE(CAST(CRE_DT AS DATE), DATE '1900-01-01') <= CAST('{ETL_START_DATE}'  AS DATE))
     )  SBSD_PRE
  JOIN
   ( Select * from `fsa-{env}-cnsv`.`subsidiary_customer`   
   ) SBSD_CUST_MAIN
ON  ( SBSD_PRE.subsidiary_customer_identifier = SBSD_CUST_MAIN.subsidiary_customer_identifier 
       And ( SBSD_PRE.fsa_county_identifier <> SBSD_CUST_MAIN.fsa_county_identifier Or SBSD_PRE.core_customer_identifier <> SBSD_CUST_MAIN.core_customer_identifier )
     )
     UNION
   Select * FROM `fsa-{env}-cnsv`.`subsidiary_customer`  where   subsidiary_customer.OP ='I'
 ) subsidiary_customer
  JOIN  
  (
   Select * From  `fsa-{env}-cnsv`.`PAYMENT_ATTRIBUTION_AUDIT`  where cast(Date,PAYMENT_ATTRIBUTION_AUDIT.data_action_date) <='{ETL_START_DATE}'
  ) PAYMENT_ATTRIBUTION_AUDIT
    ON (PAYMENT_ATTRIBUTION_AUDIT.subsidiary_customer_identifier = subsidiary_customer.subsidiary_customer_identifier)
    LEFT JOIN 
     (
       Select * From  `fsa-{env}-cnsv`.`PAYMENT_ATTRIBUTION_AUDIT`  where cast(Date,PAYMENT_ATTRIBUTION_AUDIT.data_action_date) <='{ETL_START_DATE}'
    ) PRNT_PAYMENT_ATTRIBUTION_AUDIT 
       on (PAYMENT_ATTRIBUTION_AUDIT.parent_payment_attribution_identifier = PRNT_PAYMENT_ATTRIBUTION_AUDIT.payment_attribution_identifier)
       And (PAYMENT_ATTRIBUTION_AUDIT.proposed_payment_identifier = PRNT_PAYMENT_ATTRIBUTION_AUDIT.proposed_payment_identifier)   
    Left join `fsa-{env}-cnsv`.`fsa_county`
       On (subsidiary_customer.fsa_county_identifier =fsa_county.fsa_county_identifier)
             /*join to subsidary customer again to get parent values*/
    LEFT JOIN `fsa-{env}-cnsv`.`subsidiary_customer` parent_subsidiary_customer
     ON (PRNT_PAYMENT_ATTRIBUTION_AUDIT.subsidiary_customer_identifier = parent_subsidiary_customer.subsidiary_customer_identifier)
            /*join to fsa county again to get parent values*/
        left join `fsa-{env}-cnsv`.`fsa_county` parent_fsa_county
        on (parent_subsidiary_customer.fsa_county_identifier=parent_fsa_county.fsa_county_identifier)
   /*get paid customer information*/
       LEFT JOIN `fsa-{env}-cnsv`.`subsidiary_customer` paid_subsidiary_customer
       ON (PAYMENT_ATTRIBUTION_AUDIT.paid_subsidiary_customer_identifier = paid_subsidiary_customer.subsidiary_customer_identifier)
             /*get paid fsa county information*/
        left join `fsa-{env}-cnsv`.`fsa_county` paid_fsa_county
        on (paid_subsidiary_customer.fsa_county_identifier = paid_fsa_county.fsa_county_identifier)
       Left join `fsa-{env}-cnsv`.`PROPOSED_PAYMENT` 
        ON (PAYMENT_ATTRIBUTION_AUDIT.proposed_payment_identifier =proposed_payment.proposed_payment_identifier)
        /*join to Proposed Payment again to get parent values*/
             Left join `fsa-{env}-cnsv`.`PROPOSED_PAYMENT`  PRNT_PROPOSED_PAYMENT
            ON (PRNT_PAYMENT_ATTRIBUTION_AUDIT.proposed_payment_identifier =PRNT_PROPOSED_PAYMENT.proposed_payment_identifier)
WHERE subsidiary_customer.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND subsidiary_customer.OP <> 'D'

UNION
SELECT 
PAYMENT_ATTRIBUTION_AUDIT.payment_attribution_identifier PYMT_ATRB_ID,
PAYMENT_ATTRIBUTION_AUDIT.proposed_payment_identifier PRPS_PYMT_ID,
PAYMENT_ATTRIBUTION_AUDIT.accounting_program_code ACCT_PGM_CD,
PAYMENT_ATTRIBUTION_AUDIT.agi_eligible_indicator AGI_ELG_IND,
PAYMENT_ATTRIBUTION_AUDIT.agi_error_reference_code AGI_ERR_REF_CD,
PAYMENT_ATTRIBUTION_AUDIT.business_type_code BUS_TYPE_CD,
PAYMENT_ATTRIBUTION_AUDIT.cash_rent_cropland_factor CASH_RENT_CPLD_FCTR,
PAYMENT_ATTRIBUTION_AUDIT.combined_party_payment_share_percentage CMB_PTY_PYMT_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.member_contribution_eligible_indicator MBR_CTRB_ELG_IND,
PAYMENT_ATTRIBUTION_AUDIT.member_contribution_share_percentage MBR_CTRB_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.member_share_percentage MBR_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.net_payment_amount NET_PYMT_AMT,
PAYMENT_ATTRIBUTION_AUDIT.ownership_hierarchy_level_share_percentage OWNSHP_HRCH_LVL_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.paid_party_share_percentage PAID_PTY_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.paid_subsidiary_customer_identifier PAID_SBSD_CUST_ID,
PAYMENT_ATTRIBUTION_AUDIT.program_ineligibility_reason_type_code PGM_INELG_RSN_TYPE_CD,
PAYMENT_ATTRIBUTION_AUDIT.permitted_entity_eligible_indicator PMIT_ENTY_ELG_IND,
PAYMENT_ATTRIBUTION_AUDIT.parent_payment_attribution_identifier PRNT_PYMT_ATRB_ID,
PAYMENT_ATTRIBUTION_AUDIT.subsidiary_period_start_year  SBSD_PRD_STRT_YR,
PAYMENT_ATTRIBUTION_AUDIT.payment_attribution_amount PYMT_ATRB_AMT,
PAYMENT_ATTRIBUTION_AUDIT.payment_attribution_business_category_code PYMT_ATRB_BUS_CAT_CD,
PAYMENT_ATTRIBUTION_AUDIT.payment_attribution_share_percentage PYMT_ATRB_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.payment_limitation_year PYMT_LMT_YR,
PAYMENT_ATTRIBUTION_AUDIT.limited_payment_program_identifier PYMT_PGM_ID,
PAYMENT_ATTRIBUTION_AUDIT.payment_party_relationship_code PYMT_PTY_RLT_CD,
PAYMENT_ATTRIBUTION_AUDIT.record_reference_primary_identification RCD_REF_PRIM_ID,
PAYMENT_ATTRIBUTION_AUDIT.record_reference_primary_identification_type_code   RCD_REF_PRIM_ID_TYPE_CD,
PAYMENT_ATTRIBUTION_AUDIT.record_reference_secondary_identification RCD_REF_SCND_ID,
PAYMENT_ATTRIBUTION_AUDIT.record_reference_secondary_identification_type_code RCD_REF_SCND_ID_TYPE_CD,
PAYMENT_ATTRIBUTION_AUDIT.subsidiary_customer_identifier SBSD_CUST_ID,
PAYMENT_ATTRIBUTION_AUDIT.substantive_change_eligible_indicator SBST_CHG_ELG_IND,
PAYMENT_ATTRIBUTION_AUDIT.tax_identification_type_code  TAX_ID_TYPE_CD,
PAYMENT_ATTRIBUTION_AUDIT.total_net_payment_amount TOT_NET_PYMT_AMT,
PAYMENT_ATTRIBUTION_AUDIT.total_payment_attribution_amount TOT_PYMT_ATRB_AMT,
PAYMENT_ATTRIBUTION_AUDIT.creation_date CRE_DT,
PAYMENT_ATTRIBUTION_AUDIT.creation_user_name CRE_USER_NM,
PAYMENT_ATTRIBUTION_AUDIT.data_status_code DATA_STAT_CD,
PAYMENT_ATTRIBUTION_AUDIT.foreign_person_eligible_indicator FGN_PRSN_ELG_IND,
PAYMENT_ATTRIBUTION_AUDIT.last_change_date LAST_CHG_DT,
PAYMENT_ATTRIBUTION_AUDIT.last_change_user_name LAST_CHG_USER_NM,
PAYMENT_ATTRIBUTION_AUDIT.data_action_code DATA_ACTN_CD,
PAYMENT_ATTRIBUTION_AUDIT.data_action_date DATA_ACTN_DT,
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
PRNT_proposed_payment.subsidiary_period_start_year  PRNT_SBSD_PRD_STRT_YR,
PAYMENT_ATTRIBUTION_AUDIT.OP,
3 AS TBL_PRIORITY
from  (  SELECT 
    fsa_county_identifier,
    state_county_fsa_code,
    last_change_date, 
    last_change_user_name,
    PAYMENT_ATTRIBUTION_AUDIT.OP 
    FROM (
    SELECT *,ROW_NUMBER() OVER (PARTITION BY fsa_county_identifier ORDER BY LAST_CHG_DT DESC
            ) as rnum 
            FROM `fsa-{env}-cnsv`.`fsa_county`) as SubQry
    WHERE SubQry.rnum=1
) fsa_county
     /* Need to inner join untill we reach Primary Table */
     join `fsa-{env}-cnsv`.`subsidiary_customer`
    On (subsidiary_customer.fsa_county_identifier = fsa_county.fsa_county_identifier)
    JOIN    
     (
      Select * From  `fsa-{env}-cnsv`.`PAYMENT_ATTRIBUTION_AUDIT`  where cast(Date,PAYMENT_ATTRIBUTION_AUDIT.data_action_date) <='{ETL_END_DATE}'
    ) PAYMENT_ATTRIBUTION_AUDIT
      ON (PAYMENT_ATTRIBUTION_AUDIT.subsidiary_customer_identifier = subsidiary_customer.subsidiary_customer_identifier)
       LEFT JOIN 
         (
           Select * From  `fsa-{env}-cnsv`.`PAYMENT_ATTRIBUTION_AUDIT`  where cast(Date,PAYMENT_ATTRIBUTION_AUDIT.data_action_date) <='{ETL_END_DATE}'
        )  PRNT_PAYMENT_ATTRIBUTION_AUDIT
       on (PAYMENT_ATTRIBUTION_AUDIT.parent_payment_attribution_identifier = PRNT_PAYMENT_ATTRIBUTION_AUDIT.payment_attribution_identifier)
       And (PAYMENT_ATTRIBUTION_AUDIT.proposed_payment_identifier = PRNT_PAYMENT_ATTRIBUTION_AUDIT.proposed_payment_identifier)   
                /*join to subsidary customer again to get parent values*/
       LEFT JOIN `fsa-{env}-cnsv`.`subsidiary_customer` parent_subsidiary_customer
       ON (PRNT_PAYMENT_ATTRIBUTION_AUDIT.subsidiary_customer_identifier = parent_subsidiary_customer.subsidiary_customer_identifier)
            /*join to fsa county again to get parent values*/
        left join `fsa-{env}-cnsv`.`fsa_county` parent_fsa_county
        on (parent_subsidiary_customer.fsa_county_identifier=parent_fsa_county.fsa_county_identifier)
   /*get paid customer information*/
       LEFT JOIN `fsa-{env}-cnsv`.`subsidiary_customer` paid_subsidiary_customer
       ON (PAYMENT_ATTRIBUTION_AUDIT.paid_subsidiary_customer_identifier = paid_subsidiary_customer.subsidiary_customer_identifier)
             /*get paid fsa county information*/
        left join `fsa-{env}-cnsv`.`fsa_county` paid_fsa_county
        on (paid_subsidiary_customer.fsa_county_identifier = paid_fsa_county.fsa_county_identifier)
       Left join `fsa-{env}-cnsv`.`PROPOSED_PAYMENT` 
        ON (PAYMENT_ATTRIBUTION_AUDIT.proposed_payment_identifier =proposed_payment.proposed_payment_identifier)
        /*join to Proposed Payment again to get parent values*/
             Left join `fsa-{env}-cnsv`.`PROPOSED_PAYMENT`  PRNT_PROPOSED_PAYMENT
            ON (PRNT_PAYMENT_ATTRIBUTION_AUDIT.proposed_payment_identifier =PRNT_PROPOSED_PAYMENT.proposed_payment_identifier)
WHERE PRNT_PROPOSED_PAYMENT.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND PRNT_PROPOSED_PAYMENT.OP <> 'D'

UNION
Select
PAYMENT_ATTRIBUTION_AUDIT.payment_attribution_identifier PYMT_ATRB_ID,
PAYMENT_ATTRIBUTION_AUDIT.proposed_payment_identifier PRPS_PYMT_ID,
PAYMENT_ATTRIBUTION_AUDIT.accounting_program_code ACCT_PGM_CD,
PAYMENT_ATTRIBUTION_AUDIT.agi_eligible_indicator AGI_ELG_IND,
PAYMENT_ATTRIBUTION_AUDIT.agi_error_reference_code AGI_ERR_REF_CD,
PAYMENT_ATTRIBUTION_AUDIT.business_type_code BUS_TYPE_CD,
PAYMENT_ATTRIBUTION_AUDIT.cash_rent_cropland_factor CASH_RENT_CPLD_FCTR,
PAYMENT_ATTRIBUTION_AUDIT.combined_party_payment_share_percentage CMB_PTY_PYMT_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.member_contribution_eligible_indicator MBR_CTRB_ELG_IND,
PAYMENT_ATTRIBUTION_AUDIT.member_contribution_share_percentage MBR_CTRB_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.member_share_percentage MBR_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.net_payment_amount NET_PYMT_AMT,
PAYMENT_ATTRIBUTION_AUDIT.ownership_hierarchy_level_share_percentage OWNSHP_HRCH_LVL_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.paid_party_share_percentage PAID_PTY_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.paid_subsidiary_customer_identifier PAID_SBSD_CUST_ID,
PAYMENT_ATTRIBUTION_AUDIT.program_ineligibility_reason_type_code PGM_INELG_RSN_TYPE_CD,
PAYMENT_ATTRIBUTION_AUDIT.permitted_entity_eligible_indicator PMIT_ENTY_ELG_IND,
PAYMENT_ATTRIBUTION_AUDIT.parent_payment_attribution_identifier PRNT_PYMT_ATRB_ID,
PAYMENT_ATTRIBUTION_AUDIT.subsidiary_period_start_year  SBSD_PRD_STRT_YR,
PAYMENT_ATTRIBUTION_AUDIT.payment_attribution_amount PYMT_ATRB_AMT,
PAYMENT_ATTRIBUTION_AUDIT.payment_attribution_business_category_code PYMT_ATRB_BUS_CAT_CD,
PAYMENT_ATTRIBUTION_AUDIT.payment_attribution_share_percentage PYMT_ATRB_SHR_PCT,
PAYMENT_ATTRIBUTION_AUDIT.payment_limitation_year PYMT_LMT_YR,
PAYMENT_ATTRIBUTION_AUDIT.limited_payment_program_identifier PYMT_PGM_ID,
PAYMENT_ATTRIBUTION_AUDIT.payment_party_relationship_code PYMT_PTY_RLT_CD,
PAYMENT_ATTRIBUTION_AUDIT.record_reference_primary_identification RCD_REF_PRIM_ID,
PAYMENT_ATTRIBUTION_AUDIT.record_reference_primary_identification_type_code   RCD_REF_PRIM_ID_TYPE_CD,
PAYMENT_ATTRIBUTION_AUDIT.record_reference_secondary_identification RCD_REF_SCND_ID,
PAYMENT_ATTRIBUTION_AUDIT.record_reference_secondary_identification_type_code RCD_REF_SCND_ID_TYPE_CD,
PAYMENT_ATTRIBUTION_AUDIT.subsidiary_customer_identifier SBSD_CUST_ID,
PAYMENT_ATTRIBUTION_AUDIT.substantive_change_eligible_indicator SBST_CHG_ELG_IND,
PAYMENT_ATTRIBUTION_AUDIT.tax_identification_type_code  TAX_ID_TYPE_CD,
PAYMENT_ATTRIBUTION_AUDIT.total_net_payment_amount TOT_NET_PYMT_AMT,
PAYMENT_ATTRIBUTION_AUDIT.total_payment_attribution_amount TOT_PYMT_ATRB_AMT,
PAYMENT_ATTRIBUTION_AUDIT.creation_date CRE_DT,
PAYMENT_ATTRIBUTION_AUDIT.creation_user_name CRE_USER_NM,
PAYMENT_ATTRIBUTION_AUDIT.data_status_code DATA_STAT_CD,
PAYMENT_ATTRIBUTION_AUDIT.foreign_person_eligible_indicator FGN_PRSN_ELG_IND,
PAYMENT_ATTRIBUTION_AUDIT.last_change_date LAST_CHG_DT,
PAYMENT_ATTRIBUTION_AUDIT.last_change_user_name LAST_CHG_USER_NM,
PAYMENT_ATTRIBUTION_AUDIT.data_action_code DATA_ACTN_CD,
PAYMENT_ATTRIBUTION_AUDIT.data_action_date DATA_ACTN_DT,
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
PRNT_proposed_payment.subsidiary_period_start_year  PRNT_SBSD_PRD_STRT_YR,
PAYMENT_ATTRIBUTION_AUDIT.OP,
4 AS TBL_PRIORITY
from `fsa-{env}-cnsv-cdc`.`PROPOSED_PAYMENT`
 JOIN    
  (
   Select * From  `fsa-{env}-cnsv`.`PAYMENT_ATTRIBUTION_AUDIT`  where cast(Date,PAYMENT_ATTRIBUTION_AUDIT.data_action_date) <='{ETL_END_DATE}'
  ) PAYMENT_ATTRIBUTION_AUDIT  
   ON (PAYMENT_ATTRIBUTION_AUDIT.proposed_payment_identifier =proposed_payment.proposed_payment_identifier)
        /*join to PAYMENT_ATTRIBUTION_AUDIT again to get parent values*/
       LEFT JOIN 
         (
       Select * From  `fsa-{env}-cnsv`.`PAYMENT_ATTRIBUTION_AUDIT`  where cast(Date,PAYMENT_ATTRIBUTION_AUDIT.data_action_date) <='{ETL_END_DATE}'
    ) PRNT_PAYMENT_ATTRIBUTION_AUDIT
       on (PAYMENT_ATTRIBUTION_AUDIT.parent_payment_attribution_identifier = PRNT_PAYMENT_ATTRIBUTION_AUDIT.payment_attribution_identifier)
       And (PAYMENT_ATTRIBUTION_AUDIT.proposed_payment_identifier = PRNT_PAYMENT_ATTRIBUTION_AUDIT.proposed_payment_identifier)   
       Left join `fsa-{env}-cnsv`.`subsidiary_customer`
       ON (PAYMENT_ATTRIBUTION_AUDIT.subsidiary_customer_identifier = subsidiary_customer.subsidiary_customer_identifier)
       Left join    `fsa-{env}-cnsv`.`fsa_county`
       On (subsidiary_customer.fsa_county_identifier =fsa_county.fsa_county_identifier)
                     /*join to subsidary customer again to get parent values*/
       LEFT JOIN `fsa-{env}-cnsv`.`subsidiary_customer` parent_subsidiary_customer
       ON (PRNT_PAYMENT_ATTRIBUTION_AUDIT.subsidiary_customer_identifier = parent_subsidiary_customer.subsidiary_customer_identifier)
            /*join to fsa county again to get parent values*/
        left join `fsa-{env}-cnsv`.`fsa_county` parent_fsa_county
        on (parent_subsidiary_customer.fsa_county_identifier=parent_fsa_county.fsa_county_identifier)
       /*get paid customer information*/
       LEFT JOIN `fsa-{env}-cnsv`.`subsidiary_customer` paid_subsidiary_customer
       ON (PAYMENT_ATTRIBUTION_AUDIT.paid_subsidiary_customer_identifier = paid_subsidiary_customer.subsidiary_customer_identifier)
             /*get paid fsa county information*/
        left join `fsa-{env}-cnsv`.`fsa_county` paid_fsa_county
        on (paid_subsidiary_customer.fsa_county_identifier = paid_fsa_county.fsa_county_identifier)
                  /*join to Proposed Payment again to get parent values*/
          Left join `fsa-{env}-cnsv`.`PROPOSED_PAYMENT`  PRNT_PROPOSED_PAYMENT
       ON (PRNT_PAYMENT_ATTRIBUTION_AUDIT.proposed_payment_identifier =PRNT_PROPOSED_PAYMENT.proposed_payment_identifier)
WHERE PROPOSED_PAYMENT.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
AND PROPOSED_PAYMENT.OP <> 'D'

UNION

Select
DELETE_NET.PYMT_ATRB_ID,
DELETE_NET.PRPS_PYMT_ID,
DELETE_NET.ACCT_PGM_CD,
DELETE_NET.AGI_ELG_IND,
DELETE_NET.AGI_ERR_REF_CD,
DELETE_NET.BUS_TYPE_CD,
DELETE_NET.CASH_RENT_CPLD_FCTR,
DELETE_NET.CMB_PTY_PYMT_SHR_PCT,
DELETE_NET.MBR_CTRB_ELG_IND,
DELETE_NET.MBR_CTRB_SHR_PCT,
DELETE_NET.MBR_SHR_PCT,
DELETE_NET.NET_PYMT_AMT,
DELETE_NET.OWNSHP_HRCH_LVL_SHR_PCT,
DELETE_NET.PAID_PTY_SHR_PCT,
DELETE_NET.PAID_SBSD_CUST_ID,
DELETE_NET.PGM_INELG_RSN_TYPE_CD,
DELETE_NET.PMIT_ENTY_ELG_IND,
DELETE_NET.PRNT_PYMT_ATRB_ID,
DELETE_NET.SBSD_PRD_STRT_YR,
DELETE_NET.PYMT_ATRB_AMT,
DELETE_NET.PYMT_ATRB_BUS_CAT_CD,
DELETE_NET.PYMT_ATRB_SHR_PCT,
DELETE_NET.PYMT_LMT_YR,
DELETE_NET.PYMT_PGM_ID,
DELETE_NET.PYMT_PTY_RLT_CD,
DELETE_NET.RCD_REF_PRIM_ID,
DELETE_NET.RCD_REF_PRIM_ID_TYPE_CD,
DELETE_NET.RCD_REF_SCND_ID,
DELETE_NET.RCD_REF_SCND_ID_TYPE_CD,
DELETE_NET.SBSD_CUST_ID,
DELETE_NET.SBST_CHG_ELG_IND,
DELETE_NET.TAX_ID_TYPE_CD,
DELETE_NET.TOT_NET_PYMT_AMT,
DELETE_NET.TOT_PYMT_ATRB_AMT,
DELETE_NET.CRE_DT,
DELETE_NET.CRE_USER_NM,
DELETE_NET.DATA_STAT_CD)))
