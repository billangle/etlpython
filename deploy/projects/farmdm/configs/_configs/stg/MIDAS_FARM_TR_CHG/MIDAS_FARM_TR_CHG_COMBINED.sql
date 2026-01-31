INSERT INTO sql_farm_rcd_stg.midas_farm_tr_chg
(midas_farm_tr_chg_id, farm_chg_type_id, prnt_st_fsa_cd, 
prnt_cnty_fsa_cd, prnt_farm_nbr, prnt_tr_nbr, rslt_st_fsa_cd, 
rslt_cnty_fsa_cd, rslt_farm_nbr, rslt_tr_nbr, data_stat_cd, 
cre_dt, cre_user_nm, last_chg_dt, last_chg_user_nm, hash_dif, 
cdc_oper_cd, load_dt, data_src_nm, cdc_dt, tm_prd_id, 
rcon_seq_nbr, rcon_init_dt, rcon_apvl_dt, tm_prd_nm)

select distinct m.midas_farm_tract_change_identifier As MIDAS_FARM_TR_CHG_ID,
m.farm_change_type_identifier As FARM_CHG_TYPE_ID,
LTrim(RTrim(m.parent_state_fsa_code)) As PRNT_ST_FSA_CD,
LTrim(RTrim(m.parent_county_fsa_code)) As PRNT_CNTY_FSA_CD,
LTrim(RTrim(m.parent_farm_number)) As PRNT_FARM_NBR,
LTrim(RTrim(m.parent_tract_number)) As PRNT_TR_NBR,
LTrim(RTrim(m.resulting_state_fsa_code)) As RSLT_ST_FSA_CD,
LTrim(RTrim(m.resulting_county_fsa_code)) As RSLT_CNTY_FSA_CD,
LTrim(RTrim(m.resulting_farm_number)) As RSLT_FARM_NBR,
LTrim(RTrim(m.resulting_tract_number)) As RSLT_TR_NBR,
LTrim(RTrim(m.data_status_code)) As DATA_STAT_CD,
m.creation_date As CRE_DT,
LTrim(RTrim(m.creation_user_name)) As CRE_USER_NM,
m.last_change_date As LAST_CHG_DT,
LTrim(RTrim(m.last_change_user_name)) As LAST_CHG_USER_NM,
''  as hash_dif,
m.cdc_oper_cd,
CAST(current_date as date) as load_dt,
'SAP/CRM' as data_src_nm,
CAST(current_date-1 as date) as CDC_DT,
m.time_period_identifier AS TM_PRD_ID,
LTrim(RTrim(m.reconstitution_sequence_number)) AS RCON_SEQ_NBR,
m.reconstitution_initiation_date AS RCON_INIT_DT,
m.reconstitution_approval_date AS RCON_APVL_DT,
LTrim(RTrim(t.time_period_name)) AS TM_PRD_NM
from farm_records_reporting.midas_farm_tract_change m 
left join farm_records_reporting.time_period t on m.time_period_identifier = t.time_period_identifier
where m.cdc_dt >= current_date - 1