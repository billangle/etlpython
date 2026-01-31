INSERT INTO sql_farm_rcd_stg.tm_prd
(tm_prd_id, tm_prd_nm, tm_prd_strt_dt, tm_prd_end_dt, pgm_abr, 
data_stat_cd, cre_dt, last_chg_dt, last_chg_user_nm, 
hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt)

select distinct t.time_period_identifier As TM_PRD_ID,
LTrim(RTrim(t.time_period_name)) As TM_PRD_NM,
t.time_period_start_date As TM_PRD_STRT_DT,
t.time_period_end_date As TM_PRD_END_DT,
LTrim(RTrim(t.program_abbreviation)) As PGM_ABR,
LTrim(RTrim(t.data_status_code)) As DATA_STAT_CD,
t.creation_date As CRE_DT,
t.last_change_date As LAST_CHG_DT,
LTrim(RTrim(t.last_change_user_name)) As LAST_CHG_USER_NM,
''  as hash_dif,
t.cdc_oper_cd,
CAST(current_date as date) as LOAD_DT,
'SAP/CRM' as DATA_SRC_NM,
CAST(current_date-1 as date) as CDC_DT
from farm_records_reporting.time_period t 
where t.cdc_dt >= current_date - 1