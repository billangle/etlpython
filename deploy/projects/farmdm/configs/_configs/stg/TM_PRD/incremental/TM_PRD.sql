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
t.cdc_dt as CDC_DT
from farm_records_reporting.time_period t 
where t.cdc_dt between date '{ETL_START_DATE}' and date '{ETL_END_DATE}'