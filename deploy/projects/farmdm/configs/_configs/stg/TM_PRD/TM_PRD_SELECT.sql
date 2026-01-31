SELECT creation_date As CRE_DT,
LTrim(RTrim(data_status_code) ) As DATA_STAT_CD,
last_change_date As LAST_CHG_DT,
LTrim(RTrim(last_change_user_name) ) As LAST_CHG_USER_NM,
LTrim(RTrim(program_abbreviation) ) As PGM_ABR,
time_period_end_date As TM_PRD_END_DT,
time_period_identifier As TM_PRD_ID,
LTrim(RTrim(time_period_name) ) As TM_PRD_NM,
time_period_start_date As TM_PRD_STRT_DT
FROM farm_records_reporting.time_period