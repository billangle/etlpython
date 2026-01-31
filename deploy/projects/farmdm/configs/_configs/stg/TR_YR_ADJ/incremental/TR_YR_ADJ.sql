select distinct t.tract_year_adjustment_identifier As TR_YR_ADJ_ID,
t.tract_year_identifier As TR_YR_ID,
t.tract_year_adjustment_type_code As TR_YR_ADJ_TYPE_CD,
t.tract_year_adjustment_reason_code As TR_YR_ADJ_RSN_CD,
t.after_adjustment_value As AFT_ADJ_VAL,
t.before_adjustment_value As BEF_ADJ_VAL,
LTrim(RTrim(f.farm_number) ) As FARM_NBR,
LTrim(RTrim(tr.tract_number) ) As TR_NBR,
LTrim(RTrim(c.state_fsa_code) ) As ST_FSA_CD,
LTrim(RTrim(c.county_fsa_code) ) As CNTY_FSA_CD,
CAST(t2.time_period_name AS numeric(4)) as PGM_YR,
LTrim(RTrim(t.data_status_code) ) As DATA_STAT_CD,
t.creation_date As CRE_DT,
t.last_change_date As LAST_CHG_DT,
LTrim(RTrim(t.last_change_user_name) ) As LAST_CHG_USER_NM,
''  as HASH_DIF,
t.cdc_oper_cd AS CDC_OPER_CD,
CAST(current_date as date) as LOAD_DT,
'SAP/CRM' as DATA_SRC_NM,
t.cdc_dt as CDC_DT,
t.parent_table_physical_name AS PRNT_TBL_PHY_NM
from farm_records_reporting.tract_year_adjustment t
left join farm_records_reporting.tract_year ty on t.tract_year_identifier = ty.tract_year_identifier
join farm_records_reporting.tract tr on ty.tract_identifier = tr.tract_identifier
join farm_records_reporting.farm_year fy on ty.farm_year_identifier = fy.farm_year_identifier
join farm_records_reporting.farm f on fy.farm_identifier = f.farm_identifier
join farm_records_reporting.county_office_control c on f.county_office_control_identifier = c.county_office_control_identifier
join farm_records_reporting.time_period t2 on fy.time_period_identifier = t2.time_period_identifier
where t.cdc_dt between date '{ETL_START_DATE}' and date '{ETL_END_DATE}'
