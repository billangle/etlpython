SELECT distinct t.tract_identifier As TR_ID,
t.county_office_control_identifier As CNTY_OFC_CTL_ID,
LTrim(RTrim(t.tract_number) ) As TR_NBR,
LTrim(RTrim(t.tract_description) ) As TR_DESC,
LTrim(RTrim(t.bia_range_unit_number)) As BIA_RNG_UNIT_NBR,
LTrim(RTrim(t.location_state_fsa_code) ) As LOC_ST_FSA_CD,
LTrim(RTrim(t.location_county_fsa_code) ) As LOC_CNTY_FSA_CD,
LTrim(RTrim(t.congressional_district_code) ) As CONG_DIST_CD,
t.wl_certification_completion_code As WL_CERT_CPLT_CD,
t.wl_certification_completion_year As WL_CERT_CPLT_YR,
LTrim(RTrim(c.state_fsa_code) ) As ST_FSA_CD,
LTrim(RTrim(c.county_fsa_code) ) As CNTY_FSA_CD,
LTrim(RTrim(t.data_status_code) ) As DATA_STAT_CD,
t.creation_date As CRE_DT,
t.last_change_date As LAST_CHG_DT,
LTrim(RTrim(t.last_change_user_name) ) As LAST_CHG_USER_NM,
''  as hash_dif,
t.CDC_OPER_CD AS CDC_OPER_CD,
CAST(current_date as date) as load_dt,
'SAP/CRM' as data_src_nm,
t.cdc_dt as CDC_DT
from farm_records_reporting.tract t
left join farm_records_reporting.county_office_control c on t.county_office_control_identifier = c.county_office_control_identifier
where t.cdc_dt between date '{ETL_START_DATE}' and date '{ETL_END_DATE}'
