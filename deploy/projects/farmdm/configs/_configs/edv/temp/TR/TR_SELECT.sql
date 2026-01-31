SELECT LTrim(RTrim(t.bia_range_unit_number) ) As BIA_RNG_UNIT_NBR,
LTrim(RTrim(c.county_fsa_code) ) As CNTY_FSA_CD,
t.county_office_control_identifier As CNTY_OFC_CTL_ID,
LTrim(RTrim(t.congressional_district_code) ) As CONG_DIST_CD,
t.creation_date As CRE_DT,
LTrim(RTrim(t.data_status_code) ) As DATA_STAT_CD,
t.last_change_date As LAST_CHG_DT,
LTrim(RTrim(t.last_change_user_name) ) As LAST_CHG_USER_NM,
LTrim(RTrim(t.location_county_fsa_code) ) As LOC_CNTY_FSA_CD,
LTrim(RTrim(t.location_state_fsa_code) ) As LOC_ST_FSA_CD,
LTrim(RTrim(c.state_fsa_code) ) As ST_FSA_CD,
LTrim(RTrim(t.tract_description) ) As TR_DESC,
t.tract_identifier As TR_ID,
LTrim(RTrim(t.tract_number) ) As TR_NBR,
t.wl_certification_completion_code As WL_CERT_CPLT_CD,
t.wl_certification_completion_year As WL_CERT_CPLT_YR,
t.CDC_OPER_CD AS CDC_OPER_CD
from farm_records_reporting.tract t
left join farm_records_reporting.county_office_control c on t.county_office_control_identifier = c.county_office_control_identifier