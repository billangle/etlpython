SELECT distinct tract_year_dcp.tract_year_dcp_identifier As TR_YR_DCP_ID,
tract_year_dcp.tract_year_identifier As TR_YR_ID,
tract_year_dcp.dcp_double_crop_acreage As DCP_DBL_CROP_ACRG,
tract_year_dcp.dcp_cropland_acreage As DCP_CPLD_ACRG,
tract_year_dcp.dcp_after_reduction_acreage As DCP_AFT_RDN_ACRG,
LTrim(RTrim(tract_year_dcp.fav_wr_history_indicator) ) As FAV_WR_HIST_IND,
LTrim(RTrim(farm.farm_number) ) As FARM_NBR,
LTrim(RTrim(tract.tract_number) ) As TR_NBR,
LTrim(RTrim(county_office_control.state_fsa_code) ) As ST_FSA_CD,
LTrim(RTrim(county_office_control.county_fsa_code) ) As CNTY_FSA_CD,
CAST(time_period.time_period_name AS numeric(4)) as PGM_YR,
LTrim(RTrim(tract_year_dcp.data_status_code) ) As DATA_STAT_CD,
tract_year_dcp.creation_date As CRE_DT,
tract_year_dcp.last_change_date As LAST_CHG_DT,
LTrim(RTrim(tract_year_dcp.last_change_user_name) ) As LAST_CHG_USER_NM,
''  as HASH_DIF,
tract_year_dcp.cdc_oper_cd AS CDC_OPER_CD,
CAST(current_date as date) as LOAD_DT,
'SAP/CRM' as DATA_SRC_NM,
tract_year_dcp.cdc_dt as CDC_DT
from farm_records_reporting.tract_year_dcp
LEFT JOIN farm_records_reporting.tract_year ON ( tract_year_dcp.tract_year_identifier = tract_year.tract_year_identifier ) 
JOIN farm_records_reporting.tract ON ( tract_year.tract_identifier = tract.tract_identifier ) 
JOIN farm_records_reporting.farm_year ON (tract_year.farm_year_identifier = farm_year.farm_year_identifier ) 
JOIN farm_records_reporting.farm ON ( farm_year.farm_identifier = farm.farm_identifier )
JOIN farm_records_reporting.county_office_control ON ( farm.county_office_control_identifier = county_office_control.county_office_control_identifier)
JOIN farm_records_reporting.time_period ON (farm_year.time_period_identifier =time_period.time_period_identifier)
where tract_year_dcp.cdc_dt between date '{ETL_START_DATE}' and date '{ETL_END_DATE}'
