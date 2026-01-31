SELECT crop_tract_year_dcp.crop_tract_year_dcp_identifier As CROP_TR_YR_DCP_ID,crop_tract_year_dcp.tract_year_dcp_identifier As TR_YR_DCP_ID,
crop_tract_year_dcp.crop_identifier As CROP_ID,crop_tract_year_dcp.crp_reduction_acreage As CRP_RDN_ACRG,
crop_tract_year_dcp.crp_released_acreage As CRP_REL_ACRG,crop_tract_year_dcp.dcp_crop_base_acreage As DCP_CROP_BASE_ACRG,
crop_tract_year_dcp.fav_reduction_acreage As FAV_RDN_ACRG,crop_tract_year_dcp.ccp_payment_yield As CCP_PYMT_YLD,
crop_tract_year_dcp.crp_payment_yield As CRP_PYMT_YLD,crop_tract_year_dcp.direct_payment_yield As DIR_PYMT_YLD,
crop_tract_year_dcp.fav_direct_payment_yield As FAV_DIR_PYMT_YLD,crop_tract_year_dcp.fav_ccp_payment_yield As FAV_CCP_PYMT_YLD,
LTrim(RTrim(crop.program_abbreviation) ) As PGM_ABR,LTrim(RTrim(crop.fsa_crop_code) ) As FSA_CROP_CD,LTrim(RTrim(crop.fsa_crop_type_code) ) As FSA_CROP_TYPE_CD,
LTrim(RTrim(farm.farm_number) ) As FARM_NBR,LTrim(RTrim(tract.tract_number) ) As TR_NBR,LTrim(RTrim(county_office_control.state_fsa_code) ) As ST_FSA_CD,
LTrim(RTrim(county_office_control.county_fsa_code) ) As CNTY_FSA_CD,time_period.time_period_name As PGM_YR,LTrim(RTrim(crop_tract_year_dcp.data_status_code) ) As DATA_STAT_CD,
crop_tract_year_dcp.creation_date As CRE_DT,crop_tract_year_dcp.last_change_date As LAST_CHG_DT,LTrim(RTrim(crop_tract_year_dcp.last_change_user_name) ) As LAST_CHG_USER_NM
FROM farm_records_reporting.crop_tract_year_dcp
LEFT JOIN farm_records_reporting.tract_year_dcp ON (crop_tract_year_dcp.tract_year_dcp_identifier = tract_year_dcp.tract_year_dcp_identifier) 
JOIN farm_records_reporting.tract_year ON (tract_year_dcp.tract_year_identifier =tract_year.tract_year_identifier) 
JOIN farm_records_reporting.farm_year ON (tract_year.farm_year_identifier =farm_year.farm_year_identifier ) 
JOIN farm_records_reporting.farm ON (farm_year.farm_identifier =farm.farm_identifier ) 
JOIN farm_records_reporting.county_office_control ON (farm.county_office_control_identifier=county_office_control.county_office_control_identifier) 
JOIN farm_records_reporting.crop ON (crop_tract_year_dcp.crop_identifier=crop.crop_identifier ) 
JOIN farm_records_reporting.time_period ON (farm_year.time_period_identifier =time_period.time_period_identifier) 
JOIN farm_records_reporting.tract ON (tract_year.tract_identifier=tract.tract_identifier)