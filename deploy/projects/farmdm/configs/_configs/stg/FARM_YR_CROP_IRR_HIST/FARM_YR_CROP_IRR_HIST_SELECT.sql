SELECT LTrim(RTrim(phyloc.county_fsa_code) ) As CNTY_FSA_CD,
farm_year_crop_irrigation_history.creation_date As CRE_DT,
LTrim(RTrim(farm_year_crop_irrigation_history.creation_user_name) ) As CRE_USER_NM,
LTrim(RTrim(farm_year_crop_irrigation_history.data_status_code) ) As DATA_STAT_CD,
LTrim(RTrim(admnloc.county_fsa_code) ) As FARM_CNTY_FSA_CD,
LTrim(RTrim(farm.farm_number) ) As FARM_NBR,
LTrim(RTrim(admnloc.state_fsa_code) ) As FARM_ST_FSA_CD,
farm_year_crop_irrigation_history.farm_year_crop_irrigation_history_identifier As FARM_YR_CROP_IRR_HIST_ID,
farm_year_crop_irrigation_history.farm_year_identifier As FARM_YR_ID,LTrim(RTrim(crop.fsa_crop_code) ) As FSA_CROP_CD,
LTrim(RTrim(crop.fsa_crop_type_code) ) As FSA_CROP_TYPE_CD,farm_year_crop_irrigation_history.historical_irrigation_percentage As HIST_IRR_PCT,
farm_year_crop_irrigation_history.irrigation_county_crop_identifier As IRR_CNTY_CROP_ID,farm_year_crop_irrigation_history.last_change_date As LAST_CHG_DT,
LTrim(RTrim(farm_year_crop_irrigation_history.last_change_user_name) ) As LAST_CHG_USER_NM,LTrim(RTrim(crop.program_abbreviation) ) As PGM_ABR,
time_period.time_period_name As PGM_YR,
LTrim(RTrim(phyloc.state_fsa_code) ) As ST_FSA_CD
FROM farm_records_reporting.farm_year_crop_irrigation_history
LEFT JOIN farm_records_reporting.irrigation_county_crop ON (farm_year_crop_irrigation_history.irrigation_county_crop_identifier = irrigation_county_crop.irrigation_county_crop_identifier) 
LEFT JOIN farm_records_reporting.farm_year ON (farm_year_crop_irrigation_history.farm_year_identifier = farm_year.farm_year_identifier ) 
LEFT JOIN farm_records_reporting.farm ON (farm_year.farm_identifier = farm.farm_identifier ) 
LEFT JOIN farm_records_reporting.crop ON (irrigation_county_crop.crop_identifier = crop.crop_identifier ) 
LEFT JOIN farm_records_reporting.time_period ON (farm_year.time_period_identifier = time_period.time_period_identifier ) 
LEFT JOIN farm_records_reporting.county_office_control AS phyloc ON (irrigation_county_crop.county_office_control_identifier = phyloc.county_office_control_identifier)
LEFT JOIN farm_records_reporting.county_office_control AS admnloc ON (farm.county_office_control_identifier = admnloc.county_office_control_identifier)
