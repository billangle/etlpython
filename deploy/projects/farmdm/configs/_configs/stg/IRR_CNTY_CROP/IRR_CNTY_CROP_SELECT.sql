SELECT LTrim(RTrim(county_office_control.county_fsa_code) ) As CNTY_FSA_CD,
irrigation_county_crop.county_office_control_identifier As CNTY_OFC_CTL_ID,
irrigation_county_crop.creation_date As CRE_DT,
LTrim(RTrim(irrigation_county_crop.creation_user_name) ) As CRE_USER_NM,
irrigation_county_crop.crop_identifier As CROP_ID,
LTrim(RTrim(irrigation_county_crop.data_status_code) ) As DATA_STAT_CD,
LTrim(RTrim(crop.fsa_crop_code) ) As FSA_CROP_CD,
LTrim(RTrim(crop.fsa_crop_type_code) ) As FSA_CROP_TYPE_CD,
irrigation_county_crop.irrigation_county_crop_identifier As IRR_CNTY_CROP_ID,
irrigation_county_crop.last_change_date As LAST_CHG_DT,
LTrim(RTrim(irrigation_county_crop.last_change_user_name) ) As LAST_CHG_USER_NM,
LTrim(RTrim(crop.program_abbreviation) ) As PGM_ABR,
LTrim(RTrim(county_office_control.state_fsa_code) ) As ST_FSA_CD
FROM farm_records_reporting.irrigation_county_crop
LEFT JOIN farm_records_reporting.county_office_control ON irrigation_county_crop.county_office_control_identifier = county_office_control.county_office_control_identifier 
JOIN farm_records_reporting.crop ON irrigation_county_crop.crop_identifier = crop.crop_identifier