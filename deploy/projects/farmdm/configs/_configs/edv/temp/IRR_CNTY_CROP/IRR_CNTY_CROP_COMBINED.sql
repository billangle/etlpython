INSERT INTO sql_farm_rcd_stg.irr_cnty_crop
(irr_cnty_crop_id, crop_id, cnty_ofc_ctl_id, st_fsa_cd, cnty_fsa_cd, 
pgm_abr, fsa_crop_cd, fsa_crop_type_cd, data_stat_cd, cre_dt, 
cre_user_nm, last_chg_dt, last_chg_user_nm, hash_dif, cdc_oper_cd, 
load_dt, data_src_nm, cdc_dt)

SELECT distinct i.irrigation_county_crop_identifier As IRR_CNTY_CROP_ID,
i.crop_identifier As CROP_ID,
i.county_office_control_identifier As CNTY_OFC_CTL_ID,
LTrim(RTrim(c.state_fsa_code)) As ST_FSA_CD,
LTrim(RTrim(c.county_fsa_code) ) As CNTY_FSA_CD,
LTrim(RTrim(cr.program_abbreviation)) As PGM_ABR,
LTrim(RTrim(cr.fsa_crop_code)) As FSA_CROP_CD,
LTrim(RTrim(cr.fsa_crop_type_code)) As FSA_CROP_TYPE_CD,
LTrim(RTrim(i.data_status_code)) As DATA_STAT_CD,
i.creation_date As CRE_DT,
LTrim(RTrim(i.creation_user_name)) As CRE_USER_NM,
i.last_change_date As LAST_CHG_DT,
LTrim(RTrim(i.last_change_user_name)) As LAST_CHG_USER_NM,
''  as hash_dif,
i.cdc_oper_cd AS CDC_OPER_CD,
CAST(current_date as date) as load_dt,
'SAP/CRM' as data_src_nm,
CAST(current_date-1 as date) as CDC_DT
from farm_records_reporting.irrigation_county_crop i 
LEFT JOIN farm_records_reporting.county_office_control c ON i.county_office_control_identifier = c.county_office_control_identifier
JOIN farm_records_reporting.crop cr ON i.crop_identifier = cr.crop_identifier
where i.cdc_dt >= current_date - 1