INSERT INTO sql_farm_rcd_stg.rcon_crop
(rcon_crop_id, rcon_id, crop_id, rcon_div_mthd_cd, pgm_abr, fsa_crop_cd, 
fsa_crop_type_cd, st_fsa_cd, cnty_fsa_cd, data_stat_cd, cre_dt, 
last_chg_dt, last_chg_user_nm, hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt)

select distinct r.reconstitution_crop_identifier As RCON_CROP_ID,
r.reconstitution_identifier As RCON_ID,
r.crop_identifier As CROP_ID,
r.reconstitution_division_method_code As RCON_DIV_MTHD_CD,
LTrim(RTrim(cr.program_abbreviation)) As PGM_ABR,
LTrim(RTrim(cr.fsa_crop_code)) As FSA_CROP_CD,
LTrim(RTrim(cr.fsa_crop_type_code)) As FSA_CROP_TYPE_CD,
LTrim(RTrim(c.state_fsa_code)) As ST_FSA_CD,
LTrim(RTrim(c.county_fsa_code)) As CNTY_FSA_CD,
LTrim(RTrim(r.data_status_code)) As DATA_STAT_CD,
r.creation_date As CRE_DT,
r.last_change_date As LAST_CHG_DT,
LTrim(RTrim(r.last_change_user_name)) As LAST_CHG_USER_NM,
''  as hash_dif,
r.cdc_oper_cd AS CDC_OPER_CD,
CAST(current_date as date) as LOAD_DT,
'SAP/CRM' as data_src_nm,
CAST(current_date-1 as date) as CDC_DT
from farm_records_reporting.reconstitution_crop r 
left join farm_records_reporting.reconstitution r2 on r.reconstitution_identifier = r2.reconstitution_identifier
join farm_records_reporting.county_office_control c on r2.county_office_control_identifier = c.county_office_control_identifier
join farm_records_reporting.crop cr on r.crop_identifier = cr.crop_identifier
where r.cdc_dt >= current_date - 1