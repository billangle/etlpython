INSERT INTO sql_farm_rcd_stg.tr_yr
(tr_yr_id, tr_id, farm_yr_id, fmld_acrg, cpld_acrg, crp_acrg, mpl_acrg, wbp_acrg, wrp_tr_acrg, 
grp_cpld_acrg, st_cnsv_acrg, ot_cnsv_acrg, sugarcane_acrg, nap_crop_acrg, ntv_sod_brk_out_acrg,
 hel_tr_cd, wl_pres_cd, farm_nbr, tr_nbr, st_fsa_cd, cnty_fsa_cd, pgm_yr, data_stat_cd, cre_dt, last_chg_dt,
 last_chg_user_nm, hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt, ewp_tr_acrg)		
		
SELECT distinct ty.tract_year_identifier As TR_YR_ID,
ty.tract_identifier As TR_ID,
ty.farm_year_identifier As FARM_YR_ID,
ty.farmland_acreage As FMLD_ACRG,
ty.cropland_acreage As CPLD_ACRG,
ty.crp_acreage As CRP_ACRG,
ty.mpl_acreage As MPL_ACRG,
ty.wbp_acreage As WBP_ACRG,
ty.wrp_tract_acreage As WRP_TR_ACRG,
ty.grp_cropland_acreage As GRP_CPLD_ACRG,
ty.state_conservation_acreage As ST_CNSV_ACRG,
ty.other_conservation_acreage As OT_CNSV_ACRG,
ty.sugarcane_acreage As SUGARCANE_ACRG,
ty.nap_crop_acreage As NAP_CROP_ACRG,
ty.native_sod_broken_out_acreage As NTV_SOD_BRK_OUT_ACRG,
ty.hel_tract_code As HEL_TR_CD,
ty.wl_presence_code As WL_PRES_CD,
LTrim(RTrim(f.farm_number)) As FARM_NBR,
LTrim(RTrim(t.tract_number)) As TR_NBR,
LTrim(RTrim(c.state_fsa_code)) As ST_FSA_CD,
LTrim(RTrim(c.county_fsa_code) ) As CNTY_FSA_CD,
CAST(tp.time_period_name AS numeric(4)) as PGM_YR,
LTrim(RTrim(ty.data_status_code)) As DATA_STAT_CD,
ty.creation_date As CRE_DT,
ty.last_change_date As LAST_CHG_DT,
LTrim(RTrim(ty.last_change_user_name)) As LAST_CHG_USER_NM,
''  as hash_dif,
ty.cdc_oper_cd AS CDC_OPER_CD,
CAST(current_date as date) as load_dt,
'SAP/CRM' as data_src_nm,
CAST(current_date-1 as date) as CDC_DT,
ty.ewp_acreage As EWP_TR_ACRG 
FROM farm_records_reporting.tract_year ty
LEFT JOIN farm_records_reporting.tract t ON ( t.tract_identifier = ty.tract_identifier ) 
JOIN farm_records_reporting.county_office_control c ON ( t.county_office_control_identifier = c.county_office_control_identifier ) 
JOIN farm_records_reporting.farm_year fy ON ( ty.farm_year_identifier = fy.farm_year_identifier ) 
JOIN farm_records_reporting.time_period tp ON ( fy.time_period_identifier = tp.time_period_identifier ) 
JOIN farm_records_reporting.farm f ON (fy.farm_identifier=f.farm_identifier)
where t.cdc_dt >= current_date - 1	
