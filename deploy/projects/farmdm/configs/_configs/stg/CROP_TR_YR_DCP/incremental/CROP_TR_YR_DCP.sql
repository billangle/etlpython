INSERT INTO sql_farm_rcd_stg.crop_tr_yr_dcp (
crop_tr_yr_dcp_id,
tr_yr_dcp_id,
crop_id,
crp_rdn_acrg,
crp_rel_acrg,
dcp_crop_base_acrg,
fav_rdn_acrg,
ccp_pymt_yld,
crp_pymt_yld,
dir_pymt_yld,
fav_dir_pymt_yld,
fav_ccp_pymt_yld,
pgm_abr,
fsa_crop_cd,
fsa_crop_type_cd,
farm_nbr,
tr_nbr,
st_fsa_cd,
cnty_fsa_cd,
pgm_yr,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
hash_dif,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt
)
SELECT
ctyd.crop_tract_year_dcp_identifier,
ctyd.tract_year_dcp_identifier,
ctyd.crop_identifier,
ctyd.crp_reduction_acreage,
ctyd.crp_released_acreage,
ctyd.dcp_crop_base_acreage,
ctyd.fav_reduction_acreage,
ctyd.ccp_payment_yield,
ctyd.crp_payment_yield,
ctyd.direct_payment_yield,
ctyd.fav_direct_payment_yield,
ctyd.fav_ccp_payment_yield,
c.program_abbreviation,
c.fsa_crop_code,
c.fsa_crop_type_code,
f.farm_number,
t.tract_number,
co.state_fsa_code,
co.county_fsa_code,
cast(time_period.time_period_name as int) as pgm_yr,
ctyd.data_status_code,
ctyd.creation_date,
ctyd.last_change_date,
ctyd.last_change_user_name,
'' AS hash_dif,
ctyd.cdc_oper_cd AS cdc_oper_cd,
CURRENT_DATE,
'SQL_FARM_RCD',
ctyd.cdc_dt
FROM farm_records_reporting.crop_tract_year_dcp ctyd
LEFT JOIN farm_records_reporting.tract_year_dcp tyd
ON ctyd.tract_year_dcp_identifier = tyd.tract_year_dcp_identifier
JOIN farm_records_reporting.tract_year ty
ON tyd.tract_year_identifier = ty.tract_year_identifier
JOIN farm_records_reporting.farm_year fy
ON ty.farm_year_identifier = fy.farm_year_identifier
JOIN farm_records_reporting.farm f
ON fy.farm_identifier = f.farm_identifier
JOIN farm_records_reporting.county_office_control co
ON f.county_office_control_identifier = co.county_office_control_identifier
JOIN farm_records_reporting.crop c
ON ctyd.crop_identifier = c.crop_identifier
JOIN farm_records_reporting.time_period
ON fy.time_period_identifier = time_period.time_period_identifier
JOIN farm_records_reporting.tract t
ON ty.tract_identifier = t.tract_identifier
where ctyd.cdc_dt between date '{ETL_START_DATE}' and date '{ETL_END_DATE}'