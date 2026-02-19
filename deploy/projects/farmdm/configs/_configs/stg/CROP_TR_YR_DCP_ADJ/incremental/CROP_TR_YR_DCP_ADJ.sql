INSERT INTO sql_farm_rcd_stg.crop_tr_yr_dcp_adj (
crop_tr_yr_dcp_adj_id,
crop_tr_yr_dcp_id,
dcp_adj_type_cd,
dcp_adj_rsn_cd,
aft_adj_val,
bef_adj_val,
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
ctda.crop_tract_year_dcp_adjustment_identifier,
ctyd.crop_tract_year_dcp_identifier,
ctda.dcp_adjustment_type_code,
ctda.dcp_adjustment_reason_code,
ctda.after_adjustment_value,
ctda.before_adjustment_value,
c.program_abbreviation,
c.fsa_crop_code,
c.fsa_crop_type_code,
f.farm_number,
t.tract_number,
co.state_fsa_code,
co.county_fsa_code,
CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE) < 10
THEN EXTRACT(YEAR FROM CURRENT_DATE)
ELSE EXTRACT(YEAR FROM CURRENT_DATE) + 1 END,
ctyd.data_status_code,
ctyd.creation_date,
ctyd.last_change_date,
ctyd.last_change_user_name,
'' AS hash_dif,
ctda.cdc_oper_cd AS cdc_oper_cd,
CURRENT_DATE,
'SQL_FARM_RCD',
ctda.cdc_dt
FROM farm_records_reporting.crop_tract_year_dcp_adjustment ctda
JOIN farm_records_reporting.crop_tract_year_dcp ctyd
    ON ctda.crop_tract_year_dcp_identifier = ctyd.crop_tract_year_dcp_identifier
JOIN farm_records_reporting.crop c
    ON ctyd.crop_identifier = c.crop_identifier
JOIN farm_records_reporting.crop_farm_year_dcp cfyd
    ON c.crop_identifier = cfyd.crop_identifier
JOIN farm_records_reporting.farm_year_dcp fyd
    ON cfyd.farm_year_dcp_identifier = fyd.farm_year_dcp_identifier
JOIN farm_records_reporting.farm_year fy
    ON fyd.farm_year_identifier = fy.farm_year_identifier
JOIN farm_records_reporting.farm f
    ON fy.farm_identifier = f.farm_identifier
JOIN farm_records_reporting.county_office_control co
    ON f.county_office_control_identifier = co.county_office_control_identifier
JOIN farm_records_reporting.tract_year ty
    ON fy.farm_year_identifier = ty.farm_year_identifier
JOIN farm_records_reporting.tract t
    ON t.tract_identifier = ty.tract_identifier
    AND t.county_office_control_identifier = f.county_office_control_identifier
JOIN farm_records_reporting.crop_tract_contract ctc
    ON c.crop_identifier = ctc.crop_identifier
JOIN farm_records_reporting.tract_year_dcp tyd
    ON ty.tract_year_identifier = tyd.tract_year_identifier
where ctda.cdc_dt between date '{ETL_START_DATE}' and date '{ETL_END_DATE}'
