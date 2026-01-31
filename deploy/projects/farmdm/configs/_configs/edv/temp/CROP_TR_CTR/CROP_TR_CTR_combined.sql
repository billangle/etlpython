INSERT INTO sql_farm_rcd_stg.crop_tr_ctr (
crop_tr_ctr_id,
tr_id,
crop_id,
ctr_nbr,
rdn_acrg_strt_yr,
rdn_acrg,
crop_pgm_pymt_yld,
crop_pgm_alt_pymt_yld,
tr_nbr,
st_fsa_cd,
cnty_fsa_cd,
pgm_abr,
fsa_crop_cd,
fsa_crop_type_cd,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
hash_dif,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
tr_yr_id,
pgm_yr
)
SELECT
ctc.crop_tract_contract_identifier,
t.tract_identifier,
ctc.crop_identifier,
ctc.contract_number,
ctc.reduction_acreage_start_year,
ctc.reduction_acreage,
ctc.crop_program_payment_yield,
ctc.crop_program_alternate_payment_yield,
t.tract_number,
co.state_fsa_code,
co.county_fsa_code,
c.program_abbreviation,
c.fsa_crop_code,
c.fsa_crop_type_code,
ctc.data_status_code,
ctc.creation_date,
ctc.last_change_date,
ctc.last_change_user_name,
'' AS hash_dif,
'I' AS cdc_oper_cd,
CURRENT_DATE,
'SQL_FARM_RCD',
CURRENT_DATE - 1,
ctc.tract_year_identifier,
CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE) < 10
THEN EXTRACT(YEAR FROM CURRENT_DATE)
ELSE EXTRACT(YEAR FROM CURRENT_DATE) + 1 END
-- c.fsa_crop_abbreviation,
-- c.fsa_crop_name,
-- c.fsa_crop_type_name,
-- c.display_sequence_number,
-- CONCAT_WS('-', co.state_fsa_code, co.county_fsa_code, co.last_assigned_farm_number)
FROM farm_records_reporting.crop_tract_contract ctc
JOIN farm_records_reporting.crop c ON c.crop_identifier = ctc.crop_identifier
JOIN farm_records_reporting.tract_year ty ON ctc.tract_year_identifier = ty.tract_year_identifier
JOIN farm_records_reporting.tract t ON t.tract_identifier = ty.tract_identifier
JOIN farm_records_reporting.county_office_control co ON co.county_office_control_identifier = t.county_office_control_identifier
where ctc.cdc_dt >= DATE '2025-11-17'


