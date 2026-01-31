SELECT
cfyd.crop_farm_year_dcp_identifier,
cfyd.farm_year_dcp_identifier,
cfyd.crop_identifier,
cfyd.crp_reduction_acreage,
cfyd.crp_released_acreage,
cfyd.ccp_payment_yield,
cfyd.direct_payment_yield,
c.program_abbreviation,
c.fsa_crop_code,
c.fsa_crop_type_code,
f.farm_number,
co.state_fsa_code,
co.county_fsa_code,
CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE) < 10
THEN EXTRACT(YEAR FROM CURRENT_DATE)
ELSE EXTRACT(YEAR FROM CURRENT_DATE) + 1 END,
cfyd.data_status_code,
cfyd.creation_date,
cfyd.last_change_date,
cfyd.last_change_user_name,
'' AS hash_dif,
cdc_oper_cd AS cdc_oper_cd,
CURRENT_DATE - 1,
'SQL_FARM_RCD',
cdc_dt
FROM farm_records_reporting.crop_farm_year_dcp cfyd
JOIN farm_records_reporting.crop c ON c.crop_identifier = cfyd.crop_identifier
JOIN farm_records_reporting.farm_year_dcp fyd ON fyd.farm_year_dcp_identifier = cfyd.farm_year_dcp_identifier
JOIN farm_records_reporting.farm_year fy ON fyd.farm_year_identifier = fy.farm_year_identifier
JOIN farm_records_reporting.farm f ON f.farm_identifier = fy.farm_identifier
JOIN farm_records_reporting.county_office_control co ON co.county_office_control_identifier = f.county_office_control_identifier
JOIN farm_records_reporting.tract_year ty ON fy.farm_year_identifier = ty.farm_year_identifier
JOIN farm_records_reporting.tract t ON t.tract_identifier = ty.tract_identifier
AND f.county_office_control_identifier = t.county_office_control_identifier
where cfyd.cdc_dt between date '{ETL_START_DATE}' and date '{ETL_END_DATE}'