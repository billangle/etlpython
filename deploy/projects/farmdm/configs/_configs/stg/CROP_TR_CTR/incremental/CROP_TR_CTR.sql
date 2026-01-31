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
ctc.cdc_oper_cd AS cdc_oper_cd,
CURRENT_DATE,
'SQL_FARM_RCD',
ctc.cdc_dt,
ctc.tract_year_identifier,
CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE) < 10
THEN EXTRACT(YEAR FROM CURRENT_DATE)
ELSE EXTRACT(YEAR FROM CURRENT_DATE) + 1 END
FROM farm_records_reporting.crop_tract_contract ctc
JOIN farm_records_reporting.crop c ON c.crop_identifier = ctc.crop_identifier
JOIN farm_records_reporting.tract_year ty ON ctc.tract_year_identifier = ty.tract_year_identifier
JOIN farm_records_reporting.tract t ON t.tract_identifier = ty.tract_identifier
JOIN farm_records_reporting.county_office_control co ON co.county_office_control_identifier = t.county_office_control_identifier
where ctc.cdc_dt between date '{ETL_START_DATE}' and date '{ETL_END_DATE}'


