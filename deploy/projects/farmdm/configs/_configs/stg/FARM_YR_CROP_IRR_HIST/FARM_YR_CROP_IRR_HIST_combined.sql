INSERT INTO sql_farm_rcd_stg.farm_yr_crop_irr_hist
(
    farm_yr_crop_irr_hist_id, 
    farm_yr_id, 
    irr_cnty_crop_id, 
    hist_irr_pct, 
    st_fsa_cd, 
    cnty_fsa_cd, 
    pgm_abr, 
    fsa_crop_cd, 
    fsa_crop_type_cd, 
    farm_nbr, 
    farm_st_fsa_cd, 
    farm_cnty_fsa_cd, 
    pgm_yr, 
    data_stat_cd, 
    cre_dt, 
    cre_user_nm, 
    last_chg_dt, 
    last_chg_user_nm, 
    hash_dif, 
    cdc_oper_cd, 
    load_dt, 
    data_src_nm, 
    cdc_dt
)
SELECT
    farm_year_crop_irrigation_history.farm_year_crop_irrigation_history_identifier AS farm_yr_crop_irr_hist_id,
    farm_year_crop_irrigation_history.farm_year_identifier AS farm_yr_id,
    farm_year_crop_irrigation_history.irrigation_county_crop_identifier AS irr_cnty_crop_id,
    farm_year_crop_irrigation_history.historical_irrigation_percentage AS hist_irr_pct,
    LTRIM(RTRIM(phyloc.state_fsa_code)) AS st_fsa_cd,
    LTRIM(RTRIM(phyloc.county_fsa_code)) AS cnty_fsa_cd,
    LTRIM(RTRIM(crop.program_abbreviation)) AS pgm_abr,
    LTRIM(RTRIM(crop.fsa_crop_code)) AS fsa_crop_cd,
    LTRIM(RTRIM(crop.fsa_crop_type_code)) AS fsa_crop_type_cd,
    LTRIM(RTRIM(farm.farm_number)) AS farm_nbr,
    LTRIM(RTRIM(admnloc.state_fsa_code)) AS farm_st_fsa_cd,
    LTRIM(RTRIM(admnloc.county_fsa_code)) AS farm_cnty_fsa_cd,
    cast(time_period.time_period_name AS int) pgm_yr,
    LTRIM(RTRIM(farm_year_crop_irrigation_history.data_status_code)) AS data_stat_cd,
    farm_year_crop_irrigation_history.creation_date AS cre_dt,
    LTRIM(RTRIM(farm_year_crop_irrigation_history.creation_user_name)) AS cre_user_nm,
    farm_year_crop_irrigation_history.last_change_date AS last_chg_dt,
    LTRIM(RTRIM(farm_year_crop_irrigation_history.last_change_user_name)) AS last_chg_user_nm,
	''  as hash_dif,
	'I' as cdc_oper_cd,
	CAST(current_date as date) as load_dt,
	'SAP/CRM' as data_src_nm,
	CAST((current_date - 1) as date) as cdc_dt
    -- The following columns were in original SELECT but are not in INSERT, so commented out:
    -- LTRIM(RTRIM(phyloc.state_fsa_code)) AS ST_FSA_CD,
    -- LTRIM(RTRIM(phyloc.county_fsa_code)) AS CNTY_FSA_CD,
    -- LTRIM(RTRIM(admnloc.state_fsa_code)) AS FARM_ST_FSA_CD,
    -- LTRIM(RTRIM(admnloc.county_fsa_code)) AS FARM_CNTY_FSA_CD
FROM farm_records_reporting.farm_year_crop_irrigation_history 
LEFT JOIN farm_records_reporting.irrigation_county_crop 
    ON farm_year_crop_irrigation_history.irrigation_county_crop_identifier = irrigation_county_crop.irrigation_county_crop_identifier
LEFT JOIN farm_records_reporting.farm_year 
    ON farm_year_crop_irrigation_history.farm_year_identifier = farm_year.farm_year_identifier
LEFT JOIN farm_records_reporting.farm 
    ON farm_year.farm_identifier = farm.farm_identifier
LEFT JOIN farm_records_reporting.crop 
    ON irrigation_county_crop.crop_identifier = crop.crop_identifier
LEFT JOIN farm_records_reporting.time_period 
    ON farm_year.time_period_identifier = time_period.time_period_identifier
LEFT JOIN farm_records_reporting.county_office_control AS phyloc 
    ON irrigation_county_crop.county_office_control_identifier = phyloc.county_office_control_identifier
LEFT JOIN farm_records_reporting.county_office_control AS admnloc 
    ON farm.county_office_control_identifier = admnloc.county_office_control_identifier
where farm_year_crop_irrigation_history.cdc_dt >= current_date - 1