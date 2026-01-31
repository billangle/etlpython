SELECT DISTINCT
    fyd.farm_year_dcp_identifier AS farm_yr_dcp_id,
    fyd.farm_year_identifier AS farm_yr_id,
    dcp_double_crop_acreage AS dcp_dbl_crop_acrg,
    f.farm_number AS farm_nbr,
    c.state_fsa_code AS st_fsa_cd,
    c.county_fsa_code AS cnty_fsa_cd,
    c.time_period_identifier + 1998 AS pgm_yr,
    f.data_status_code AS data_stat_cd,
    f.creation_date AS cre_dt,
    f.last_change_date AS last_chg_dt,
    f.last_change_user_name AS last_chg_user_nm,
    '' AS hash_dif,
    fyd.cdc_oper_cd AS cdc_oper_cd,
    CURRENT_DATE AS load_dt,
    'SQL_FARM_RCD' AS data_src_nm,
    fyd.cdc_dt AS cdc_dt
FROM farm_records_reporting.farm_year_dcp fyd
INNER JOIN farm_records_reporting.farm_year fy
    ON fyd.farm_year_identifier = fy.farm_year_identifier
INNER JOIN farm_records_reporting.farm f
    ON f.farm_identifier = fy.farm_identifier
INNER JOIN farm_records_reporting.county_office_control c
    ON f.county_office_control_identifier = c.county_office_control_identifier
where fyd.cdc_dt between date '{ETL_START_DATE}' and date '{ETL_END_DATE}'