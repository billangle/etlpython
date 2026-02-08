INSERT INTO sql_farm_rcd_stg.farm_prdr_yr
(
    farm_prdr_yr_id, 
    core_cust_id, 
    farm_yr_id, 
    prdr_invl_cd, 
    prdr_invl_intrpt_ind, 
    prdr_invl_strt_dt, 
    prdr_invl_end_dt, 
    farm_prdr_hel_excp_cd, 
    farm_prdr_cw_excp_cd, 
    farm_prdr_pcw_excp_cd, 
    data_stat_cd, 
    cre_dt, 
    last_chg_dt, 
    last_chg_user_nm, 
    tm_prd_id, 
    pgm_yr, 
    st_fsa_cd, 
    cnty_fsa_cd, 
    farm_id, 
    farm_nbr, 
    hash_dif, 
    cdc_oper_cd, 
    load_dt, 
    data_src_nm, 
    cdc_dt, 
    hel_apls_exhst_dt, 
    cw_apls_exhst_dt, 
    pcw_apls_exhst_dt, 
    farm_prdr_rma_hel_excp_cd, 
    farm_prdr_rma_cw_excp_cd, 
    farm_prdr_rma_pcw_excp_cd
)
SELECT DISTINCT
    fpy.farm_producer_year_identifier AS farm_prdr_yr_id,
    fpy.core_customer_identifier AS core_cust_id,
    fpy.farm_year_identifier AS farm_yr_id,
    fpy.producer_involvement_code AS prdr_invl_cd,
    fpy.producer_involvement_interrupted_indicator AS prdr_invl_intrpt_ind,
    fpy.producer_involvement_start_date AS prdr_invl_strt_dt,
    fpy.producer_involvement_end_date AS prdr_invl_end_dt,
    fpy.farm_producer_hel_exception_code AS farm_prdr_hel_excp_cd,
    fpy.farm_producer_cw_exception_code AS farm_prdr_cw_excp_cd,
    fpy.farm_producer_pcw_exception_code AS farm_prdr_pcw_excp_cd,
    fpy.data_status_code AS data_stat_cd,
    fpy.creation_date AS cre_dt,
    fpy.last_change_date AS last_chg_dt,
    fpy.last_change_user_name AS last_chg_user_nm,
    fpy.time_period_identifier AS tm_prd_id,
    c.time_period_identifier + 1998 AS pgm_yr,
    fpy.state_fsa_code AS st_fsa_cd,
    fpy.county_fsa_code AS cnty_fsa_cd,
    fpy.farm_identifier AS farm_id,
    f.farm_number AS farm_nbr,
    '' AS hash_dif,
    cpy.cdc_oper_cd AS cdc_oper_cd,
    CAST(current_date AS date) AS load_dt,
    'SQL_FARM_RCD' AS data_src_nm,
    fpy.cdc_dt AS cdc_dt,
    fpy.hel_appeals_exhausted_date AS hel_apls_exhst_dt,
    fpy.cw_appeals_exhausted_date AS cw_apls_exhst_dt,
    fpy.pcw_appeals_exhausted_date AS pcw_apls_exhst_dt,
    fpy.farm_producer_rma_hel_exception_code AS farm_prdr_rma_hel_excp_cd,
    fpy.farm_producer_rma_cw_exception_code AS farm_prdr_rma_cw_excp_cd,
    fpy.farm_producer_rma_pcw_exception_code AS farm_prdr_rma_pcw_excp_cd
FROM farm_records_reporting.farm_producer_year fpy
INNER JOIN farm_records_reporting.farm_year fy
    ON fy.farm_year_identifier = fpy.farm_year_identifier
INNER JOIN farm_records_reporting.farm f
    ON f.farm_identifier = fy.farm_identifier
INNER JOIN farm_records_reporting.county_office_control c
    ON f.county_office_control_identifier = c.county_office_control_identifier
WHERE fpy.cdc_dt between date '{ETL_START_DATE}' and date '{ETL_END_DATE}'
