INSERT INTO sql_farm_rcd_stg.crop_farm_yr_mng_val_adj
(
    crop_farm_yr_mng_val_adj_id,
    yr_arc_plc_ptcp_elct_id,
    farm_yr_crop_irr_hist_id,
    crop_farm_yr_dcp_id,
    crop_farm_yr_mng_val_adj_ty,
    crop_farm_yr_mng_val_adj_rs,
    aft_adj_val,
    bef_adj_val,
    pgm_abr,
    fsa_crop_cd,
    fsa_crop_type_cd,
    farm_nbr,
    st_fsa_cd,
    cnty_fsa_cd,
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
    CAST(c.crop_farm_year_managed_values_adjustment_identifier AS bigint) AS crop_farm_yr_mng_val_adj_id,
    CAST(c.year_arc_plc_participation_election_identifier AS bigint) AS yr_arc_plc_ptcp_elct_id,
    CAST(c.farm_year_crop_irrigation_history_identifier AS bigint) AS farm_yr_crop_irr_hist_id,
    CAST(c.crop_farm_year_dcp_identifier AS bigint) AS crop_farm_yr_dcp_id,
    CAST(c.crop_farm_year_managed_values_adjustment_type_code AS int) AS crop_farm_yr_mng_val_adj_ty,
    CAST(c.crop_farm_year_managed_values_adjustment_reason_code AS int) AS crop_farm_yr_mng_val_adj_rs,
    CAST(c.after_adjustment_value AS numeric) AS aft_adj_val,
    CAST(c.before_adjustment_value AS numeric) AS bef_adj_val,
    CAST(cr.program_abbreviation AS text) AS pgm_abr,
    CAST(cr.fsa_crop_code AS text) AS fsa_crop_cd,
    CAST(cr.fsa_crop_type_code AS text) AS fsa_crop_type_cd,
    CAST(f.farm_number AS int) AS farm_nbr,
    CAST(co.state_fsa_code AS text) AS st_fsa_cd,
    CAST(co.county_fsa_code AS text) AS cnty_fsa_cd,
    CAST(tp.time_period_name AS int) AS pgm_yr,
    CAST(c.data_status_code AS text) AS data_stat_cd,
    CAST(c.creation_date AS date) AS cre_dt,
    CAST(c.creation_user_name AS text) AS cre_user_nm,
    CAST(c.last_change_date AS date) AS last_chg_dt,
    CAST(c.last_change_user_name AS text) AS last_chg_user_nm,
    '' AS hash_dif,
    'I' AS cdc_oper_cd,
    CURRENT_DATE AS load_dt,
    'SQL_FARM_RCD' AS data_src_nm,
    CURRENT_DATE - 1 AS cdc_dt
FROM farm_records_reporting.crop_farm_year_managed_values_adjustment c
LEFT JOIN farm_records_reporting.year_arc_plc_participation_election y
    ON y.year_arc_plc_participation_election_identifier = c.year_arc_plc_participation_election_identifier
JOIN farm_records_reporting.farm_year fy
    ON y.farm_year_identifier = fy.farm_year_identifier
JOIN farm_records_reporting.crop cr
    ON y.crop_identifier = cr.crop_identifier
JOIN farm_records_reporting.farm f
    ON fy.farm_identifier = f.farm_identifier
JOIN farm_records_reporting.county_office_control co
    ON f.county_office_control_identifier = co.county_office_control_identifier
JOIN farm_records_reporting.time_period tp
    ON fy.time_period_identifier = tp.time_period_identifier
WHERE c.year_arc_plc_participation_election_identifier IS NOT NULL

UNION ALL

SELECT
    CAST(c.crop_farm_year_managed_values_adjustment_identifier AS bigint) AS crop_farm_yr_mng_val_adj_id,
    CAST(c.year_arc_plc_participation_election_identifier AS bigint) AS yr_arc_plc_ptcp_elct_id,
    CAST(c.farm_year_crop_irrigation_history_identifier AS bigint) AS farm_yr_crop_irr_hist_id,
    CAST(c.crop_farm_year_dcp_identifier AS bigint) AS crop_farm_yr_dcp_id,
    CAST(c.crop_farm_year_managed_values_adjustment_type_code AS int) AS crop_farm_yr_mng_val_adj_ty,
    CAST(c.crop_farm_year_managed_values_adjustment_reason_code AS int) AS crop_farm_yr_mng_val_adj_rs,
    CAST(c.after_adjustment_value AS numeric) AS aft_adj_val,
    CAST(c.before_adjustment_value AS numeric) AS bef_adj_val,
    CAST(cr.program_abbreviation AS text) AS pgm_abr,
    CAST(cr.fsa_crop_code AS text) AS fsa_crop_cd,
    CAST(cr.fsa_crop_type_code AS text) AS fsa_crop_type_cd,
    CAST(f.farm_number AS int) AS farm_nbr,
    CAST(co.state_fsa_code AS text) AS st_fsa_cd,
    CAST(co.county_fsa_code AS text) AS cnty_fsa_cd,
    CAST(tp.time_period_name AS int) AS pgm_yr,
    CAST(c.data_status_code AS text) AS data_stat_cd,
    CAST(c.creation_date AS date) AS cre_dt,
    CAST(c.creation_user_name AS text) AS cre_user_nm,
    CAST(c.last_change_date AS date) AS last_chg_dt,
    CAST(c.last_change_user_name AS text) AS last_chg_user_nm,
    '' AS hash_dif,
    'I' AS cdc_oper_cd,
    CURRENT_DATE AS load_dt,
    'SQL_FARM_RCD' AS data_src_nm,
    CURRENT_DATE - 1 AS cdc_dt
FROM farm_records_reporting.crop_farm_year_managed_values_adjustment c
LEFT JOIN farm_records_reporting.farm_year_crop_irrigation_history fyc
    ON fyc.farm_year_crop_irrigation_history_identifier = c.farm_year_crop_irrigation_history_identifier
JOIN farm_records_reporting.irrigation_county_crop icc
    ON fyc.irrigation_county_crop_identifier = icc.irrigation_county_crop_identifier
JOIN farm_records_reporting.crop cr
    ON icc.crop_identifier = cr.crop_identifier
JOIN farm_records_reporting.farm_year fy
    ON fyc.farm_year_identifier = fy.farm_year_identifier
JOIN farm_records_reporting.farm f
    ON fy.farm_identifier = f.farm_identifier
JOIN farm_records_reporting.county_office_control co
    ON f.county_office_control_identifier = co.county_office_control_identifier
JOIN farm_records_reporting.time_period tp
    ON fy.time_period_identifier = tp.time_period_identifier
WHERE c.farm_year_crop_irrigation_history_identifier IS NOT NULL
  AND c.year_arc_plc_participation_election_identifier IS NULL

UNION ALL

SELECT
    CAST(c.crop_farm_year_managed_values_adjustment_identifier AS bigint) AS crop_farm_yr_mng_val_adj_id,
    CAST(c.year_arc_plc_participation_election_identifier AS bigint) AS yr_arc_plc_ptcp_elct_id,
    CAST(c.farm_year_crop_irrigation_history_identifier AS bigint) AS farm_yr_crop_irr_hist_id,
    CAST(c.crop_farm_year_dcp_identifier AS bigint) AS crop_farm_yr_dcp_id,
    CAST(c.crop_farm_year_managed_values_adjustment_type_code AS int) AS crop_farm_yr_mng_val_adj_ty,
    CAST(c.crop_farm_year_managed_values_adjustment_reason_code AS int) AS crop_farm_yr_mng_val_adj_rs,
    CAST(c.after_adjustment_value AS numeric) AS aft_adj_val,
    CAST(c.before_adjustment_value AS numeric) AS bef_adj_val,
    CAST(cr.program_abbreviation AS text) AS pgm_abr,
    CAST(cr.fsa_crop_code AS text) AS fsa_crop_cd,
    CAST(cr.fsa_crop_type_code AS text) AS fsa_crop_type_cd,
    CAST(f.farm_number AS int) AS farm_nbr,
    CAST(co.state_fsa_code AS text) AS st_fsa_cd,
    CAST(co.county_fsa_code AS text) AS cnty_fsa_cd,
    CAST(tp.time_period_name AS int) AS pgm_yr,
    CAST(c.data_status_code AS text) AS data_stat_cd,
    CAST(c.creation_date AS date) AS cre_dt,
    CAST(c.creation_user_name AS text) AS cre_user_nm,
    CAST(c.last_change_date AS date) AS last_chg_dt,
    CAST(c.last_change_user_name AS text) AS last_chg_user_nm,
    '' AS hash_dif,
    'I' AS cdc_oper_cd,
    CURRENT_DATE AS load_dt,
    'SQL_FARM_RCD' AS data_src_nm,
    CURRENT_DATE - 1 AS cdc_dt
FROM farm_records_reporting.crop_farm_year_managed_values_adjustment c
LEFT JOIN farm_records_reporting.crop_farm_year_dcp dcp
    ON dcp.crop_farm_year_dcp_identifier = c.crop_farm_year_dcp_identifier
JOIN farm_records_reporting.crop cr
    ON dcp.crop_identifier = cr.crop_identifier
JOIN farm_records_reporting.farm_year_dcp fyd
    ON dcp.farm_year_dcp_identifier = fyd.farm_year_dcp_identifier
JOIN farm_records_reporting.farm_year fy
    ON fyd.farm_year_identifier = fy.farm_year_identifier
JOIN farm_records_reporting.farm f
    ON fy.farm_identifier = f.farm_identifier
JOIN farm_records_reporting.county_office_control co
    ON f.county_office_control_identifier = co.county_office_control_identifier
JOIN farm_records_reporting.time_period tp
    ON fy.time_period_identifier = tp.time_period_identifier
WHERE c.crop_farm_year_dcp_identifier IS NOT NULL
  AND c.farm_year_crop_irrigation_history_identifier IS NULL
  AND c.year_arc_plc_participation_election_identifier IS NULL
  AND c.cdc_dt >= DATE '2025-11-17'
