INSERT INTO sql_farm_rcd_stg.farm_rcon
(farm_rcon_id, rcon_id, prnt_farm_yr_id, rslt_farm_yr_id, prnt_farm_nbr, 
prnt_st_fsa_cd, prnt_cnty_fsa_cd, rslt_farm_nbr, rslt_st_fsa_cd, rslt_cnty_fsa_cd, 
pgm_yr, data_stat_cd, cre_dt, last_chg_dt, last_chg_user_nm, 
hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt)
SELECT DISTINCT
    fr.farm_reconstitution_identifier,
    fr.reconstitution_identifier,
    fr.parent_farm_year_identifier,
    fr.resulting_farm_year_identifier,
    f.farm_number as prnt_farm_nbr,
    coc.state_fsa_code as prnt_st_fsa_cd,
    coc.county_fsa_code as prnt_cnty_fsa_cd,
    f.farm_number as rslt_farm_nbr,
    coc.state_fsa_code as rslt_st_fsa_cd, 
    coc.county_fsa_code  as rslt_cnty_fsa_cd,
    cast(t.time_period_name as int) as pgm_yr,
    fr.data_status_code,
    fr.creation_date,
    fr.last_change_date,
    fr.last_change_user_name,
    ''  as hash_dif,
	'I' as cdc_oper_cd,
	CAST(current_date as date) as load_dt,
	'SAP/CRM' as data_src_nm,
	fr.cdc_dt as cdc_dt
FROM
    farm_records_reporting.farm_reconstitution fr
inner join farm_records_reporting.farm_year fy 
on fy.farm_year_identifier = fr.parent_farm_year_identifier
and fr.resulting_farm_year_identifier = fy.farm_year_identifier
inner join farm_records_reporting.farm f
on f.farm_identifier = fy.farm_identifier
join farm_records_reporting.time_period t
on t.time_period_identifier = fy.time_period_identifier
inner join farm_records_reporting.county_office_control coc
on f.county_office_control_identifier = coc.county_office_control_identifier
where fr.cdc_dt >= DATE '2025-11-17'