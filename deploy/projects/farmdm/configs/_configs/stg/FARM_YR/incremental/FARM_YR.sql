INSERT INTO sql_farm_rcd_stg.farm_yr
(farm_yr_id, farm_id, tm_prd_id, crp_acrg, mpl_acrg, farm_nbr, st_fsa_cd, cnty_fsa_cd, pgm_yr, 
data_stat_cd, cre_dt, last_chg_dt, last_chg_user_nm, ver_nbr, hash_dif, cdc_oper_cd, load_dt, 
data_src_nm, cdc_dt, arc_plc_elg_dter_cd)
select  distinct fy.farm_year_identifier as farm_yr_id,
        f.farm_identifier as farm_id,
		c.time_period_identifier as tm_prd_id,
		fy.crp_acreage as crp_acrg,
		fy.mpl_acreage as mpl_acrg,
		f.farm_number as farm_nbr,
		c.state_fsa_code as st_fsa_cd,
		c.county_fsa_code as cnty_fsa_cd,
		cast(t.time_period_name as int) as pgm_yr,
		f.data_status_code as data_stat_cd,
		f.creation_date as cre_dt,
        f.last_change_date as last_chg_dt,
        f.last_change_user_name as last_chg_user_nm,
		fy.version_number as ver_nbr,
		''  as hash_dif,
		fy.cdc_oper_cd as cdc_oper_cd,
		CAST(current_date as date) as load_dt,
		'SAP/CRM' as data_src_nm,
		fy.cdc_dt as cdc_dt,
		--fy.arc_plc_elg_dter_id as arc_plc_elg_dter_id,
		fy.arc_plc_eligibility_determination_code as arc_plc_elg_dter_cd		
from farm_records_reporting.farm_year fy
--from farm_records_reporting.county_office_control c
inner join farm_records_reporting.time_period t
on t.time_period_identifier = fy.time_period_identifier
inner join farm_records_reporting.farm f
on f.farm_identifier = fy.farm_identifier
inner join farm_records_reporting.county_office_control c
on f.county_office_control_identifier = c.county_office_control_identifier
and fy.time_period_identifier = c.time_period_identifier
where fy.cdc_dt between date '{ETL_START_DATE}' and date '{ETL_END_DATE}'
