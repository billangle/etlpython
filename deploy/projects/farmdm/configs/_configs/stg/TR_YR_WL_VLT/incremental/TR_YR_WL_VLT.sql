select  
		tywv.tract_year_wetland_violation_identifier as tr_yr_wl_vlt_id,
		ty.tract_year_identifier as tr_yr_id,
		tywv.wetland_violation_type_code as wl_vlt_type_cd,
		f.farm_number as farm_nbr,
        t.tract_number  as tr_nbr,
		c.state_fsa_code as st_fsa_cd,
		c.county_fsa_code as cnty_fsa_cd,
		cast(tp.time_period_name as int) as pgm_yr,
		tywv.data_status_code as data_stat_cd, ---changed to tywv
		tywv.creation_date as cre_dt,	---changed to tywv
		tywv.last_change_date as last_chg_dt,	---changed to tywv
		tywv.last_change_user_name as last_chg_user_nm,	---changed to tywv
		'' as hash_dif,
		tywv.cdc_oper_cd as cdc_oper_cd,
		CAST(current_date as date) as load_dt,
		'SQL_FARM_RCD' as data_src_nm,
		tywv.cdc_dt as cdc_dt
from farm_records_reporting.tract_year_wetland_violation tywv
	inner join farm_records_reporting.tract_year ty
		on ty.tract_year_identifier = tywv.tract_year_identifier
	inner join farm_records_reporting.farm_year fy
		on fy.farm_year_identifier = ty.farm_year_identifier
	inner join farm_records_reporting.time_period tp
		on tp.time_period_identifier = fy.time_period_identifier
	inner join farm_records_reporting.tract t
		on t.tract_identifier = ty.tract_identifier
	inner join farm_records_reporting.farm f
		on fy.farm_identifier = f.farm_identifier
	inner join farm_records_reporting.county_office_control c
		on f.county_office_control_identifier = c.county_office_control_identifier --- county_office_control_identifier is PK
where tywv.cdc_dt between date '{ETL_START_DATE}' and date '{ETL_END_DATE}'