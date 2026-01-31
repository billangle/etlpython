INSERT INTO sql_farm_rcd_stg.tr_yr_wl_vlt
(tr_yr_wl_vlt_id, tr_yr_id, wl_vlt_type_cd, farm_nbr, tr_nbr, st_fsa_cd, cnty_fsa_cd,
pgm_yr, data_stat_cd, cre_dt, last_chg_dt, last_chg_user_nm, hash_dif, cdc_oper_cd, load_dt, data_src_nm, cdc_dt)
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
		-- ty.ewp_acreage as ewp_tr_acrg,
		tywv.creation_date as cre_dt,	---changed to tywv
		tywv.last_change_date as last_chg_dt,	---changed to tywv
		tywv.last_change_user_name as last_chg_user_nm,	---changed to tywv
		'' as hash_dif,
		'I' as cdc_oper_cd,
		CAST(current_date as date) as load_dt,
		'SQL_FARM_RCD' as data_src_nm,
		CAST((current_date - 1) as date) as cdc_dt
        --tya.tract_year_adjustment_identifier as tr_yr_adj_id,
		--tya.tract_year_adjustment_type_code as tr_yr_adj_type_cd,
		--tya.tract_year_adjustment_reason_code as tr_yr_adj_rsn_cd,
		--tya.after_adjustment_value as aft_adj_val,
		--tya.before_adjustment_value as bef_adj_val,
		--tya.parent_table_physical_name as prnt_tbl_phy_nm,
		--CONCAT_WS('-', c.state_fsa_code, c.county_fsa_code, c.last_assigned_farm_number) as farm_description,
		--c.time_period_identifier as tm_prd_id,
		-- t.location_state_fsa_code as st_fsa_cd,
		-- t.location_county_fsa_code as cnty_fsa_cd,
		--t.tract_description as tr_desc,
from farm_records_reporting.tract_year_wetland_violation tywv
	inner join farm_records_reporting.tract_year ty
		on ty.tract_year_identifier = tywv.tract_year_identifier
	inner join farm_records_reporting.farm_year fy
		on fy.farm_year_identifier = ty.farm_year_identifier
	inner join farm_records_reporting.time_period tp
		on tp.time_period_identifier = fy.time_period_identifier
	inner join farm_records_reporting.tract t
		on t.tract_identifier = ty.tract_identifier
	---inner join farm_records_reporting.tract_year_adjustment tya 	--- confirmed with Mark - not needed
	---	on ty.tract_year_identifier = tya.tract_year_identifier 	---tract_year_adjustment_identifier is PK
	---	
	---inner join farm_records_reporting.tract_year_dcp tyd			--- confirmed with Mark - not needed
	---	on ty.tract_year_identifier = tyd.tract_year_identifier  	---tract_year_dcp_identifier is PK
	inner join farm_records_reporting.farm f
		on fy.farm_identifier = f.farm_identifier
	inner join farm_records_reporting.county_office_control c
		on f.county_office_control_identifier = c.county_office_control_identifier --- county_office_control_identifier is PK
		-- and c.last_assigned_farm_number = cast(f.farm_number as INT)
		-- and (DATE_PART('year', f.creation_date) - 1998) = c.time_period_identifier
where tywv.cdc_dt >= current_date - 1