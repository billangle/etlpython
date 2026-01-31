select  
        t.tract_number  as tr_nbr,
		-- ty.ewp_acreage as ewp_tr_acrg,
		ty.tract_year_identifier as tr_yr_id,
        tya.tract_year_adjustment_identifier as tr_yr_adj_id,
		tya.tract_year_adjustment_type_code as tr_yr_adj_type_cd,
		tya.tract_year_adjustment_reason_code as tr_yr_adj_rsn_cd,
		tya.after_adjustment_value as aft_adj_val,
		tya.before_adjustment_value as bef_adj_val,
		tya.parent_table_physical_name as prnt_tbl_phy_nm,
		f.farm_number as farm_nbr,
		CONCAT_WS('-', c.state_fsa_code, c.county_fsa_code, c.last_assigned_farm_number) as farm_description,
		c.time_period_identifier as tm_prd_id,
        c.state_fsa_code as st_fsa_cd, 
        c.county_fsa_code as cnty_fsa_cd,
		-- t.location_state_fsa_code as st_fsa_cd,
		-- t.location_county_fsa_code as cnty_fsa_cd,
		'' as pgm_yr,
		t.tract_description as tr_desc,
        tya.data_status_code as data_stat_cd,
        tya.creation_date as cre_dt,
        tya.last_change_date as last_chg_dt,
        tya.last_change_user_name as last_chg_user_nm,
		tywv.tract_year_wetland_violation_identifier as tr_yr_wl_vlt_id,
		tywv.wetland_violation_type_code as wl_vlt_type_cd,
		'I' as cdc_oper_cd,
        CAST(current_date as date) as load_dt,
        'SQL_FARM_RCD' as data_src_nm,
        CAST((current_date - 1) as date) as cdc_dt
from farm_records_reporting.tract_year ty
inner join farm_records_reporting.tract t
	on t.tract_identifier = ty.tract_identifier
	-- and c.county_office_control_identifier = t.county_office_control_identifier
inner join farm_records_reporting.tract_year_adjustment tya
    on ty.tract_year_identifier = tya.tract_year_identifier
	-- 	on tya.tract_year_dcp_identifier = tyd.tract_year_dcp_identifier
inner join farm_records_reporting.tract_year_dcp tyd
 	on tyd.tract_year_identifier = ty.tract_year_identifier
	and tyd.tract_year_dcp_identifier = tya.tract_year_dcp_identifier
inner join farm_records_reporting.farm_year fy
	on fy.farm_year_identifier = ty.farm_year_identifier
inner join farm_records_reporting.farm f
	on f.farm_identifier = fy.farm_identifier
inner join farm_records_reporting.county_office_control c
	on f.county_office_control_identifier = c.county_office_control_identifier
	and c.last_assigned_farm_number = cast(f.farm_number as INT)
	-- and (DATE_PART('year', f.creation_date) - 1998) = c.time_period_identifier
inner join farm_records_reporting.tract_year_wetland_violation tywv
	on ty.tract_year_identifier = tywv.tract_year_identifier
	
