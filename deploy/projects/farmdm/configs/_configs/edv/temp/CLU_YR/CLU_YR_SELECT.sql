SELECT distinct 
	cy.clu_year_identifier as clu_yr_id, 
	cy.tract_year_identifier as tr_yr_id, 
	cy.clu_alternate_identifier as clu_alt_id, 
	cy.field_number as fld_nbr, 
	cy.location_county_fsa_code as loc_cnty_fsa_cd, 
	cy.location_state_fsa_code as loc_st_fsa_cd, 
	cy.state_ansi_code as st_ansi_cd, 
	cy.county_ansi_code as cnty_ansi_cd, 
	cy.congressional_district_code as cong_dist_cd, 
	cy.land_classification_identifier as land_cls_id, 
	cy.clu_acreage as clu_acrg, 
	cy.hel_status_identifier as hel_stat_id, 
	cy.cropland_indicator_3cm as cpld_ind_3cm, 
	cy.clu_description as clu_desc, 
	cy.crp_contract_number as crp_ctr_nbr, 
	cy.crp_contract_expiration_date as crp_ctr_expr_dt, 
	cy.conservation_practice_identifier as cnsv_prac_id, 
	cy.native_sod_conversion_date as ntv_sod_cvsn_dt, 
	cy.sod_conversion_crop_year_1 as sod_cvsn_crop_yr_1, 
	cy.sod_conversion_crop_year_2 as sod_cvsn_crop_yr_2, 
	cy.sod_conversion_crop_year_3 as sod_cvsn_crop_yr_3, 
	cy.sod_conversion_crop_year_4 as sod_cvsn_crop_yr_4, 
	f.farm_number as farm_nbr, 
	t.tract_number as tr_nbr, 
	t.location_state_fsa_code as st_fsa_cd, 
	t.location_county_fsa_code as cnty_fsa_cd, 
	fy.time_period_identifier + 1998  as pgm_yr, 
	cy.data_status_code as data_stat_cd, 
	cy.creation_date as cre_dt, 
	cy.last_change_date as last_chg_dt, 
	cy.last_change_user_name as last_chg_user_nm, 
	'' as hash_dif,
	'I' as cdc_oper_cd,
	CAST(current_date as date) as load_dt,
	'SQL_FARM_RCD' as data_src_nm,
	CAST((current_date - 1) as date) as cdc_dt
FROM farm_records_reporting.clu_year cy
inner join farm_records_reporting.tract_year ty
	ON CY.tract_year_identifier = TY.tract_year_identifier
inner join farm_records_reporting.tract t
	on ty.tract_identifier = T.tract_identifier
inner join farm_records_reporting.farm_year fy
	on fy.farm_year_identifier = ty.farm_year_identifier
inner join farm_records_reporting.farm f
	on f.farm_identifier = fy.farm_identifier 






