SELECT distinct 
       c.program_abbreviation as pgm_abr, 
       c.fsa_crop_code as fsa_crop_cd, 
       c.fsa_crop_abbreviation as fsa_crop_abr, 
       c.fsa_crop_name as fsa_crop_nm, 
       c.fsa_crop_type_name as fsa_crop_type_nm, 
       c.fsa_crop_type_code as fsa_crop_type_cd, 
       c.display_sequence_number as dply_seq_nbr,
        cfyd.farm_year_dcp_identifier as farm_yr_dcp_id,
       -- cfyd.crop_identifier as crop_id,
	   -- c.crop_identifier as crop_id, 
       -- cfyd.crp_reduction_acreage as crp_rdn_acrg,
       -- cfyd.crp_released_acreage as crp_rel_acrg,
       -- cfyd.ccp_payment_yield as ccp_pymt_yld,
       -- cfyd.direct_payment_yield as dir_pymt_yld, 
       -- cfyd. as pgm_abr,
       -- cfyd. as fsa_crop_cd,
       -- cfyd. as fsa_crop_type_cd,
       f.farm_number as farm_nbr,
       co.state_fsa_code as st_fsa_cd, 
       co.county_fsa_code as cnty_fsa_cd,
       CASE
		   WHEN EXTRACT(MONTH FROM CURRENT_DATE) < 10 THEN EXTRACT(YEAR FROM CURRENT_DATE)
		   ELSE EXTRACT(YEAR FROM CURRENT_DATE) + 1
	   END AS pgm_yr,
       ctyd.data_status_code as data_stat_cd, 
       ctyd.creation_date as cre_dt, 
       ctyd.last_change_date as last_chg_dt, 
       ctyd.last_change_user_name as last_chg_user_nm,
       '' as hash_dif,
       'I' as cdc_oper_cd,
       CURRENT_DATE as load_dt,
       'SQL_FARM_RCD' as data_src_nm,
       CAST((current_date - 1) as date) as cdc_dt,
	   CONCAT_WS('-', co.state_fsa_code, co.county_fsa_code, co.last_assigned_farm_number) as farm_description,
	   t.tract_number as tr_nbr,
	   cfyd.crop_farm_year_dcp_identifier as crop_farm_yr_dcp_id,
	   ctc.crop_tract_contract_identifier as crop_tr_ctr_id,
	   t.tract_identifier as tr_id,
	   -- ctc.crop_identifier as crop_id,
	   ctc.contract_number  as ctr_nbr,
	   ctc.reduction_acreage_start_year  as rdn_acrg_strt_yr,
	   ctc.reduction_acreage  as rdn_acrg,
	   ctc.crop_program_payment_yield as crop_pgm_pymt_yld,
	   ctc.crop_program_alternate_payment_yield  as crop_pgm_alt_pymt_yld,
	   ctc.tract_year_identifier  as tr_yr_id,
	   ctyd.crop_tract_year_dcp_identifier as crop_tr_yr_dcp_id,
	   ctyd.tract_year_dcp_identifier as tr_yr_dcp_id,
       ctyd.crop_identifier as crop_id,
       ctyd.crp_reduction_acreage as crp_rdn_acrg,
       ctyd.crp_released_acreage as crp_rel_acrg,
	   ctyd.dcp_crop_base_acreage as dcp_crop_base_acrg,
       ctyd.fav_reduction_acreage as fav_rdn_acrg,
       ctyd.ccp_payment_yield as ccp_pymt_yld,
       ctyd.crp_payment_yield as crp_pymt_yld,
	   ctyd.direct_payment_yield as dir_pymt_yld,
       ctyd.fav_direct_payment_yield as fav_dir_pymt_yld,
	   ctyd.fav_ccp_payment_yield as fav_ccp_pymt_yld,
	   ctda.crop_tract_year_dcp_adjustment_identifier as crop_tr_yr_dcp_adj_id,
	   ctda.dcp_adjustment_type_code as dcp_adj_type_cd,
	   ctda.dcp_adjustment_reason_code as dcp_adj_rsn_cd,
	   ctda.after_adjustment_value as aft_adj_val,
	   ctda.before_adjustment_value as bef_adj_val 
FROM farm_records_reporting.crop c 
inner join farm_records_reporting.crop_farm_year_dcp cfyd
	on c.crop_identifier = cfyd.crop_identifier
inner join farm_records_reporting.farm_year_dcp fyd
	on fyd.farm_year_dcp_identifier = cfyd.farm_year_dcp_identifier
inner join farm_records_reporting.farm_year fy
	on fyd.farm_year_identifier = fy.farm_year_identifier
inner join farm_records_reporting.farm f
	on f.farm_identifier = fy.farm_identifier
inner join farm_records_reporting.county_office_control co
	on co.county_office_control_identifier = f.county_office_control_identifier
inner join farm_records_reporting.tract_year ty
	on fy.farm_year_identifier = ty.farm_year_identifier	
inner join farm_records_reporting.tract t
	on t.tract_identifier = ty.tract_identifier
	and f.county_office_control_identifier = t.county_office_control_identifier
inner join farm_records_reporting.crop_tract_contract ctc
	on c.crop_identifier = ctc.crop_identifier
	-- and ty.tract_year_identifier = ctc.tract_year_identifier
inner join farm_records_reporting.tract_year_dcp tyd
	on tyd.tract_year_identifier = ty.tract_year_identifier
inner join farm_records_reporting.crop_tract_year_dcp ctyd
	on c.crop_identifier = ctyd.crop_identifier	
	-- on tyd.tract_year_dcp_identifier = ctyd.tract_year_dcp_identifier
	-- and ty.tract_year_identifier = ctc.tract_year_identifier
inner join farm_records_reporting.crop_tract_year_dcp_adjustment ctda
	on ctda.crop_tract_year_dcp_identifier = ctyd.crop_tract_year_dcp_identifier