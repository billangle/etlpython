SELECT distinct 
       c.program_abbreviation as pgm_abr, 
       c.fsa_crop_code as fsa_crop_cd, 
       c.fsa_crop_abbreviation as fsa_crop_abr, 
       c.fsa_crop_name as fsa_crop_nm, 
       c.fsa_crop_type_name as fsa_crop_type_nm, 
       c.fsa_crop_type_code as fsa_crop_type_cd, 
       c.display_sequence_number as dply_seq_nbr,
        cfyd.farm_year_dcp_identifier as farm_yr_dcp_id,
       cfyd.crop_identifier as crop_id,
	   -- c.crop_identifier as crop_id, 
       cfyd.crp_reduction_acreage as crp_rdn_acrg,
       cfyd.crp_released_acreage as crp_rel_acrg,
       cfyd.ccp_payment_yield as ccp_pymt_yld,
       cfyd.direct_payment_yield as dir_pymt_yld, 
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
       cfyd.data_status_code as data_stat_cd, 
       cfyd.creation_date as cre_dt, 
       cfyd.last_change_date as last_chg_dt, 
       cfyd.last_change_user_name as last_chg_user_nm,
       '' as hash_dif,
       'I' as cdc_oper_cd,
       CAST(current_date as date) as load_dt,
       'SQL_FARM_RCD' as data_src_nm,
       CAST((current_date - 1) as date) as cdc_dt,
	   CONCAT_WS('-', co.state_fsa_code, co.county_fsa_code, co.last_assigned_farm_number) as farm_description,
	   t.tract_number  as tr_nbr,
	   cfyd.crop_farm_year_dcp_identifier as crop_farm_yr_dcp_id
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
	
	
	
	