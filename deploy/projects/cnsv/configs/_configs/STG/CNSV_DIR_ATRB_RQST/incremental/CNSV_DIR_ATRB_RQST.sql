-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_DIR_ATRB_RQST (incremental)
-- Location: s3://c108-cert-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_DIR_ATRB_RQST/incremental/CNSV_DIR_ATRB_RQST.sql
-- =============================================================================

select * from
(
	select	distinct 
			pymt_isu_yr,
			st_fsa_cd,
			cnty_fsa_cd,
			sgnp_sub_cat_nm,
			acct_pgm_cd,
			cnsv_pymt_type,
			cnsv_ctr_nbr,
			core_cust_id,
			dir_atrb_rqst_id,
			acct_ref_1_nbr,
			acct_ref_4_nbr,
			hrch_lvl_nm,
			dir_atrb_cnfrm_nbr,
			fscl_yr,
			prps_pymt_id,
			est_pymt_amt,
			app_agi_rule_ind,
			pymt_lmt_ind,
			note_txt,
			tot_rdn_amt,
			cnsv_ctr_sfx_cd,
			data_stat_cd,
			cre_dt,
			last_chg_dt,
			last_chg_user_nm,
			cdc_oper_cd,
			load_dt,
			data_src_nm,
			cdc_dt,
			row_number() over ( partition by 
			dir_atrb_rqst_id
			order by tbl_priority asc, last_chg_dt desc ) as row_num_part
		from
		(
			select 
				proposed_payment.pymt_isu_yr pymt_isu_yr,
				proposed_payment.st_fsa_cd st_fsa_cd,
				proposed_payment.cnty_fsa_cd cnty_fsa_cd,
				(case 
				when  payment_type.app_cd in ( 'fcrp', 'bcap') then  ewt14sgnp.sgnp_stype_desc 
				when payment_type.app_cd =  'crp' then signup_sub_category.signup_sub_category_name 
				end)  sgnp_sub_cat_nm,
				payment_type.acct_pgm_cd acct_pgm_cd,
				payment_type.cnsv_pymt_type cnsv_pymt_type,
				-- spark sql convertion by JLU 
				left(proposed_payment.acct_ref_1_nbr,regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',0)-1) cnsv_ctr_nbr, 
				entity.core_cust_id core_cust_id,
				direct_attribution_request.dir_atrb_rqst_id dir_atrb_rqst_id,
				proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
				proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
				program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
				direct_attribution_request.dir_atrb_cnfrm_nbr dir_atrb_cnfrm_nbr,
				direct_attribution_request.fscl_yr fscl_yr,
				direct_attribution_request.prps_pymt_id prps_pymt_id,
				direct_attribution_request.est_pymt_amt est_pymt_amt,
				direct_attribution_request.app_agi_rule_ind app_agi_rule_ind,
				direct_attribution_request.pymt_lmt_ind pymt_lmt_ind,
				direct_attribution_request.note_txt note_txt,
				direct_attribution_request.tot_rdn_amt tot_rdn_amt,
				(case 
				-- spark sql convertion by JLU 
				when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\w+)',1),proposed_payment.acct_ref_1_nbr)  =  1 
				then null 
				-- spark sql convertion by JLU 
				when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
				then null 
				else 
				-- spark sql convertion by JLU 
				rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
				end) cnsv_ctr_sfx_cd,
				direct_attribution_request.data_stat_cd data_stat_cd,
				direct_attribution_request.cre_dt cre_dt,
				direct_attribution_request.last_chg_dt last_chg_dt,
				direct_attribution_request.last_chg_user_nm last_chg_user_nm,
				direct_attribution_request.op as cdc_oper_cd,
				current_timestamp() as load_dt,
				'CNSV_DIR_ATRB_RQST' as data_src_nm,
				'{etl_start_date}' as cdc_dt,
				1 as tbl_priority 
				from `fsa-{env}-cnsv-cdc`.`direct_attribution_request`    
				 left join `fsa-{env}-cnsv`.`proposed_payment` 
				   on (direct_attribution_request.prps_pymt_id = proposed_payment.prps_pymt_id) 
				 left join `fsa-{env}-cnsv`.`payment_type` 
				   on (proposed_payment.pymt_type_id = payment_type.pymt_type_id) 
				 left join `fsa-{env}-cnsv`.`pymt_impl` 
				   on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id) 
				 left join `fsa-{env}-cnsv`.`program_hierarchy_level` 
				   on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id) 
				 left join  
				(select distinct 
				 ltrim(rtrim(concat(cast(master_contract.contract_number as varchar(10)),contract_detail.contract_suffix_number))) as cat,  
				 master_contract.signup_identifier, 
				 master_contract.administrative_county_fsa_code, 
				 master_contract.administrative_state_fsa_code   
				 from ( select * from  `fsa-{env}-ccms`.`master_contract` where master_contract.supplemental_contract_type_abbreviation is null ) master_contract 
				  left join `fsa-{env}-ccms`.`contract_detail` 
				   on (contract_detail.contract_identifier = master_contract.contract_identifier) 
				) as temp 
				on ( temp.administrative_county_fsa_code = proposed_payment.cnty_fsa_cd 
				and temp.administrative_state_fsa_code = proposed_payment.st_fsa_cd  
				and temp.cat = ltrim(rtrim(proposed_payment.acct_ref_1_nbr))) 
				left join `fsa-{env}-ccms`.`signup`  
				on (temp.signup_identifier = signup.signup_identifier) 
				left join `fsa-{env}-ccms`.`signup_sub_category` 
				on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code) 
				left join ( select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where cnsv_ctr_seq_nbr > 0 ) ewt40ofrsc 
				on ( proposed_payment.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd 
				and proposed_payment.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd 
				and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) 
				) 
				left join `fsa-{env}-cnsv`.`ewt14sgnp` 
				on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
				left join ( select distinct prps_pymt_id,core_cust_id from `fsa-{env}-cnsv`.`entity` where  entity.prnt_enty_id is null) entity 
				on (direct_attribution_request.prps_pymt_id = entity.prps_pymt_id 
				and proposed_payment.prps_pymt_id = entity.prps_pymt_id) 
				where direct_attribution_request.op <> 'D' 
				AND direct_attribution_request.dart_filedate BETWEEN '{etl_start_date}' AND '{etl_end_date}' 

			union 
			select  
				proposed_payment.pymt_isu_yr pymt_isu_yr, 
				proposed_payment.st_fsa_cd st_fsa_cd,
				proposed_payment.cnty_fsa_cd cnty_fsa_cd,
				(case  
				when  payment_type.app_cd in ( 'fcrp', 'bcap') then  ewt14sgnp.sgnp_stype_desc 
				when payment_type.app_cd =  'crp' then signup_sub_category.signup_sub_category_name   
				end)  sgnp_sub_cat_nm,
				payment_type.acct_pgm_cd acct_pgm_cd,
				payment_type.cnsv_pymt_type cnsv_pymt_type,
				-- JLU update for spark SQL 
				left( proposed_payment.acct_ref_1_nbr,regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',0)-1) cnsv_ctr_nbr,  
				entity.core_cust_id core_cust_id,
				direct_attribution_request.dir_atrb_rqst_id dir_atrb_rqst_id,
				proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
				proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
				program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
				direct_attribution_request.dir_atrb_cnfrm_nbr dir_atrb_cnfrm_nbr,
				direct_attribution_request.fscl_yr fscl_yr,
				direct_attribution_request.prps_pymt_id prps_pymt_id,
				direct_attribution_request.est_pymt_amt est_pymt_amt,
				direct_attribution_request.app_agi_rule_ind app_agi_rule_ind,
				direct_attribution_request.pymt_lmt_ind pymt_lmt_ind,
				direct_attribution_request.note_txt note_txt,
				direct_attribution_request.tot_rdn_amt tot_rdn_amt,
				(case 
				-- JLU update for spark SQL 
				when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\w+)',1),proposed_payment.acct_ref_1_nbr)  =  1 
				then null 
				-- JLU update for spark SQL  
				when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
				then null 
				else 
				-- JLU update for spark SQL 
				rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
				end) cnsv_ctr_sfx_cd,
				direct_attribution_request.data_stat_cd data_stat_cd,
				direct_attribution_request.cre_dt cre_dt,
				direct_attribution_request.last_chg_dt last_chg_dt,
				direct_attribution_request.last_chg_user_nm last_chg_user_nm,
				direct_attribution_request.op as cdc_oper_cd,
				current_timestamp() as load_dt,
				'CNSV_DIR_ATRB_RQST' as data_src_nm,
				'{etl_start_date}' as cdc_dt,
				2 as tbl_priority 
				from `fsa-{env}-cnsv`.`proposed_payment`   
				join `fsa-{env}-cnsv-cdc`.`direct_attribution_request`  
				on (direct_attribution_request.prps_pymt_id = proposed_payment.prps_pymt_id) 
				left join `fsa-{env}-cnsv`.`payment_type` 
				 on (proposed_payment.pymt_type_id = payment_type.pymt_type_id) 
				left join `fsa-{env}-cnsv`.`pymt_impl` 
				on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id) 
				left join `fsa-{env}-cnsv`.`program_hierarchy_level` 
				 on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id) 
				left join  
				(select distinct 
				ltrim(rtrim(concat(cast(master_contract.contract_number as varchar(10)),contract_detail.contract_suffix_number))) as cat,   
				master_contract.signup_identifier, 
				master_contract.administrative_county_fsa_code, 
				master_contract.administrative_state_fsa_code  
				from ( select * from  `fsa-{env}-ccms`.`master_contract` where master_contract.supplemental_contract_type_abbreviation is null ) master_contract 
				left join `fsa-{env}-ccms`.`contract_detail` 
				on (contract_detail.contract_identifier = master_contract.contract_identifier) 
				) as temp 
				on ( temp.administrative_county_fsa_code = proposed_payment.cnty_fsa_cd 
				and temp.administrative_state_fsa_code = proposed_payment.st_fsa_cd 
				and temp.cat = ltrim(rtrim(proposed_payment.acct_ref_1_nbr)))   
				left join `fsa-{env}-ccms`.`signup`  
				on (temp.signup_identifier = signup.signup_identifier) 
				left join `fsa-{env}-ccms`.`signup_sub_category` 
				on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code) 
				left join ( select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where cnsv_ctr_seq_nbr > 0 ) ewt40ofrsc 
				on ( proposed_payment.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd 
				and proposed_payment.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd 
				and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) 
				) 
				left join `fsa-{env}-cnsv`.`ewt14sgnp` 
				on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
				left join ( select distinct prps_pymt_id,core_cust_id from `fsa-{env}-cnsv`.`entity` where  entity.prnt_enty_id is null) entity 
				on (direct_attribution_request.prps_pymt_id = entity.prps_pymt_id 
				and proposed_payment.prps_pymt_id = entity.prps_pymt_id) 
				where direct_attribution_request.op <> 'D'

			union 
			select  
				proposed_payment.pymt_isu_yr pymt_isu_yr,
				proposed_payment.st_fsa_cd st_fsa_cd,
				proposed_payment.cnty_fsa_cd cnty_fsa_cd,
				(case 
				when  payment_type.app_cd in ( 'fcrp', 'bcap') then  ewt14sgnp.sgnp_stype_desc 
				when payment_type.app_cd =  'crp' then signup_sub_category.signup_sub_category_name 
				end)  sgnp_sub_cat_nm,
				payment_type.acct_pgm_cd acct_pgm_cd,
				payment_type.cnsv_pymt_type cnsv_pymt_type,
				 -- JLU update for spark SQL 
				left(proposed_payment.acct_ref_1_nbr,regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',0)-1) cnsv_ctr_nbr, 
				entity.core_cust_id core_cust_id,
				direct_attribution_request.dir_atrb_rqst_id dir_atrb_rqst_id,
				proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
				proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
				program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
				direct_attribution_request.dir_atrb_cnfrm_nbr dir_atrb_cnfrm_nbr,
				direct_attribution_request.fscl_yr fscl_yr,
				direct_attribution_request.prps_pymt_id prps_pymt_id,
				direct_attribution_request.est_pymt_amt est_pymt_amt,
				direct_attribution_request.app_agi_rule_ind app_agi_rule_ind,
				direct_attribution_request.pymt_lmt_ind pymt_lmt_ind,
				direct_attribution_request.note_txt note_txt,
				direct_attribution_request.tot_rdn_amt tot_rdn_amt,
				(case 
				-- JLU update for spark SQL 
				when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\w+)',1),proposed_payment.acct_ref_1_nbr)  =  1 
				then null 
				-- JLU update for spark SQL 
				when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
				then null 
				else 
				 -- JLU update for spark SQL 
				rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
				end) cnsv_ctr_sfx_cd,
				direct_attribution_request.data_stat_cd data_stat_cd, 
				direct_attribution_request.cre_dt cre_dt,
				direct_attribution_request.last_chg_dt last_chg_dt,
				direct_attribution_request.last_chg_user_nm last_chg_user_nm,
				direct_attribution_request.op as cdc_oper_cd,
				current_timestamp() as load_dt,
				'CNSV_DIR_ATRB_RQST' as data_src_nm,
				'{etl_start_date}' as cdc_dt,
				3 as tbl_priority 
				from  `fsa-{env}-cnsv`.`payment_type`    
				left join `fsa-{env}-cnsv`.`proposed_payment` 
				 on (proposed_payment.pymt_type_id = payment_type.pymt_type_id) 
				join `fsa-{env}-cnsv-cdc`.`direct_attribution_request` 
				on (direct_attribution_request.prps_pymt_id = proposed_payment.prps_pymt_id) 
				left join `fsa-{env}-cnsv`.`pymt_impl` 
				on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id) 
				left join `fsa-{env}-cnsv`.`program_hierarchy_level` 
				 on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id) 
				left join  
				(select distinct 
				ltrim(rtrim(concat(cast(master_contract.contract_number as varchar(10)),contract_detail.contract_suffix_number))) as cat,   
				master_contract.signup_identifier, 
				master_contract.administrative_county_fsa_code, 
				master_contract.administrative_state_fsa_code  
				from ( select * from  `fsa-{env}-ccms`.`master_contract` where master_contract.supplemental_contract_type_abbreviation is null ) master_contract 
				left join `fsa-{env}-ccms`.`contract_detail` 
				on (contract_detail.contract_identifier = master_contract.contract_identifier) 
				) as temp 
				on ( temp.administrative_county_fsa_code = proposed_payment.cnty_fsa_cd 
				and temp.administrative_state_fsa_code = proposed_payment.st_fsa_cd 
				and temp.cat = ltrim(rtrim(proposed_payment.acct_ref_1_nbr)))  
				left join `fsa-{env}-ccms`.`signup`  
				on (temp.signup_identifier = signup.signup_identifier) 
				left join `fsa-{env}-ccms`.`signup_sub_category` 
				on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code) 
				left join ( select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where cnsv_ctr_seq_nbr > 0 ) ewt40ofrsc 
				on ( proposed_payment.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd 
				and proposed_payment.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd 
				and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) 
				)  
				left join `fsa-{env}-cnsv`.`ewt14sgnp` 
				on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
				left join ( select distinct prps_pymt_id,core_cust_id from `fsa-{env}-cnsv`.`entity` where  entity.prnt_enty_id is null) entity 
				on (direct_attribution_request.prps_pymt_id = entity.prps_pymt_id 
				and proposed_payment.prps_pymt_id = entity.prps_pymt_id) 
				where direct_attribution_request.op <> 'D' 

			union 
			select  
				proposed_payment.pymt_isu_yr pymt_isu_yr,
				proposed_payment.st_fsa_cd st_fsa_cd,
				proposed_payment.cnty_fsa_cd cnty_fsa_cd,
				(case 
				when  payment_type.app_cd in ( 'fcrp', 'bcap') then  ewt14sgnp.sgnp_stype_desc 
				when payment_type.app_cd =  'crp' then signup_sub_category.signup_sub_category_name 
				end)  sgnp_sub_cat_nm,
				payment_type.acct_pgm_cd acct_pgm_cd,
				payment_type.cnsv_pymt_type cnsv_pymt_type,
				-- JLU convert to Spark SQL
				left(proposed_payment.acct_ref_1_nbr,regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',0)-1) cnsv_ctr_nbr, 
				entity.core_cust_id core_cust_id,
				direct_attribution_request.dir_atrb_rqst_id dir_atrb_rqst_id,
				proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
				proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
				program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
				direct_attribution_request.dir_atrb_cnfrm_nbr dir_atrb_cnfrm_nbr,
				direct_attribution_request.fscl_yr fscl_yr,
				direct_attribution_request.prps_pymt_id prps_pymt_id,
				direct_attribution_request.est_pymt_amt est_pymt_amt,
				direct_attribution_request.app_agi_rule_ind app_agi_rule_ind,
				direct_attribution_request.pymt_lmt_ind pymt_lmt_ind,
				direct_attribution_request.note_txt note_txt,
				direct_attribution_request.tot_rdn_amt tot_rdn_amt,
				(case 
				-- JLU update for spark SQL 
				when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\w+)',1),proposed_payment.acct_ref_1_nbr)  =  1 
				then null 
				-- JLU update for spark SQL 
				when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
				then null 
				else 
				-- JLU update for spark SQL 
				rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
				end) cnsv_ctr_sfx_cd,
				direct_attribution_request.data_stat_cd data_stat_cd,
				direct_attribution_request.cre_dt cre_dt,
				direct_attribution_request.last_chg_dt last_chg_dt,
				direct_attribution_request.last_chg_user_nm last_chg_user_nm,
				direct_attribution_request.op as cdc_oper_cd,
				current_timestamp() as load_dt,
				'CNSV_DIR_ATRB_RQST' as data_src_nm,
				'{etl_start_date}' as cdc_dt,
				4 as tbl_priority  
				from  `fsa-{env}-cnsv`.`pymt_impl`     
				left join `fsa-{env}-cnsv`.`payment_type` 
				on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id) 
				left join `fsa-{env}-cnsv`.`proposed_payment` 
				on (proposed_payment.pymt_type_id = payment_type.pymt_type_id)  
				join `fsa-{env}-cnsv-cdc`.`direct_attribution_request` 
				on (direct_attribution_request.prps_pymt_id = proposed_payment.prps_pymt_id) 
				left join `fsa-{env}-cnsv`.`program_hierarchy_level` 
				on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id) 
				left join  
				(select distinct 
				ltrim(rtrim(concat(cast(master_contract.contract_number as varchar(10)),contract_detail.contract_suffix_number))) as cat,   
				master_contract.signup_identifier, 
				master_contract.administrative_county_fsa_code, 
				master_contract.administrative_state_fsa_code  
				from ( select * from  `fsa-{env}-ccms`.`master_contract` where master_contract.supplemental_contract_type_abbreviation is null ) master_contract 
				left join `fsa-{env}-ccms`.`contract_detail` 
				on (contract_detail.contract_identifier = master_contract.contract_identifier) 
				) as temp 
				on ( temp.administrative_county_fsa_code = proposed_payment.cnty_fsa_cd 
				and temp.administrative_state_fsa_code = proposed_payment.st_fsa_cd 
				and temp.cat = ltrim(rtrim(proposed_payment.acct_ref_1_nbr)))  
				left join `fsa-{env}-ccms`.`signup`  
				on (temp.signup_identifier = signup.signup_identifier) 
				left join `fsa-{env}-ccms`.`signup_sub_category` 
				on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code) 
				left join ( select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where cnsv_ctr_seq_nbr > 0 ) ewt40ofrsc 
				on ( proposed_payment.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd 
				and proposed_payment.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd 
				and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) 
				)  
				left join `fsa-{env}-cnsv`.`ewt14sgnp` 
				on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)  
				left join ( select distinct prps_pymt_id,core_cust_id from `fsa-{env}-cnsv`.`entity` where  entity.prnt_enty_id is null) entity 
				on (direct_attribution_request.prps_pymt_id = entity.prps_pymt_id 
				and proposed_payment.prps_pymt_id = entity.prps_pymt_id) 
				where direct_attribution_request.op <> 'D' 

			union 
			select  
				proposed_payment.pymt_isu_yr pymt_isu_yr,
				proposed_payment.st_fsa_cd st_fsa_cd,
				proposed_payment.cnty_fsa_cd cnty_fsa_cd,
				(case 
				when  payment_type.app_cd in ( 'fcrp', 'bcap') then  ewt14sgnp.sgnp_stype_desc 
				when payment_type.app_cd =  'crp' then signup_sub_category.signup_sub_category_name 
				end)  sgnp_sub_cat_nm,
				payment_type.acct_pgm_cd acct_pgm_cd,
				payment_type.cnsv_pymt_type cnsv_pymt_type,
				-- JLU update for spark SQL 
				left(proposed_payment.acct_ref_1_nbr,regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',0)-1) cnsv_ctr_nbr, 
				entity.core_cust_id core_cust_id,
				direct_attribution_request.dir_atrb_rqst_id dir_atrb_rqst_id,
				proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
				proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
				program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
				direct_attribution_request.dir_atrb_cnfrm_nbr dir_atrb_cnfrm_nbr,
				direct_attribution_request.fscl_yr fscl_yr,
				direct_attribution_request.prps_pymt_id prps_pymt_id,
				direct_attribution_request.est_pymt_amt est_pymt_amt,
				direct_attribution_request.app_agi_rule_ind app_agi_rule_ind,
				direct_attribution_request.pymt_lmt_ind pymt_lmt_ind,
				direct_attribution_request.note_txt note_txt,
				direct_attribution_request.tot_rdn_amt tot_rdn_amt,
				(case 
				-- JLU update for spark SQL 
				when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\w+)',1),proposed_payment.acct_ref_1_nbr)  =  1 
				then null 
				-- JLU update for spark SQL. 
				when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
				then null 
				else 
				-- JLU update for spark SQL 
				rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
				end) cnsv_ctr_sfx_cd,
				direct_attribution_request.data_stat_cd data_stat_cd,
				direct_attribution_request.cre_dt cre_dt,
				direct_attribution_request.last_chg_dt last_chg_dt,
				direct_attribution_request.last_chg_user_nm last_chg_user_nm,
				direct_attribution_request.op as cdc_oper_cd,
				current_timestamp() as load_dt,
				'CNSV_DIR_ATRB_RQST' as data_src_nm,
				'{etl_start_date}' as cdc_dt,
				5 as tbl_priority 
				from `fsa-{env}-cnsv`.`program_hierarchy_level`     
				left join `fsa-{env}-cnsv`.`pymt_impl` 
				on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id) 
				left join `fsa-{env}-cnsv`.`payment_type` 
				on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id) 
				left join `fsa-{env}-cnsv`.`proposed_payment`  
				on (proposed_payment.pymt_type_id = payment_type.pymt_type_id) 
				join  `fsa-{env}-cnsv-cdc`.`direct_attribution_request` 
				on (direct_attribution_request.prps_pymt_id = proposed_payment.prps_pymt_id) 
				left join  
				(select distinct 
				ltrim(rtrim(concat(cast(master_contract.contract_number as varchar(10)),contract_detail.contract_suffix_number))) as cat,   
				master_contract.signup_identifier,  
				master_contract.administrative_county_fsa_code, 
				master_contract.administrative_state_fsa_code  
				from ( select * from  `fsa-{env}-ccms`.`master_contract` where master_contract.supplemental_contract_type_abbreviation is null ) master_contract  
				left join `fsa-{env}-ccms`.`contract_detail`
				on (contract_detail.contract_identifier = master_contract.contract_identifier) 
				) as temp 
				on ( temp.administrative_county_fsa_code = proposed_payment.cnty_fsa_cd 
				and temp.administrative_state_fsa_code = proposed_payment.st_fsa_cd 
				and temp.cat = ltrim(rtrim(proposed_payment.acct_ref_1_nbr)))   
				left join `fsa-{env}-ccms`.`signup`  
				on (temp.signup_identifier = signup.signup_identifier) 
				left join `fsa-{env}-ccms`.`signup_sub_category` 
				on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code) 
				left join ( select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where cnsv_ctr_seq_nbr > 0 ) ewt40ofrsc 
				on ( proposed_payment.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd 
				and proposed_payment.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd 
				and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) 
				)  
				left join `fsa-{env}-cnsv`.`ewt14sgnp` 
				on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
				left join ( select distinct prps_pymt_id,core_cust_id from `fsa-{env}-cnsv`.`entity` where  entity.prnt_enty_id is null) entity 
				on (direct_attribution_request.prps_pymt_id = entity.prps_pymt_id  
				and proposed_payment.prps_pymt_id = entity.prps_pymt_id) 
				where direct_attribution_request.op <> 'D' 

			union 
			select  
				proposed_payment.pymt_isu_yr pymt_isu_yr,
				proposed_payment.st_fsa_cd st_fsa_cd,
				proposed_payment.cnty_fsa_cd cnty_fsa_cd,
				(case 
				when  payment_type.app_cd in ( 'fcrp', 'bcap') then  ewt14sgnp.sgnp_stype_desc 
				when payment_type.app_cd =  'crp' then signup_sub_category.signup_sub_category_name 
				end)  sgnp_sub_cat_nm,
				payment_type.acct_pgm_cd acct_pgm_cd,
				payment_type.cnsv_pymt_type cnsv_pymt_type,
				-- JLU update for spark SQL 
				left(proposed_payment.acct_ref_1_nbr,regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',0)-1) cnsv_ctr_nbr, 
				entity.core_cust_id core_cust_id,
				direct_attribution_request.dir_atrb_rqst_id dir_atrb_rqst_id,
				proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
				proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
				program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
				direct_attribution_request.dir_atrb_cnfrm_nbr dir_atrb_cnfrm_nbr,
				direct_attribution_request.fscl_yr fscl_yr,
				direct_attribution_request.prps_pymt_id prps_pymt_id,
				direct_attribution_request.est_pymt_amt est_pymt_amt,
				direct_attribution_request.app_agi_rule_ind app_agi_rule_ind,
				direct_attribution_request.pymt_lmt_ind pymt_lmt_ind,
				direct_attribution_request.note_txt note_txt,
				direct_attribution_request.tot_rdn_amt tot_rdn_amt,
				(case 
				-- JLU update for spark SQL 
				when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\w+)',1),proposed_payment.acct_ref_1_nbr)  =  1 
				then null 
				-- JLU update for spark SQL. 
				when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
				then null 
				else 
				-- JLU update for spark SQL 
				rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
				end) cnsv_ctr_sfx_cd,
				direct_attribution_request.data_stat_cd data_stat_cd,
				direct_attribution_request.cre_dt cre_dt,
				direct_attribution_request.last_chg_dt last_chg_dt,
				direct_attribution_request.last_chg_user_nm last_chg_user_nm,
				direct_attribution_request.op as cdc_oper_cd,
				current_timestamp() as load_dt,
				'CNSV_DIR_ATRB_RQST' as data_src_nm,
				'{etl_start_date}' as cdc_dt,
				6 as tbl_priority 
				from (select distinct 
				ltrim(rtrim(concat(cast(master_contract.contract_number as varchar(10)),contract_detail.contract_suffix_number))) as cat,   
				master_contract.signup_identifier, 
				master_contract.administrative_county_fsa_code, 
				master_contract.administrative_state_fsa_code, 
				direct_attribution_request.op as cdc_oper_cd
				from ( select * from  `fsa-{env}-ccms`.`master_contract` where master_contract.supplemental_contract_type_abbreviation is null ) master_contract 
				left join `fsa-{env}-ccms`.`contract_detail` 
				on (contract_detail.contract_identifier = master_contract.contract_identifier)
				) as temp 
				left join `fsa-{env}-cnsv`.`proposed_payment` 
				on ( temp.administrative_county_fsa_code = proposed_payment.cnty_fsa_cd 
				and temp.administrative_state_fsa_code = proposed_payment.st_fsa_cd 
				and temp.cat = ltrim(rtrim(proposed_payment.acct_ref_1_nbr))) 
				join `fsa-{env}-cnsv-cdc`.`direct_attribution_request` 
				on (direct_attribution_request.prps_pymt_id = proposed_payment.prps_pymt_id) 
				left join `fsa-{env}-cnsv`.`payment_type` 
				on (proposed_payment.pymt_type_id = payment_type.pymt_type_id) 
				left join `fsa-{env}-cnsv`.`pymt_impl`  
				on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id) 
				left join `fsa-{env}-cnsv`.`program_hierarchy_level` 
				on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id) 
				left join `fsa-{env}-ccms`.`signup`  
				on (temp.signup_identifier = signup.signup_identifier) 
				left join `fsa-{env}-ccms`.`signup_sub_category` 
				on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code) 
				left join ( select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where cnsv_ctr_seq_nbr > 0 ) ewt40ofrsc 
				on ( proposed_payment.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd 
				and proposed_payment.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd 
				and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar(10)),ewt40ofrsc.cnsv_ctr_sufx_cd)))
				) 
				left join `fsa-{env}-cnsv`.`ewt14sgnp` 
				on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
				left join ( select distinct prps_pymt_id,core_cust_id from `fsa-{env}-cnsv`.`entity` where  entity.prnt_enty_id is null) entity 
				on (direct_attribution_request.prps_pymt_id = entity.prps_pymt_id 
				and proposed_payment.prps_pymt_id = entity.prps_pymt_id) 
				where direct_attribution_request.op <> 'D'

			union 
			select  
				proposed_payment.pymt_isu_yr pymt_isu_yr,
				proposed_payment.st_fsa_cd st_fsa_cd,
				proposed_payment.cnty_fsa_cd cnty_fsa_cd,
				(case 
				when  payment_type.app_cd in ( 'fcrp', 'bcap') then  ewt14sgnp.sgnp_stype_desc 
				when payment_type.app_cd =  'crp' then signup_sub_category.signup_sub_category_name 
				end)  sgnp_sub_cat_nm,
				payment_type.acct_pgm_cd acct_pgm_cd,
				payment_type.cnsv_pymt_type cnsv_pymt_type,
				-- JLU update for spark SQL. 
				left(proposed_payment.acct_ref_1_nbr,regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',0)-1) cnsv_ctr_nbr, 
				entity.core_cust_id core_cust_id,
				direct_attribution_request.dir_atrb_rqst_id dir_atrb_rqst_id,
				proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
				proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
				program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
				direct_attribution_request.dir_atrb_cnfrm_nbr dir_atrb_cnfrm_nbr,
				direct_attribution_request.fscl_yr fscl_yr,
				direct_attribution_request.prps_pymt_id prps_pymt_id,
				direct_attribution_request.est_pymt_amt est_pymt_amt,
				direct_attribution_request.app_agi_rule_ind app_agi_rule_ind,
				direct_attribution_request.pymt_lmt_ind pymt_lmt_ind,
				direct_attribution_request.note_txt note_txt,
				direct_attribution_request.tot_rdn_amt tot_rdn_amt,
				(case 
				-- JLU update for spark SQL. 
				when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\w+)',1),proposed_payment.acct_ref_1_nbr)  =  1 
				then null 
				-- JLU update for spark SQL 
				when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
				then null 
				else 
				-- JLU update for spark SQL 
				rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
				end) cnsv_ctr_sfx_cd,
				direct_attribution_request.data_stat_cd data_stat_cd,
				direct_attribution_request.cre_dt cre_dt,
				direct_attribution_request.last_chg_dt last_chg_dt,
				direct_attribution_request.last_chg_user_nm last_chg_user_nm,
				direct_attribution_request.op as cdc_oper_cd,
				current_timestamp() as load_dt,
				'CNSV_DIR_ATRB_RQST' as data_src_nm,
				'{etl_start_date}' as cdc_dt,
				7 as tbl_priority 
				from (select distinct 
				ltrim(rtrim(concat(cast(master_contract.contract_number as varchar(10)),contract_detail.contract_suffix_number))) as cat,   
				master_contract.signup_identifier, 
				master_contract.administrative_county_fsa_code, 
				master_contract.administrative_state_fsa_code,
				direct_attribution_request.op
				from `fsa-{env}-ccms`.`contract_detail`     
				left join ( select * from  `fsa-{env}-ccms`.`master_contract` where master_contract.supplemental_contract_type_abbreviation is null ) master_contract 
				on (contract_detail.contract_identifier = master_contract.contract_identifier)
				) as temp 
				left join `fsa-{env}-cnsv`.`proposed_payment` 
				on ( temp.administrative_county_fsa_code = proposed_payment.cnty_fsa_cd 
				and temp.administrative_state_fsa_code = proposed_payment.st_fsa_cd 
				and temp.cat = ltrim(rtrim(proposed_payment.acct_ref_1_nbr)))   
				join `fsa-{env}-cnsv-cdc`.`direct_attribution_request`  
				on (direct_attribution_request.prps_pymt_id = proposed_payment.prps_pymt_id) 
				left join `fsa-{env}-cnsv`.`payment_type`  
				on (proposed_payment.pymt_type_id = payment_type.pymt_type_id) 
				left join `fsa-{env}-cnsv`.`pymt_impl` 
				on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id) 
				left join `fsa-{env}-cnsv`.`program_hierarchy_level` 
				on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)  
				left join `fsa-{env}-ccms`.`signup`  
				on (temp.signup_identifier = signup.signup_identifier)  
				left join `fsa-{env}-ccms`.`signup_sub_category` 
				on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code) 
				left join ( select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where cnsv_ctr_seq_nbr > 0 ) ewt40ofrsc 
				on ( proposed_payment.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd 
				and proposed_payment.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd 
				and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) 
				)  
				left join `fsa-{env}-cnsv`.`ewt14sgnp` 
				on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
				left join ( select distinct prps_pymt_id,core_cust_id from `fsa-{env}-cnsv`.`entity` where  entity.prnt_enty_id is null) entity  
				on (direct_attribution_request.prps_pymt_id = entity.prps_pymt_id 
				and proposed_payment.prps_pymt_id = entity.prps_pymt_id) 
				where direct_attribution_request.op <> 'D' 

			union 
			select  
				proposed_payment.pymt_isu_yr pymt_isu_yr,
				proposed_payment.st_fsa_cd st_fsa_cd,
				proposed_payment.cnty_fsa_cd cnty_fsa_cd,
				(case 
				when  payment_type.app_cd in ( 'fcrp', 'bcap') then  ewt14sgnp.sgnp_stype_desc 
				when payment_type.app_cd =  'crp' then signup_sub_category.signup_sub_category_name 
				end)  sgnp_sub_cat_nm,
				payment_type.acct_pgm_cd acct_pgm_cd,
				payment_type.cnsv_pymt_type cnsv_pymt_type,
				-- JLU update for spark SQL 
				left(proposed_payment.acct_ref_1_nbr,regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',0)-1) cnsv_ctr_nbr, 
				entity.core_cust_id core_cust_id,
				direct_attribution_request.dir_atrb_rqst_id dir_atrb_rqst_id,
				proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
				proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
				program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
				direct_attribution_request.dir_atrb_cnfrm_nbr dir_atrb_cnfrm_nbr,
				direct_attribution_request.fscl_yr fscl_yr,
				direct_attribution_request.prps_pymt_id prps_pymt_id,
				direct_attribution_request.est_pymt_amt est_pymt_amt,
				direct_attribution_request.app_agi_rule_ind app_agi_rule_ind,
				direct_attribution_request.pymt_lmt_ind pymt_lmt_ind,
				direct_attribution_request.note_txt note_txt,
				direct_attribution_request.tot_rdn_amt tot_rdn_amt,
				(case 
				 -- JLU update for spark SQL.   
				when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\w+)',1),proposed_payment.acct_ref_1_nbr)  =  1 
				then null 
				-- JLU update for spark SQL.  
				when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
				then null 
				else 
				-- JLU update for spark SQL 
				rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
				end) cnsv_ctr_sfx_cd,
				direct_attribution_request.data_stat_cd data_stat_cd,
				direct_attribution_request.cre_dt cre_dt,
				direct_attribution_request.last_chg_dt last_chg_dt,
				direct_attribution_request.last_chg_user_nm last_chg_user_nm,
				direct_attribution_request.op as cdc_oper_cd,
				current_timestamp() as load_dt,
				'CNSV_DIR_ATRB_RQST' as data_src_nm,
				'{etl_start_date}' as cdc_dt,
				8 as tbl_priority 
				from   `fsa-{env}-ccms`.`signup`    
				left join (select distinct 
				ltrim(rtrim(concat(cast(master_contract.contract_number as varchar(10)),contract_detail.contract_suffix_number))) as cat,   
				master_contract.signup_identifier, 
				master_contract.administrative_county_fsa_code, 
				master_contract.administrative_state_fsa_code  
				from ( select * from  `fsa-{env}-ccms`.`master_contract` where master_contract.supplemental_contract_type_abbreviation is null ) master_contract 
				left join `fsa-{env}-ccms`.`contract_detail` 
				on (contract_detail.contract_identifier = master_contract.contract_identifier) 
				) as temp  
				on (temp.signup_identifier = signup.signup_identifier) 
				left join `fsa-{env}-cnsv`.`proposed_payment` 
				on ( temp.administrative_county_fsa_code = proposed_payment.cnty_fsa_cd 
				and temp.administrative_state_fsa_code = proposed_payment.st_fsa_cd 
				and temp.cat = ltrim(rtrim(proposed_payment.acct_ref_1_nbr)))   
				join `fsa-{env}-cnsv-cdc`.`direct_attribution_request` 
				on (direct_attribution_request.prps_pymt_id = proposed_payment.prps_pymt_id) 
				left join `fsa-{env}-cnsv`.`payment_type`  
				on (proposed_payment.pymt_type_id = payment_type.pymt_type_id) 
				left join `fsa-{env}-cnsv`.`pymt_impl` 
				on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id) 
				left join `fsa-{env}-cnsv`.`program_hierarchy_level` 
				on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id) 
				left join `fsa-{env}-ccms`.`signup_sub_category`  
				on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code)  
				left join ( select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where cnsv_ctr_seq_nbr > 0 ) ewt40ofrsc 
				on ( proposed_payment.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd 
				and proposed_payment.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd 
				and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) 
				)  
				left join `fsa-{env}-cnsv`.`ewt14sgnp` 
				on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
				left join ( select distinct prps_pymt_id,core_cust_id from `fsa-{env}-cnsv`.`entity` where  entity.prnt_enty_id is null) entity 
				on (direct_attribution_request.prps_pymt_id = entity.prps_pymt_id 
				and proposed_payment.prps_pymt_id = entity.prps_pymt_id) 
				where direct_attribution_request.op <> 'D' 

			union 
			select  
				proposed_payment.pymt_isu_yr pymt_isu_yr,
				proposed_payment.st_fsa_cd st_fsa_cd,
				proposed_payment.cnty_fsa_cd cnty_fsa_cd,
				(case 
				when  payment_type.app_cd in ( 'fcrp', 'bcap') then  ewt14sgnp.sgnp_stype_desc 
				when payment_type.app_cd =  'crp' then signup_sub_category.signup_sub_category_name 
				end)  sgnp_sub_cat_nm,
				payment_type.acct_pgm_cd acct_pgm_cd,
				payment_type.cnsv_pymt_type cnsv_pymt_type,
				 -- JLU update for spark SQL 
				left(proposed_payment.acct_ref_1_nbr,regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',0)-1) cnsv_ctr_nbr, 
				entity.core_cust_id core_cust_id,
				direct_attribution_request.dir_atrb_rqst_id dir_atrb_rqst_id,
				proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
				proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
				program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
				direct_attribution_request.dir_atrb_cnfrm_nbr dir_atrb_cnfrm_nbr,
				direct_attribution_request.fscl_yr fscl_yr,
				direct_attribution_request.prps_pymt_id prps_pymt_id,
				direct_attribution_request.est_pymt_amt est_pymt_amt,
				direct_attribution_request.app_agi_rule_ind app_agi_rule_ind,
				direct_attribution_request.pymt_lmt_ind pymt_lmt_ind,
				direct_attribution_request.note_txt note_txt,
				direct_attribution_request.tot_rdn_amt tot_rdn_amt,
				(case 
				-- JLU update for spark SQL. 
				when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\w+)',1),proposed_payment.acct_ref_1_nbr)  =  1 then null 
				when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 then null 
				else rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
				end) cnsv_ctr_sfx_cd,
				direct_attribution_request.data_stat_cd data_stat_cd,
				direct_attribution_request.cre_dt cre_dt,
				direct_attribution_request.last_chg_dt last_chg_dt,
				direct_attribution_request.last_chg_user_nm last_chg_user_nm,
				direct_attribution_request.op as cdc_oper_cd,
				current_timestamp() as load_dt,
				'CNSV_DIR_ATRB_RQST' as data_src_nm,
				'{etl_start_date}' as cdc_dt,
				9 as tbl_priority 
				from `fsa-{env}-ccms`.`signup_sub_category`   
				left join `fsa-{env}-ccms`.`signup`  
				on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code) 
				left join  
				(select distinct 
				ltrim(rtrim(concat(cast(master_contract.contract_number as varchar(10)),contract_detail.contract_suffix_number))) as cat,   
				master_contract.signup_identifier, 
				master_contract.administrative_county_fsa_code, 
				master_contract.administrative_state_fsa_code  
				from ( select * from  `fsa-{env}-ccms`.`master_contract` where master_contract.supplemental_contract_type_abbreviation is null ) master_contract
				left join `fsa-{env}-ccms`.`contract_detail`
				on (contract_detail.contract_identifier = master_contract.contract_identifier) 
				) as temp 
				on (temp.signup_identifier = signup.signup_identifier) 
				left join `fsa-{env}-cnsv`.`proposed_payment` 
				on ( temp.administrative_county_fsa_code = proposed_payment.cnty_fsa_cd 
				and temp.administrative_state_fsa_code = proposed_payment.st_fsa_cd 
				and temp.cat = ltrim(rtrim(proposed_payment.acct_ref_1_nbr)))  
				join `fsa-{env}-cnsv-cdc`.`direct_attribution_request` 
				on (direct_attribution_request.prps_pymt_id = proposed_payment.prps_pymt_id) 
				left join `fsa-{env}-cnsv`.`payment_type` on (proposed_payment.pymt_type_id = payment_type.pymt_type_id) 
				left join `fsa-{env}-cnsv`.`pymt_impl` on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id) 
				left join `fsa-{env}-cnsv`.`program_hierarchy_level`  on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id) 
				left join ( select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where cnsv_ctr_seq_nbr > 0 ) ewt40ofrsc 
				on ( proposed_payment.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd 
				and proposed_payment.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd                                                              
				and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) 
				)  
				left join `fsa-{env}-cnsv`.`ewt14sgnp` 
				on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
				left join ( select distinct prps_pymt_id,core_cust_id from `fsa-{env}-cnsv`.`entity` where  entity.prnt_enty_id is null) entity 
				on (direct_attribution_request.prps_pymt_id = entity.prps_pymt_id 
				and proposed_payment.prps_pymt_id = entity.prps_pymt_id) 
				where direct_attribution_request.op <> 'D' 

			union 
			select  
				proposed_payment.pymt_isu_yr pymt_isu_yr,
				proposed_payment.st_fsa_cd st_fsa_cd,
				proposed_payment.cnty_fsa_cd cnty_fsa_cd,
				(case 
				when  payment_type.app_cd in ( 'fcrp', 'bcap') then  ewt14sgnp.sgnp_stype_desc 
				when payment_type.app_cd =  'crp' then signup_sub_category.signup_sub_category_name 
				end)  sgnp_sub_cat_nm,
				payment_type.acct_pgm_cd acct_pgm_cd,
				payment_type.cnsv_pymt_type cnsv_pymt_type,
				-- JLU update for spark SQL 
				left(proposed_payment.acct_ref_1_nbr,regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',0)-1) cnsv_ctr_nbr, 
				entity.core_cust_id core_cust_id,   
				direct_attribution_request.dir_atrb_rqst_id dir_atrb_rqst_id,
				proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
				proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
				program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
				direct_attribution_request.dir_atrb_cnfrm_nbr dir_atrb_cnfrm_nbr,
				direct_attribution_request.fscl_yr fscl_yr,
				direct_attribution_request.prps_pymt_id prps_pymt_id,
				direct_attribution_request.est_pymt_amt est_pymt_amt,
				direct_attribution_request.app_agi_rule_ind app_agi_rule_ind,
				direct_attribution_request.pymt_lmt_ind pymt_lmt_ind,
				direct_attribution_request.note_txt note_txt,
				direct_attribution_request.tot_rdn_amt tot_rdn_amt,
				(case 
				-- JLU update for spark SQL 
				when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\w+)',1),proposed_payment.acct_ref_1_nbr)  =  1 
				then null 
				-- JLU update for spark SQL 
				when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
				then null 
				else 
				rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
				end) cnsv_ctr_sfx_cd,
				direct_attribution_request.data_stat_cd data_stat_cd,
				direct_attribution_request.cre_dt cre_dt,
				direct_attribution_request.last_chg_dt last_chg_dt,
				direct_attribution_request.last_chg_user_nm last_chg_user_nm,
				direct_attribution_request.op as cdc_oper_cd,
				current_timestamp() as load_dt,
				'CNSV_DIR_ATRB_RQST' as data_src_nm,
				'{etl_start_date}' as cdc_dt,
				10 as tbl_priority
				from ( select * from `fsa-{env}-cnsv`.`ewt40ofrsc`) ewt40ofrsc
				left join `fsa-{env}-cnsv`.`proposed_payment` 
				on ( proposed_payment.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd 
				and proposed_payment.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd 
				and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) 
				)  
				join `fsa-{env}-cnsv-cdc`.`direct_attribution_request` 
				on (direct_attribution_request.prps_pymt_id = proposed_payment.prps_pymt_id) 
				left join `fsa-{env}-cnsv`.`payment_type` 
				on (proposed_payment.pymt_type_id = payment_type.pymt_type_id) 
				left join `fsa-{env}-cnsv`.`pymt_impl` 
				on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id) 
				left join `fsa-{env}-cnsv`.`program_hierarchy_level` 
				on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id) 
				left join  
				(select distinct 
				ltrim(rtrim(concat(cast(master_contract.contract_number as varchar(10)),contract_detail.contract_suffix_number))) as cat,   
				master_contract.signup_identifier, 
				master_contract.administrative_county_fsa_code, 
				master_contract.administrative_state_fsa_code  
				from ( select * from  `fsa-{env}-ccms`.`master_contract` where master_contract.supplemental_contract_type_abbreviation is null ) master_contract 
				left join `fsa-{env}-ccms`.`contract_detail` 
				on (contract_detail.contract_identifier = master_contract.contract_identifier) 
				) as temp 
				on ( temp.administrative_county_fsa_code = proposed_payment.cnty_fsa_cd 
				and temp.administrative_state_fsa_code = proposed_payment.st_fsa_cd 
				and temp.cat = ltrim(rtrim(proposed_payment.acct_ref_1_nbr)))  
				left join `fsa-{env}-ccms`.`signup`  
				on (temp.signup_identifier = signup.signup_identifier) 
				left join `fsa-{env}-ccms`.`signup_sub_category` 
				on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code) 
				left join `fsa-{env}-cnsv`.`ewt14sgnp` 
				on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
				left join ( select distinct prps_pymt_id,core_cust_id from `fsa-{env}-cnsv`.`entity` where  entity.prnt_enty_id is null) entity 
				on (direct_attribution_request.prps_pymt_id = entity.prps_pymt_id 
				and proposed_payment.prps_pymt_id = entity.prps_pymt_id) 
				where direct_attribution_request.op <> 'D' 


			union 
			select  
				proposed_payment.pymt_isu_yr pymt_isu_yr,
				proposed_payment.st_fsa_cd st_fsa_cd,
				proposed_payment.cnty_fsa_cd cnty_fsa_cd,
				(case 
				when  payment_type.app_cd in ( 'fcrp', 'bcap') then  ewt14sgnp.sgnp_stype_desc 
				when payment_type.app_cd =  'crp' then signup_sub_category.signup_sub_category_name 
				end)  sgnp_sub_cat_nm,
				payment_type.acct_pgm_cd acct_pgm_cd,
				payment_type.cnsv_pymt_type cnsv_pymt_type,
				-- JLU update for spark SQL.
				left(proposed_payment.acct_ref_1_nbr,regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',0)-1) cnsv_ctr_nbr, 
				entity.core_cust_id core_cust_id,
				direct_attribution_request.dir_atrb_rqst_id dir_atrb_rqst_id,
				proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
				proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
				program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
				direct_attribution_request.dir_atrb_cnfrm_nbr dir_atrb_cnfrm_nbr,
				direct_attribution_request.fscl_yr fscl_yr,
				direct_attribution_request.prps_pymt_id prps_pymt_id,
				direct_attribution_request.est_pymt_amt est_pymt_amt,
				direct_attribution_request.app_agi_rule_ind app_agi_rule_ind,
				direct_attribution_request.pymt_lmt_ind pymt_lmt_ind,
				direct_attribution_request.note_txt note_txt,
				direct_attribution_request.tot_rdn_amt tot_rdn_amt,
				(case 
				-- JLU update for spark SQL. 
				when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\w+)',1),proposed_payment.acct_ref_1_nbr)  =  1 
				then null 
				-- JLU update for spark SQL. 
				when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
				then null 
				else 
				-- JLU update for spark SQL. 
				rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
				end) cnsv_ctr_sfx_cd,
				direct_attribution_request.data_stat_cd data_stat_cd,
				direct_attribution_request.cre_dt cre_dt,
				direct_attribution_request.last_chg_dt last_chg_dt,
				direct_attribution_request.last_chg_user_nm last_chg_user_nm,
				direct_attribution_request.op as cdc_oper_cd,
				current_timestamp() as load_dt,
				'CNSV_DIR_ATRB_RQST' as data_src_nm,
				'{etl_start_date}' as cdc_dt,
				11 as tbl_priority 
				from  `fsa-{env}-cnsv`.`ewt14sgnp` 
				left join ( select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where cnsv_ctr_seq_nbr > 0 ) ewt40ofrsc 
				on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
				left join `fsa-{env}-cnsv`.`proposed_payment` 
				on ( proposed_payment.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd 
				and proposed_payment.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd 
				and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) 
				) 
				join  `fsa-{env}-cnsv-cdc`.`direct_attribution_request` 
				on (direct_attribution_request.prps_pymt_id = proposed_payment.prps_pymt_id) 
				left join `fsa-{env}-cnsv`.`payment_type` 
				on (proposed_payment.pymt_type_id = payment_type.pymt_type_id) 
				left join `fsa-{env}-cnsv`.`pymt_impl` 
				on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id)  
				left join `fsa-{env}-cnsv`.`program_hierarchy_level` 
				on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id) 
				left join  
				(select distinct 
				ltrim(rtrim(concat(cast(master_contract.contract_number as varchar(10)),contract_detail.contract_suffix_number))) as cat,   
				master_contract.signup_identifier, 
				master_contract.administrative_county_fsa_code, 
				master_contract.administrative_state_fsa_code  
				from ( select * from  `fsa-{env}-ccms`.`master_contract` where master_contract.supplemental_contract_type_abbreviation is null ) master_contract 
				left join `fsa-{env}-ccms`.`contract_detail` 
				on (contract_detail.contract_identifier = master_contract.contract_identifier) 
				) as temp 
				on ( temp.administrative_county_fsa_code = proposed_payment.cnty_fsa_cd 
				and temp.administrative_state_fsa_code = proposed_payment.st_fsa_cd 
				and temp.cat = ltrim(rtrim(proposed_payment.acct_ref_1_nbr)))  
				left join `fsa-{env}-ccms`.`signup`  
				on (temp.signup_identifier = signup.signup_identifier) 
				left join `fsa-{env}-ccms`.`signup_sub_category` 
				on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code) 
				left join ( select distinct prps_pymt_id,core_cust_id from `fsa-{env}-cnsv`.`entity` where  entity.prnt_enty_id is null) entity 
				on (direct_attribution_request.prps_pymt_id = entity.prps_pymt_id 
				and proposed_payment.prps_pymt_id = entity.prps_pymt_id) 
				where direct_attribution_request.op <> 'D'

			union 
			select  
				proposed_payment.pymt_isu_yr pymt_isu_yr,
				proposed_payment.st_fsa_cd st_fsa_cd,
				proposed_payment.cnty_fsa_cd cnty_fsa_cd,
				(case 
				when  payment_type.app_cd in ( 'fcrp', 'bcap') then  ewt14sgnp.sgnp_stype_desc 
				when payment_type.app_cd =  'crp' then signup_sub_category.signup_sub_category_name 
				end)  sgnp_sub_cat_nm,
				payment_type.acct_pgm_cd acct_pgm_cd,
				payment_type.cnsv_pymt_type cnsv_pymt_type,
				-- JLU update for spark SQL. 
				left(proposed_payment.acct_ref_1_nbr,regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',0)-1) cnsv_ctr_nbr, 
				entity.core_cust_id core_cust_id,
				direct_attribution_request.dir_atrb_rqst_id dir_atrb_rqst_id,
				proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
				proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
				program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
				direct_attribution_request.dir_atrb_cnfrm_nbr dir_atrb_cnfrm_nbr,
				direct_attribution_request.fscl_yr fscl_yr,
				direct_attribution_request.prps_pymt_id prps_pymt_id,
				direct_attribution_request.est_pymt_amt est_pymt_amt,
				direct_attribution_request.app_agi_rule_ind app_agi_rule_ind,
				direct_attribution_request.pymt_lmt_ind pymt_lmt_ind,
				direct_attribution_request.note_txt note_txt,
				direct_attribution_request.tot_rdn_amt tot_rdn_amt,
				(case 
				-- JLU update for spark SQL 
				when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\w+)',1),proposed_payment.acct_ref_1_nbr)  =  1 
				then null 
				-- JLU update for spark SQL 
				when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
				then null 
				else 
				-- JLU update for spark SQL.
				rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
				end) cnsv_ctr_sfx_cd,
				direct_attribution_request.data_stat_cd data_stat_cd,
				direct_attribution_request.cre_dt cre_dt,
				direct_attribution_request.last_chg_dt last_chg_dt,
				direct_attribution_request.last_chg_user_nm last_chg_user_nm,
				direct_attribution_request.op as cdc_oper_cd,
				current_timestamp() as load_dt,
				'CNSV_DIR_ATRB_RQST' as data_src_nm,
				'{etl_start_date}' as cdc_dt,
				12 as tbl_priority 
				from 
				(
					 select distinct prps_pymt_id,core_cust_id,entity.op  
						from `fsa-{env}-cnsv`.`entity` 
						where prnt_enty_id is null
				 ) entity
				join  `fsa-{env}-cnsv-cdc`.`direct_attribution_request` 
				on (direct_attribution_request.prps_pymt_id = entity.prps_pymt_id ) 
				left join `fsa-{env}-cnsv`.`proposed_payment` 
				on ( proposed_payment.prps_pymt_id = entity.prps_pymt_id 
				and direct_attribution_request.prps_pymt_id = proposed_payment.prps_pymt_id)   
				left join `fsa-{env}-cnsv`.`payment_type` 
				on (proposed_payment.pymt_type_id = payment_type.pymt_type_id) 
				left join `fsa-{env}-cnsv`.`pymt_impl` 
				on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id) 
				left join `fsa-{env}-cnsv`.`program_hierarchy_level` 
				on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id) 
				left join  
				(select distinct 
				ltrim(rtrim(concat(cast(master_contract.contract_number as varchar(10)),contract_detail.contract_suffix_number))) as cat,   
				master_contract.signup_identifier, 
				master_contract.administrative_county_fsa_code,                                
				master_contract.administrative_state_fsa_code  
				from ( select * from `fsa-{env}-ccms`.`master_contract` where master_contract.supplemental_contract_type_abbreviation is null ) master_contract 
				left join `fsa-{env}-ccms`.`contract_detail` 
				on (contract_detail.contract_identifier = master_contract.contract_identifier) 
				) as temp 
				on ( temp.administrative_county_fsa_code = proposed_payment.cnty_fsa_cd 
				and temp.administrative_state_fsa_code = proposed_payment.st_fsa_cd 
				and temp.cat = ltrim(rtrim(proposed_payment.acct_ref_1_nbr)))  
				left join `fsa-{env}-ccms`.`signup`  
				on (temp.signup_identifier = signup.signup_identifier) 
				left join `fsa-{env}-ccms`.`signup_sub_category` 
				on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code) 
				left join ( select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where cnsv_ctr_seq_nbr > 0 ) ewt40ofrsc 
				on ( proposed_payment.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd 
				and proposed_payment.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd 
				and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) 
				)  
				left join `fsa-{env}-cnsv`.`ewt14sgnp` 
				on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
				where direct_attribution_request.op <> 'D' 
	    ) stg_all 
) stg_unq 
where row_num_part = 1

union
select distinct
null pymt_isu_yr,
null st_fsa_cd,
null cnty_fsa_cd,
null sgnp_sub_cat_nm,
null acct_pgm_cd,
null cnsv_pymt_type,
null cnsv_ctr_nbr,
null core_cust_id,
direct_attribution_request.dir_atrb_rqst_id dir_atrb_rqst_id,
null acct_ref_1_nbr,
null acct_ref_4_nbr,
null hrch_lvl_nm,
direct_attribution_request.dir_atrb_cnfrm_nbr dir_atrb_cnfrm_nbr,
direct_attribution_request.fscl_yr fscl_yr,
direct_attribution_request.prps_pymt_id prps_pymt_id,
direct_attribution_request.est_pymt_amt est_pymt_amt,
direct_attribution_request.app_agi_rule_ind app_agi_rule_ind,
direct_attribution_request.pymt_lmt_ind pymt_lmt_ind,
direct_attribution_request.note_txt note_txt,
direct_attribution_request.tot_rdn_amt tot_rdn_amt,
null cnsv_ctr_sfx_cd,
direct_attribution_request.data_stat_cd data_stat_cd,
direct_attribution_request.cre_dt cre_dt,
direct_attribution_request.last_chg_dt last_chg_dt,
direct_attribution_request.last_chg_user_nm last_chg_user_nm,
'd' cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_DIR_ATRB_RQST' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`direct_attribution_request`
where direct_attribution_request.op = 'D'