/* Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
 Stage SQL: CNSV_ENTY (incremental)
 Location: s3://c108-cert-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_ENTY/incremental/CNSV_ENTY.sql
 =============================================================================  */

select * from
(
	select distinct 
		pymt_isu_yr,
		st_fsa_cd,
		cnty_fsa_cd,
		sgnp_sub_cat_nm,
		acct_pgm_cd,
		cnsv_pymt_type,
		cnsv_ctr_nbr,
		core_cust_id,
		enty_id,
		bus_type_cd,
		acct_ref_1_nbr,
		cnsv_ctr_sfx_cd,
		acct_ref_4_nbr,
		hrch_lvl_nm,
		prnt_enty_id,
		prps_pymt_id,
		bus_type_id,
		agi_ind,
		pymt_shr_pct,
		pmit_enty_shr_amt,
		enty_cmb_acct_nbr,
		prdr_elg_ind,
		agi_shr_pct,
		pmit_shr_pct,
		cash_rent_cpld_fctr,
		note_txt,
		enty_nm,
		pymt_elg_ind,
		agi_pmit_rule_app_shr_pct,
		prv_anl_pymt_tot_amt,
		prv_lump_sum_pymt_tot_amt,
		cmb_enty_prv_pymt_tot_amt,
		est_pymt_amt,
		pymt_lmt_rdn_amt,
		mbr_shr_pct,
		ownshp_shr_pct,
		app_mbr_ctrb_rqmt_ind,
		app_sbst_chg_rqmt_ind,
		fgn_prsn_ind,
		bus_type_desc,
		prnt_core_cust_id,
		root_core_cust_id,
		data_stat_cd,
		cre_dt,
		last_chg_dt,
		last_chg_user_nm,
		cdc_oper_cd,
		load_dt,
		data_src_nm,
		cdc_dt,
		row_number() over ( partition by 
		enty_id
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
			-- JLU update for spark SQL
			left(proposed_payment.acct_ref_1_nbr,regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',0)-1) cnsv_ctr_nbr, 
			entity.core_cust_id core_cust_id,
			entity.enty_id enty_id,
			business_type.bus_type_cd bus_type_cd,
			proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
			(case 
			-- JLU update for spark SQL
			when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','[a-z]',1),proposed_payment.acct_ref_1_nbr)  =  1 
			then null 
			when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
			then null 
			else
			-- JLU update for spark SQL
			rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
			end) cnsv_ctr_sfx_cd,
			proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
			program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
			entity.prnt_enty_id prnt_enty_id,
			entity.prps_pymt_id prps_pymt_id,
			entity.bus_type_id bus_type_id,
			entity.agi_ind agi_ind,
			entity.pymt_shr_pct pymt_shr_pct,
			entity.pmit_enty_shr_amt pmit_enty_shr_amt,
			entity.enty_cmb_acct_nbr enty_cmb_acct_nbr,
			entity.prdr_elg_ind prdr_elg_ind,
			entity.agi_shr_pct agi_shr_pct,
			entity.pmit_shr_pct pmit_shr_pct,
			entity.cash_rent_cpld_fctr cash_rent_cpld_fctr,
			entity.note_text note_txt,
			entity.enty_nm enty_nm,
			entity.pymt_elg_ind pymt_elg_ind,
			entity.agi_pmit_rule_app agi_pmit_rule_app_shr_pct,
			entity.prv_anl_pymt_tot prv_anl_pymt_tot_amt,
			entity.prv_lump_pymt_tot prv_lump_sum_pymt_tot_amt,
			entity.cmb_enty_prv_pymt cmb_enty_prv_pymt_tot_amt,
			entity.est_pymt_amt est_pymt_amt,
			entity.pymt_lmt_rdn_amt pymt_lmt_rdn_amt,
			entity.mbr_shr_pct mbr_shr_pct,
			entity.ownshp_shr_pct ownshp_shr_pct,
			entity.app_mbr_ctrb_rqmt app_mbr_ctrb_rqmt_ind,
			entity.app_sbst_chg_rqmt app_sbst_chg_rqmt_ind,
			entity.fgn_prsn_ind fgn_prsn_ind,
			business_type.bus_type_desc bus_type_desc,
			entity_prnt.core_cust_id prnt_core_cust_id,
			(case when entity.prnt_enty_id is not null then entity_root.core_cust_id  
			else null   
			end )    root_core_cust_id,
			entity.data_stat_cd data_stat_cd,
			entity.cre_dt cre_dt,
			entity.last_chg_dt last_chg_dt,
			entity.last_chg_user_nm last_chg_user_nm,
			entity.op as cdc_oper_cd,
			current_timestamp() as load_dt,
			'CNSV_ENTY' as data_src_nm,
			'{etl_start_date}' as cdc_dt,
			1 as tbl_priority
			from `fsa-{env}-cnsv-cdc`.`entity`        
			left join `fsa-{env}-cnsv`.`proposed_payment`       
			on (entity.prps_pymt_id = proposed_payment.prps_pymt_id)    
			left join `fsa-{env}-cnsv`.`payment_type`        
			on (proposed_payment.pymt_type_id = payment_type.pymt_type_id)    
			left join `fsa-{env}-cnsv`.`pymt_impl`    
			on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id)        
			left join `fsa-{env}-cnsv`.`program_hierarchy_level`    
			on ( pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)    
			left join `fsa-{env}-cnsv`.`business_type`    
			on (entity.bus_type_id = business_type.bus_type_id)        
			left join `fsa-{env}-cnsv`.`entity` entity_prnt   
			on (entity_prnt.enty_id = entity.prnt_enty_id) 
			left join    
			(select distinct 
			ltrim(rtrim(concat(cast(master_contract.contract_number as VARCHAR(10)),contract_detail.contract_suffix_number))) as cat, 
			master_contract.signup_identifier,    
			master_contract.administrative_county_fsa_code,        
			master_contract.administrative_state_fsa_code    
			from ( select * from  `fsa-{env}-ccms`.`master_contract` where master_contract.supplemental_contract_type_abbreviation is null ) master_contract  
			left join `fsa-{env}-ccms`.`contract_detail`    
			) as temp    
			on ( temp.administrative_county_fsa_code = proposed_payment.cnty_fsa_cd    
			and temp.administrative_state_fsa_code = proposed_payment.st_fsa_cd    
			and temp.cat = ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) 
			)
			left join `fsa-{env}-ccms`.`signup`     
			on (temp.signup_identifier = signup.signup_identifier)    
			left join `fsa-{env}-ccms`.`signup_sub_category`    
			on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code)        
			left join ( select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where cnsv_ctr_seq_nbr > 0 ) ewt40ofrsc  
			on ( proposed_payment.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd    
			and proposed_payment.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd    
			and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as VARCHAR(10)),ewt40ofrsc.cnsv_ctr_sufx_cd)))
			)    
			left join `fsa-{env}-cnsv`.`ewt14sgnp`    
			on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)
			left join  `fsa-{env}-cnsv`.`entity` entity_root  
			on (entity.prps_pymt_id = entity_root.prps_pymt_id and entity_root.prnt_enty_id is null)   
			where entity.op <> 'D'
			and entity.dart_filedate BETWEEN '{etl_start_date}' AND '{etl_end_date}'

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
			entity.enty_id enty_id,
			business_type.bus_type_cd bus_type_cd,
			proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
			(case 
			-- JLU update for spark SQL
			when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','[a-z]',1),proposed_payment.acct_ref_1_nbr)  =  1 
			then null 
			when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
			then null 
			else
			rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
			end) cnsv_ctr_sfx_cd,
			proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
			program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
			entity.prnt_enty_id prnt_enty_id,
			entity.prps_pymt_id prps_pymt_id,
			entity.bus_type_id bus_type_id,
			entity.agi_ind agi_ind,
			entity.pymt_shr_pct pymt_shr_pct,
			entity.pmit_enty_shr_amt pmit_enty_shr_amt,
			entity.enty_cmb_acct_nbr enty_cmb_acct_nbr,
			entity.prdr_elg_ind prdr_elg_ind,
			entity.agi_shr_pct agi_shr_pct,
			entity.pmit_shr_pct pmit_shr_pct,
			entity.cash_rent_cpld_fctr cash_rent_cpld_fctr,
			entity.note_text note_txt,
			entity.enty_nm enty_nm,
			entity.pymt_elg_ind pymt_elg_ind,
			entity.agi_pmit_rule_app agi_pmit_rule_app_shr_pct,
			entity.prv_anl_pymt_tot prv_anl_pymt_tot_amt,
			entity.prv_lump_pymt_tot prv_lump_sum_pymt_tot_amt,
			entity.cmb_enty_prv_pymt cmb_enty_prv_pymt_tot_amt,
			entity.est_pymt_amt est_pymt_amt,
			entity.pymt_lmt_rdn_amt pymt_lmt_rdn_amt,
			entity.mbr_shr_pct mbr_shr_pct,
			entity.ownshp_shr_pct ownshp_shr_pct,
			entity.app_mbr_ctrb_rqmt app_mbr_ctrb_rqmt_ind,
			entity.app_sbst_chg_rqmt app_sbst_chg_rqmt_ind,
			entity.fgn_prsn_ind fgn_prsn_ind,
			business_type.bus_type_desc bus_type_desc,
			entity_prnt.core_cust_id prnt_core_cust_id,
			(case when entity.prnt_enty_id is not null then entity_root.core_cust_id  
			else null   
			end )    root_core_cust_id,
			entity.data_stat_cd data_stat_cd,
			entity.cre_dt cre_dt,
			entity.last_chg_dt last_chg_dt,
			entity.last_chg_user_nm last_chg_user_nm,
			entity.op as cdc_oper_cd,
			current_timestamp() as load_dt,
			'CNSV_ENTY' as data_src_nm,
			'{etl_start_date}' as cdc_dt,
			2 as tbl_priority
			from `fsa-{env}-cnsv`.`proposed_payment`        
			join `fsa-{env}-cnsv-cdc`.`entity`        
			on (entity.prps_pymt_id = proposed_payment.prps_pymt_id)    
			left join `fsa-{env}-cnsv`.`payment_type`        
			on (proposed_payment.pymt_type_id = payment_type.pymt_type_id)    
			left join `fsa-{env}-cnsv`.`pymt_impl`    
			on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id)        
			left join `fsa-{env}-cnsv`.`program_hierarchy_level`    
			on ( pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)    
			left join `fsa-{env}-cnsv`.`business_type`  
			on (entity.bus_type_id = business_type.bus_type_id)        
			left join `fsa-{env}-cnsv-cdc`.`entity` entity_prnt    
			on (entity_prnt.enty_id = entity.prnt_enty_id)    
			left join    
			(select distinct
			ltrim(rtrim(concat(cast(master_contract.contract_number as VARCHAR(10)),contract_detail.contract_suffix_number))) as cat, 
			master_contract.signup_identifier,    
			master_contract.administrative_county_fsa_code,        
			master_contract.administrative_state_fsa_code    
			from ( select * from  `fsa-{env}-ccms`.`master_contract` where master_contract.supplemental_contract_type_abbreviation is null ) master_contract    
			left join `fsa-{env}-ccms`.`contract_detail` on (contract_detail.contract_identifier = master_contract.contract_identifier)    
			) as temp    
			on ( temp.administrative_county_fsa_code = proposed_payment.cnty_fsa_cd    
			and temp.administrative_state_fsa_code = proposed_payment.st_fsa_cd    
			and temp.cat = ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) 
			)        
			left join `fsa-{env}-ccms`.`signup`    
			on (temp.signup_identifier = signup.signup_identifier)    
			left join `fsa-{env}-ccms`.`signup_sub_category`    
			on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code)        
			left join ( select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where cnsv_ctr_seq_nbr > 0 ) ewt40ofrsc       
			on ( proposed_payment.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd    
			and proposed_payment.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd    
			and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as VARCHAR(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) 
			)    
			left join `fsa-{env}-cnsv`.`ewt14sgnp`    
			on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)    
			left join  `fsa-{env}-cnsv-cdc`.`entity` entity_root  
			on (entity.prps_pymt_id = entity_root.prps_pymt_id and entity_root.prnt_enty_id is null)  
			where entity.op <> 'D'

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
			entity.enty_id enty_id,
			business_type.bus_type_cd bus_type_cd,
			proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
			(case 
			-- JLU update for spark SQL
			when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','[a-z]',1),proposed_payment.acct_ref_1_nbr)  =  1 
			then null 
			when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
			then null 
			else
			rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
			end) cnsv_ctr_sfx_cd,
			proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
			program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
			entity.prnt_enty_id prnt_enty_id,
			entity.prps_pymt_id prps_pymt_id,
			entity.bus_type_id bus_type_id,
			entity.agi_ind agi_ind,
			entity.pymt_shr_pct pymt_shr_pct,
			entity.pmit_enty_shr_amt pmit_enty_shr_amt,
			entity.enty_cmb_acct_nbr enty_cmb_acct_nbr,
			entity.prdr_elg_ind prdr_elg_ind,
			entity.agi_shr_pct agi_shr_pct,
			entity.pmit_shr_pct pmit_shr_pct,
			entity.cash_rent_cpld_fctr cash_rent_cpld_fctr,
			entity.note_text note_txt,
			entity.enty_nm enty_nm,
			entity.pymt_elg_ind pymt_elg_ind,
			entity.agi_pmit_rule_app agi_pmit_rule_app_shr_pct,
			entity.prv_anl_pymt_tot prv_anl_pymt_tot_amt,
			entity.prv_lump_pymt_tot prv_lump_sum_pymt_tot_amt,
			entity.cmb_enty_prv_pymt cmb_enty_prv_pymt_tot_amt,
			entity.est_pymt_amt est_pymt_amt,
			entity.pymt_lmt_rdn_amt pymt_lmt_rdn_amt,
			entity.mbr_shr_pct mbr_shr_pct,
			entity.ownshp_shr_pct ownshp_shr_pct,
			entity.app_mbr_ctrb_rqmt app_mbr_ctrb_rqmt_ind,
			entity.app_sbst_chg_rqmt app_sbst_chg_rqmt_ind,
			entity.fgn_prsn_ind fgn_prsn_ind,
			business_type.bus_type_desc bus_type_desc,
			entity_prnt.core_cust_id prnt_core_cust_id,
			(case when entity.prnt_enty_id is not null then entity_root.core_cust_id  
			else null   
			end )    root_core_cust_id,
			entity.data_stat_cd data_stat_cd,
			entity.cre_dt cre_dt,
			entity.last_chg_dt last_chg_dt,
			entity.last_chg_user_nm last_chg_user_nm,
			entity.op as cdc_oper_cd,
			current_timestamp() as load_dt,
			'CNSV_ENTY' as data_src_nm,
			'{etl_start_date}' as cdc_dt,
			3 as tbl_priority
			from `fsa-{env}-cnsv`.`payment_type`    
			left join `fsa-{env}-cnsv`.`proposed_payment`
			on (proposed_payment.pymt_type_id = payment_type.pymt_type_id)         
			join `fsa-{env}-cnsv-cdc`.`entity`        
			on (entity.prps_pymt_id = proposed_payment.prps_pymt_id)    
			left join `fsa-{env}-cnsv`.`pymt_impl`    
			on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id)     
			left join `fsa-{env}-cnsv`.`program_hierarchy_level`  
			on ( pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)   
			left join `fsa-{env}-cnsv`.`business_type`    
			on (entity.bus_type_id = business_type.bus_type_id)     
			left join `fsa-{env}-cnsv-cdc`.`entity` entity_prnt   
			on (entity_prnt.enty_id = entity.prnt_enty_id)  
			left join   
			(select distinct 
			ltrim(rtrim(concat(cast(master_contract.contract_number as VARCHAR(10)),contract_detail.contract_suffix_number))) as cat, 
			master_contract.signup_identifier,  
			master_contract.administrative_county_fsa_code,     
			master_contract.administrative_state_fsa_code   
			from ( select * from  `fsa-{env}-ccms`.`master_contract` where master_contract.supplemental_contract_type_abbreviation is null ) master_contract  
			left join `fsa-{env}-ccms`.`contract_detail`  on (contract_detail.contract_identifier = master_contract.contract_identifier)  			
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
			and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as VARCHAR(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) 
			)   
			left join `fsa-{env}-cnsv`.`ewt14sgnp`    
			on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)    
			left join  `fsa-{env}-cnsv-cdc`.`entity` entity_root  
			on (entity.prps_pymt_id = entity_root.prps_pymt_id and entity_root.prnt_enty_id is null)   
			where entity.op <> 'D'

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
			entity.enty_id enty_id,
			business_type.bus_type_cd bus_type_cd,
			proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
			(case 
			-- JLU update for spark SQL
			when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','[a-z]',1),proposed_payment.acct_ref_1_nbr)  =  1 
			then null 
			when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
			then null 
			else
			rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
			end) cnsv_ctr_sfx_cd,
			proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
			program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
			entity.prnt_enty_id prnt_enty_id,
			entity.prps_pymt_id prps_pymt_id,
			entity.bus_type_id bus_type_id,
			entity.agi_ind agi_ind,
			entity.pymt_shr_pct pymt_shr_pct,
			entity.pmit_enty_shr_amt pmit_enty_shr_amt,
			entity.enty_cmb_acct_nbr enty_cmb_acct_nbr,
			entity.prdr_elg_ind prdr_elg_ind,
			entity.agi_shr_pct agi_shr_pct,
			entity.pmit_shr_pct pmit_shr_pct,
			entity.cash_rent_cpld_fctr cash_rent_cpld_fctr,
			entity.note_text note_txt,
			entity.enty_nm enty_nm,
			entity.pymt_elg_ind pymt_elg_ind,
			entity.agi_pmit_rule_app agi_pmit_rule_app_shr_pct,
			entity.prv_anl_pymt_tot prv_anl_pymt_tot_amt,
			entity.prv_lump_pymt_tot prv_lump_sum_pymt_tot_amt,
			entity.cmb_enty_prv_pymt cmb_enty_prv_pymt_tot_amt,
			entity.est_pymt_amt est_pymt_amt,
			entity.pymt_lmt_rdn_amt pymt_lmt_rdn_amt,
			entity.mbr_shr_pct mbr_shr_pct,
			entity.ownshp_shr_pct ownshp_shr_pct,
			entity.app_mbr_ctrb_rqmt app_mbr_ctrb_rqmt_ind,
			entity.app_sbst_chg_rqmt app_sbst_chg_rqmt_ind,
			entity.fgn_prsn_ind fgn_prsn_ind,
			business_type.bus_type_desc bus_type_desc,
			entity_prnt.core_cust_id prnt_core_cust_id,
			(case when entity.prnt_enty_id is not null then entity_root.core_cust_id  
			else null   
			end )    root_core_cust_id,
			entity.data_stat_cd data_stat_cd,
			entity.cre_dt cre_dt,
			entity.last_chg_dt last_chg_dt,
			entity.last_chg_user_nm last_chg_user_nm,
			entity.op as cdc_oper_cd,
			current_timestamp() as load_dt,
			'CNSV_ENTY' as data_src_nm,
			'{etl_start_date}' as cdc_dt,
			4 as tbl_priority
			from `fsa-{env}-cnsv`.`pymt_impl`          
			left join `fsa-{env}-cnsv`.`payment_type`    
			on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id)  
			left join `fsa-{env}-cnsv`.`proposed_payment`  
			on (proposed_payment.pymt_type_id = payment_type.pymt_type_id) 
			join `fsa-{env}-cnsv-cdc`.`entity`    
			on (entity.prps_pymt_id = proposed_payment.prps_pymt_id)    
			left join `fsa-{env}-cnsv`.`program_hierarchy_level`  
			on ( pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)   
			left join `fsa-{env}-cnsv`.`business_type`    
			on (entity.bus_type_id = business_type.bus_type_id)     
			left join `fsa-{env}-cnsv-cdc`.`entity` entity_prnt   
			on (entity_prnt.enty_id = entity.prnt_enty_id)  
			left join   
			(select distinct 
			ltrim(rtrim(concat(cast(master_contract.contract_number as VARCHAR(10)),contract_detail.contract_suffix_number))) as cat, 
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
			and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as VARCHAR(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) 
			)   
			left join `fsa-{env}-cnsv`.`ewt14sgnp`    
			on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)    
			left join  `fsa-{env}-cnsv-cdc`.`entity` entity_root  
			on (entity.prps_pymt_id = entity_root.prps_pymt_id and entity_root.prnt_enty_id is null)  
			where entity.op <> 'D'

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
			entity.enty_id enty_id,
			business_type.bus_type_cd bus_type_cd,
			proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
			(case 
			-- JLU update for spark SQL
			when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','[a-z]',1),proposed_payment.acct_ref_1_nbr)  =  1 
			then null 
			when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
			then null 
			else
			rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
			end) cnsv_ctr_sfx_cd,
			proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
			program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
			entity.prnt_enty_id prnt_enty_id,
			entity.prps_pymt_id prps_pymt_id,
			entity.bus_type_id bus_type_id,
			entity.agi_ind agi_ind,
			entity.pymt_shr_pct pymt_shr_pct,
			entity.pmit_enty_shr_amt pmit_enty_shr_amt,
			entity.enty_cmb_acct_nbr enty_cmb_acct_nbr,
			entity.prdr_elg_ind prdr_elg_ind,
			entity.agi_shr_pct agi_shr_pct,
			entity.pmit_shr_pct pmit_shr_pct,
			entity.cash_rent_cpld_fctr cash_rent_cpld_fctr,
			entity.note_text note_txt,
			entity.enty_nm enty_nm,
			entity.pymt_elg_ind pymt_elg_ind,
			entity.agi_pmit_rule_app agi_pmit_rule_app_shr_pct,
			entity.prv_anl_pymt_tot prv_anl_pymt_tot_amt,
			entity.prv_lump_pymt_tot prv_lump_sum_pymt_tot_amt,
			entity.cmb_enty_prv_pymt cmb_enty_prv_pymt_tot_amt,
			entity.est_pymt_amt est_pymt_amt,
			entity.pymt_lmt_rdn_amt pymt_lmt_rdn_amt,
			entity.mbr_shr_pct mbr_shr_pct,
			entity.ownshp_shr_pct ownshp_shr_pct,
			entity.app_mbr_ctrb_rqmt app_mbr_ctrb_rqmt_ind,
			entity.app_sbst_chg_rqmt app_sbst_chg_rqmt_ind,
			entity.fgn_prsn_ind fgn_prsn_ind,
			business_type.bus_type_desc bus_type_desc,
			entity_prnt.core_cust_id prnt_core_cust_id,
			(case when entity.prnt_enty_id is not null then entity_root.core_cust_id  
			else null   
			end )    root_core_cust_id,
			entity.data_stat_cd data_stat_cd,
			entity.cre_dt cre_dt,
			entity.last_chg_dt last_chg_dt,
			entity.last_chg_user_nm last_chg_user_nm,
			entity.op as cdc_oper_cd,
			current_timestamp() as load_dt,
			'CNSV_ENTY' as data_src_nm,
			'{etl_start_date}' as cdc_dt,
			5 as tbl_priority
			from `fsa-{env}-cnsv`.`program_hierarchy_level`        
			left join `fsa-{env}-cnsv`.`pymt_impl`    
			on ( pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)    
			left join `fsa-{env}-cnsv`.`payment_type`        
			on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id)        
			left join `fsa-{env}-cnsv`.`proposed_payment`       
			on (proposed_payment.pymt_type_id = payment_type.pymt_type_id)            
			join  `fsa-{env}-cnsv-cdc`.`entity`        
			on (entity.prps_pymt_id = proposed_payment.prps_pymt_id)        
			left join `fsa-{env}-cnsv`.`business_type`    
			on (entity.bus_type_id = business_type.bus_type_id)         
			left join `fsa-{env}-cnsv-cdc`.`entity` entity_prnt        
			on (entity_prnt.enty_id = entity.prnt_enty_id)        
			left join        
			(select distinct 
			ltrim(rtrim(concat(cast(master_contract.contract_number as VARCHAR(10)),contract_detail.contract_suffix_number))) as cat,
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
			and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as VARCHAR(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) 
			)        
			left join `fsa-{env}-cnsv`.`ewt14sgnp`        
			on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)        
			left join  `fsa-{env}-cnsv-cdc`.`entity` entity_root  
			on (entity.prps_pymt_id = entity_root.prps_pymt_id and entity_root.prnt_enty_id is null)  
			where entity.op <> 'D'

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
			entity.enty_id enty_id,
			business_type.bus_type_cd bus_type_cd,
			proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
			(case 
			-- JLU update for spark SQL
			when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','[a-z]',1),proposed_payment.acct_ref_1_nbr)  =  1 
			then null 
			when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
			then null 
			else
			rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
			end) cnsv_ctr_sfx_cd,
			proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
			program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
			entity.prnt_enty_id prnt_enty_id,
			entity.prps_pymt_id prps_pymt_id,
			entity.bus_type_id bus_type_id,
			entity.agi_ind agi_ind,
			entity.pymt_shr_pct pymt_shr_pct,
			entity.pmit_enty_shr_amt pmit_enty_shr_amt,
			entity.enty_cmb_acct_nbr enty_cmb_acct_nbr,
			entity.prdr_elg_ind prdr_elg_ind,
			entity.agi_shr_pct agi_shr_pct,
			entity.pmit_shr_pct pmit_shr_pct,
			entity.cash_rent_cpld_fctr cash_rent_cpld_fctr,
			entity.note_text note_txt,
			entity.enty_nm enty_nm,
			entity.pymt_elg_ind pymt_elg_ind,
			entity.agi_pmit_rule_app agi_pmit_rule_app_shr_pct,
			entity.prv_anl_pymt_tot prv_anl_pymt_tot_amt,
			entity.prv_lump_pymt_tot prv_lump_sum_pymt_tot_amt,
			entity.cmb_enty_prv_pymt cmb_enty_prv_pymt_tot_amt,
			entity.est_pymt_amt est_pymt_amt,
			entity.pymt_lmt_rdn_amt pymt_lmt_rdn_amt,
			entity.mbr_shr_pct mbr_shr_pct,
			entity.ownshp_shr_pct ownshp_shr_pct,
			entity.app_mbr_ctrb_rqmt app_mbr_ctrb_rqmt_ind,
			entity.app_sbst_chg_rqmt app_sbst_chg_rqmt_ind,
			entity.fgn_prsn_ind fgn_prsn_ind,
			business_type.bus_type_desc bus_type_desc,
			entity_prnt.core_cust_id prnt_core_cust_id,
			(case when entity.prnt_enty_id is not null then entity_root.core_cust_id  
			else null   
			end )    root_core_cust_id,
			entity.data_stat_cd data_stat_cd,
			entity.cre_dt cre_dt,
			entity.last_chg_dt last_chg_dt,
			entity.last_chg_user_nm last_chg_user_nm,
			entity.op as cdc_oper_cd,
			current_timestamp() as load_dt,
			'CNSV_ENTY' as data_src_nm,
			'{etl_start_date}' as cdc_dt,
			6 as tbl_priority
			from  `fsa-{env}-cnsv`.`business_type`             
			--from  `business_type`             
			join `fsa-{env}-cnsv-cdc`.`entity`    
			on (entity.bus_type_id = business_type.bus_type_id)  
			left join `fsa-{env}-cnsv`.`proposed_payment`    
			on (entity.prps_pymt_id = proposed_payment.prps_pymt_id)    
			left join `fsa-{env}-cnsv`.`payment_type`     
			on (proposed_payment.pymt_type_id = payment_type.pymt_type_id)  
			left join `fsa-{env}-cnsv`.`pymt_impl`    
			on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id)     
			left join `fsa-{env}-cnsv`.`program_hierarchy_level`  
			on ( pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)   
			left join `fsa-{env}-cnsv-cdc`.`entity` entity_prnt   
			on (entity_prnt.enty_id = entity.prnt_enty_id)  
			left join   
			(select distinct 
			ltrim(rtrim(concat(cast(master_contract.contract_number as VARCHAR(10)),contract_detail.contract_suffix_number))) as cat, 
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
			and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as VARCHAR(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) 
			)   
			left join `fsa-{env}-cnsv`.`ewt14sgnp`    
			on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)   
			left join  `fsa-{env}-cnsv-cdc`.`entity` entity_root  
			on (entity.prps_pymt_id = entity_root.prps_pymt_id and entity_root.prnt_enty_id is null)  
			where entity.op <> 'D'

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
			entity.enty_id enty_id,
			business_type.bus_type_cd bus_type_cd,
			proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
			(case 
			-- JLU update for spark SQL
			when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','[a-z]',1),proposed_payment.acct_ref_1_nbr)  =  1 
			then null 
			when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
			then null 
			else
			rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
			end) cnsv_ctr_sfx_cd,
			proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
			program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
			entity.prnt_enty_id prnt_enty_id,
			entity.prps_pymt_id prps_pymt_id,
			entity.bus_type_id bus_type_id,
			entity.agi_ind agi_ind,
			entity.pymt_shr_pct pymt_shr_pct,
			entity.pmit_enty_shr_amt pmit_enty_shr_amt,
			entity.enty_cmb_acct_nbr enty_cmb_acct_nbr,
			entity.prdr_elg_ind prdr_elg_ind,
			entity.agi_shr_pct agi_shr_pct,
			entity.pmit_shr_pct pmit_shr_pct,
			entity.cash_rent_cpld_fctr cash_rent_cpld_fctr,
			entity.note_text note_txt,
			entity.enty_nm enty_nm,
			entity.pymt_elg_ind pymt_elg_ind,
			entity.agi_pmit_rule_app agi_pmit_rule_app_shr_pct,
			entity.prv_anl_pymt_tot prv_anl_pymt_tot_amt,
			entity.prv_lump_pymt_tot prv_lump_sum_pymt_tot_amt,
			entity.cmb_enty_prv_pymt cmb_enty_prv_pymt_tot_amt,
			entity.est_pymt_amt est_pymt_amt,
			entity.pymt_lmt_rdn_amt pymt_lmt_rdn_amt,
			entity.mbr_shr_pct mbr_shr_pct,
			entity.ownshp_shr_pct ownshp_shr_pct,
			entity.app_mbr_ctrb_rqmt app_mbr_ctrb_rqmt_ind,
			entity.app_sbst_chg_rqmt app_sbst_chg_rqmt_ind,
			entity.fgn_prsn_ind fgn_prsn_ind,
			business_type.bus_type_desc bus_type_desc,
			entity_prnt.core_cust_id prnt_core_cust_id,
			(case when entity.prnt_enty_id is not null then entity_root.core_cust_id  
			else null   
			end )    root_core_cust_id,
			entity.data_stat_cd data_stat_cd,
			entity.cre_dt cre_dt,
			entity.last_chg_dt last_chg_dt,
			entity.last_chg_user_nm last_chg_user_nm,
			entity.op as cdc_oper_cd,
			current_timestamp() as load_dt,
			'CNSV_ENTY' as data_src_nm,
			'{etl_start_date}' as cdc_dt,
			7 as tbl_priority
			from  
			(select distinct 
			ltrim(rtrim(concat(cast(master_contract.contract_number as VARCHAR(10)),contract_detail.contract_suffix_number))) as cat, 
			master_contract.signup_identifier,  
			master_contract.administrative_county_fsa_code,     
			master_contract.administrative_state_fsa_code
			from (select * from `fsa-{env}-ccms`.`master_contract` where supplemental_contract_type_abbreviation is null ) master_contract 
			left join `fsa-{env}-ccms`.`contract_detail`  
			on (contract_detail.contract_identifier = master_contract.contract_identifier)  
			) as temp   
			left join `fsa-{env}-cnsv`.`proposed_payment`   
			on ( temp.administrative_county_fsa_code = proposed_payment.cnty_fsa_cd 
			and temp.administrative_state_fsa_code = proposed_payment.st_fsa_cd 
			and temp.cat = ltrim(rtrim(proposed_payment.acct_ref_1_nbr))) 
			join `fsa-{env}-cnsv-cdc`.`entity`    
			on (entity.prps_pymt_id = proposed_payment.prps_pymt_id)    
			left join `fsa-{env}-cnsv`.`payment_type`     
			on (proposed_payment.pymt_type_id = payment_type.pymt_type_id)  
			left join `fsa-{env}-cnsv`.`pymt_impl`    
			on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id)     
			left join `fsa-{env}-cnsv`.`program_hierarchy_level`  
			on ( pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)   
			left join `fsa-{env}-cnsv`.`business_type`    
			on (entity.bus_type_id = business_type.bus_type_id)     
			left join `fsa-{env}-cnsv-cdc`.`entity` entity_prnt   
			on (entity_prnt.enty_id = entity.prnt_enty_id)  
			left join `fsa-{env}-ccms`.`signup`    
			on (temp.signup_identifier = signup.signup_identifier)  
			left join `fsa-{env}-ccms`.`signup_sub_category`  
			on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code)     
			left join ( select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where cnsv_ctr_seq_nbr > 0 ) ewt40ofrsc     
			on ( proposed_payment.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd  
			and proposed_payment.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd   		
			and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as VARCHAR(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) 
			)   
			left join `fsa-{env}-cnsv`.`ewt14sgnp`    
			on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)    
			left join  `fsa-{env}-cnsv-cdc`.`entity` entity_root  
			on (entity.prps_pymt_id = entity_root.prps_pymt_id and entity_root.prnt_enty_id is null)   
			where entity.op <> 'D'

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
			entity.enty_id enty_id,
			business_type.bus_type_cd bus_type_cd,
			proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
			(case 
			-- JLU update for spark SQL
			when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','[a-z]',1),proposed_payment.acct_ref_1_nbr)  =  1 
			then null 
			when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
			then null 
			else
			rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
			end) cnsv_ctr_sfx_cd,
			proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
			program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
			entity.prnt_enty_id prnt_enty_id,
			entity.prps_pymt_id prps_pymt_id,
			entity.bus_type_id bus_type_id,
			entity.agi_ind agi_ind,
			entity.pymt_shr_pct pymt_shr_pct,
			entity.pmit_enty_shr_amt pmit_enty_shr_amt,
			entity.enty_cmb_acct_nbr enty_cmb_acct_nbr,
			entity.prdr_elg_ind prdr_elg_ind,
			entity.agi_shr_pct agi_shr_pct,
			entity.pmit_shr_pct pmit_shr_pct,
			entity.cash_rent_cpld_fctr cash_rent_cpld_fctr,
			entity.note_text note_txt,
			entity.enty_nm enty_nm,
			entity.pymt_elg_ind pymt_elg_ind,
			entity.agi_pmit_rule_app agi_pmit_rule_app_shr_pct,
			entity.prv_anl_pymt_tot prv_anl_pymt_tot_amt,
			entity.prv_lump_pymt_tot prv_lump_sum_pymt_tot_amt,
			entity.cmb_enty_prv_pymt cmb_enty_prv_pymt_tot_amt,
			entity.est_pymt_amt est_pymt_amt,
			entity.pymt_lmt_rdn_amt pymt_lmt_rdn_amt,
			entity.mbr_shr_pct mbr_shr_pct,
			entity.ownshp_shr_pct ownshp_shr_pct,
			entity.app_mbr_ctrb_rqmt app_mbr_ctrb_rqmt_ind,
			entity.app_sbst_chg_rqmt app_sbst_chg_rqmt_ind,
			entity.fgn_prsn_ind fgn_prsn_ind,
			business_type.bus_type_desc bus_type_desc,
			entity_prnt.core_cust_id prnt_core_cust_id,
			(case when entity.prnt_enty_id is not null then entity_root.core_cust_id  
			else null   
			end )    root_core_cust_id,
			entity.data_stat_cd data_stat_cd,
			entity.cre_dt cre_dt,
			entity.last_chg_dt last_chg_dt,
			entity.last_chg_user_nm last_chg_user_nm,
			entity.op as cdc_oper_cd,
			current_timestamp() as load_dt,
			'CNSV_ENTY' as data_src_nm,
			'{etl_start_date}' as cdc_dt,
			8 as tbl_priority
			from  
			(select distinct 
			ltrim(rtrim(concat(cast(master_contract.contract_number as VARCHAR(10)),contract_detail.contract_suffix_number))) as cat, 
			master_contract.signup_identifier,  
			master_contract.administrative_county_fsa_code,     
			master_contract.administrative_state_fsa_code
			from `fsa-{env}-ccms`.`contract_detail`                          
			left join ( select * from  `fsa-{env}-ccms`.`master_contract` where master_contract.supplemental_contract_type_abbreviation is null ) master_contract
			on (contract_detail.contract_identifier = master_contract.contract_identifier)  
			) as temp   
			left join `fsa-{env}-cnsv`.`proposed_payment`   
			on ( temp.administrative_county_fsa_code = proposed_payment.cnty_fsa_cd 
			and temp.administrative_state_fsa_code = proposed_payment.st_fsa_cd 
			and temp.cat = ltrim(rtrim(proposed_payment.acct_ref_1_nbr)))  
			join `fsa-{env}-cnsv-cdc`.`entity`    
			on (entity.prps_pymt_id = proposed_payment.prps_pymt_id)    
			left join `fsa-{env}-cnsv`.`payment_type`     
			on (proposed_payment.pymt_type_id = payment_type.pymt_type_id)  
			left join `fsa-{env}-cnsv`.`pymt_impl`    
			on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id)     
			left join `fsa-{env}-cnsv`.`program_hierarchy_level`  
			on ( pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)   
			left join `fsa-{env}-cnsv`.`business_type`    
			on (entity.bus_type_id = business_type.bus_type_id)     
			left join `fsa-{env}-cnsv-cdc`.`entity` entity_prnt   
			on (entity_prnt.enty_id = entity.prnt_enty_id)  
			left join `fsa-{env}-ccms`.`signup`    
			on (temp.signup_identifier = signup.signup_identifier)  
			left join `fsa-{env}-ccms`.`signup_sub_category`  
			on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code)     
			left join ( select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where cnsv_ctr_seq_nbr > 0 ) ewt40ofrsc   
			on ( proposed_payment.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd  
			and proposed_payment.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd   
			and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as VARCHAR(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) 
			)   
			left join `fsa-{env}-cnsv`.`ewt14sgnp`    
			on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)   
			left join  `fsa-{env}-cnsv-cdc`.`entity` entity_root  
			on (entity.prps_pymt_id = entity_root.prps_pymt_id and entity_root.prnt_enty_id is null)   
			where entity.op <> 'D'

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
			entity.enty_id enty_id,
			business_type.bus_type_cd bus_type_cd,
			proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
			(case 
			-- JLU update for spark SQL
			when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','[a-z]',1),proposed_payment.acct_ref_1_nbr)  =  1 
			then null 
			when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
			then null 
			else
			rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
			end) cnsv_ctr_sfx_cd,
			proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
			program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
			entity.prnt_enty_id prnt_enty_id,
			entity.prps_pymt_id prps_pymt_id,
			entity.bus_type_id bus_type_id,
			entity.agi_ind agi_ind,
			entity.pymt_shr_pct pymt_shr_pct,
			entity.pmit_enty_shr_amt pmit_enty_shr_amt,
			entity.enty_cmb_acct_nbr enty_cmb_acct_nbr,
			entity.prdr_elg_ind prdr_elg_ind,
			entity.agi_shr_pct agi_shr_pct,
			entity.pmit_shr_pct pmit_shr_pct,
			entity.cash_rent_cpld_fctr cash_rent_cpld_fctr,
			entity.note_text note_txt,
			entity.enty_nm enty_nm,
			entity.pymt_elg_ind pymt_elg_ind,
			entity.agi_pmit_rule_app agi_pmit_rule_app_shr_pct,
			entity.prv_anl_pymt_tot prv_anl_pymt_tot_amt,
			entity.prv_lump_pymt_tot prv_lump_sum_pymt_tot_amt,
			entity.cmb_enty_prv_pymt cmb_enty_prv_pymt_tot_amt,
			entity.est_pymt_amt est_pymt_amt,
			entity.pymt_lmt_rdn_amt pymt_lmt_rdn_amt,
			entity.mbr_shr_pct mbr_shr_pct,
			entity.ownshp_shr_pct ownshp_shr_pct,
			entity.app_mbr_ctrb_rqmt app_mbr_ctrb_rqmt_ind,
			entity.app_sbst_chg_rqmt app_sbst_chg_rqmt_ind,
			entity.fgn_prsn_ind fgn_prsn_ind,
			business_type.bus_type_desc bus_type_desc,
			entity_prnt.core_cust_id prnt_core_cust_id,
			(case when entity.prnt_enty_id is not null then entity_root.core_cust_id  
			else null   
			end )    root_core_cust_id,
			entity.data_stat_cd data_stat_cd,
			entity.cre_dt cre_dt,
			entity.last_chg_dt last_chg_dt,
			entity.last_chg_user_nm last_chg_user_nm,
			entity.op as cdc_oper_cd,
			current_timestamp() as load_dt,
			'CNSV_ENTY' as data_src_nm,
			'{etl_start_date}' as cdc_dt,
			9 as tbl_priority
			from ( select * from `fsa-{env}-ccms`.`signup`) signup
			left join   
			(select distinct 
			ltrim(rtrim(concat(cast(master_contract.contract_number as VARCHAR(10)),contract_detail.contract_suffix_number))) as cat, 
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
			join `fsa-{env}-cnsv-cdc`.`entity`    
			on (entity.prps_pymt_id = proposed_payment.prps_pymt_id)    
			left join`fsa-{env}-cnsv`.`payment_type`     
			on (proposed_payment.pymt_type_id = payment_type.pymt_type_id)  
			left join `fsa-{env}-cnsv`.`pymt_impl`    
			on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id)     
			left join `fsa-{env}-cnsv`.`program_hierarchy_level`  
			on ( pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)   
			left join `fsa-{env}-cnsv`.`business_type`    
			on (entity.bus_type_id = business_type.bus_type_id)     
			left join `fsa-{env}-cnsv-cdc`.`entity` entity_prnt   
			on (entity_prnt.enty_id = entity.prnt_enty_id)  
			left join `fsa-{env}-ccms`.`signup_sub_category`  
			on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code)     
			left join ( select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where cnsv_ctr_seq_nbr > 0 ) ewt40ofrsc 
			on ( proposed_payment.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd  
			and proposed_payment.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd   		
			and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as VARCHAR(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) /* ra 09/22/2017:updated code */
			)   
			left join `fsa-{env}-cnsv`.`ewt14sgnp`    
			on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)    
			left join  `fsa-{env}-cnsv-cdc`.`entity` entity_root  
			on (entity.prps_pymt_id = entity_root.prps_pymt_id and entity_root.prnt_enty_id is null)  
			where entity.op <> 'D'


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
			entity.enty_id enty_id,
			business_type.bus_type_cd bus_type_cd,
			proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
			(case 
			-- JLU update for spark SQL
			when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','[a-z]',1),proposed_payment.acct_ref_1_nbr)  =  1 
			then null 
			when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
			then null 
			else			
			rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
			end) cnsv_ctr_sfx_cd,
			proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
			program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
			entity.prnt_enty_id prnt_enty_id,
			entity.prps_pymt_id prps_pymt_id,
			entity.bus_type_id bus_type_id,
			entity.agi_ind agi_ind,
			entity.pymt_shr_pct pymt_shr_pct,
			entity.pmit_enty_shr_amt pmit_enty_shr_amt,
			entity.enty_cmb_acct_nbr enty_cmb_acct_nbr,
			entity.prdr_elg_ind prdr_elg_ind,
			entity.agi_shr_pct agi_shr_pct,
			entity.pmit_shr_pct pmit_shr_pct,
			entity.cash_rent_cpld_fctr cash_rent_cpld_fctr,
			entity.note_text note_txt,
			entity.enty_nm enty_nm,
			entity.pymt_elg_ind pymt_elg_ind,
			entity.agi_pmit_rule_app agi_pmit_rule_app_shr_pct,
			entity.prv_anl_pymt_tot prv_anl_pymt_tot_amt,
			entity.prv_lump_pymt_tot prv_lump_sum_pymt_tot_amt,
			entity.cmb_enty_prv_pymt cmb_enty_prv_pymt_tot_amt,
			entity.est_pymt_amt est_pymt_amt,
			entity.pymt_lmt_rdn_amt pymt_lmt_rdn_amt,
			entity.mbr_shr_pct mbr_shr_pct,
			entity.ownshp_shr_pct ownshp_shr_pct,
			entity.app_mbr_ctrb_rqmt app_mbr_ctrb_rqmt_ind,
			entity.app_sbst_chg_rqmt app_sbst_chg_rqmt_ind,
			entity.fgn_prsn_ind fgn_prsn_ind,
			business_type.bus_type_desc bus_type_desc,
			entity_prnt.core_cust_id prnt_core_cust_id,
			(case when entity.prnt_enty_id is not null then entity_root.core_cust_id  
			else null   
			end )    root_core_cust_id,
			entity.data_stat_cd data_stat_cd,
			entity.cre_dt cre_dt,
			entity.last_chg_dt last_chg_dt,
			entity.last_chg_user_nm last_chg_user_nm,
			entity.op as cdc_oper_cd,
			current_timestamp() as load_dt,
			'CNSV_ENTY' as data_src_nm,
			'{etl_start_date}' as cdc_dt,
			10 as tbl_priority
			from `fsa-{env}-ccms`.`signup_sub_category`      
			left join `fsa-{env}-ccms`.`signup`     
			on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code)  
			left join   
			(select distinct 
			ltrim(rtrim(concat(cast(master_contract.contract_number as VARCHAR(10)),contract_detail.contract_suffix_number))) as cat, 
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
			join `fsa-{env}-cnsv-cdc`.`entity`    
			on (entity.prps_pymt_id = proposed_payment.prps_pymt_id)    
			left join `fsa-{env}-cnsv`.`payment_type`     
			on (proposed_payment.pymt_type_id = payment_type.pymt_type_id)  
			left join `fsa-{env}-cnsv`.`pymt_impl`    
			on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id)     
			left join `fsa-{env}-cnsv`.`program_hierarchy_level`  
			on ( pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)   
			left join `fsa-{env}-cnsv`.`business_type`    
			on (entity.bus_type_id = business_type.bus_type_id)     
			left join `fsa-{env}-cnsv-cdc`.`entity` entity_prnt   
			on (entity_prnt.enty_id = entity.prnt_enty_id)  
			left join ( select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where cnsv_ctr_seq_nbr > 0 ) ewt40ofrsc     
			on ( proposed_payment.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd  
			and proposed_payment.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd   
			and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as VARCHAR(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) 
			)   
			left join `fsa-{env}-cnsv`.`ewt14sgnp`    
			on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)    
			left join  `fsa-{env}-cnsv-cdc`.`entity` entity_root  
			on (entity.prps_pymt_id = entity_root.prps_pymt_id and entity_root.prnt_enty_id is null)  
			where entity.op <> 'D'

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
			entity.enty_id enty_id,
			business_type.bus_type_cd bus_type_cd,
			proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
			(case 
			-- JLU update for spark SQL
			when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','[a-z]',1),proposed_payment.acct_ref_1_nbr)  =  1 
			then null 			
			when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
			then null 
			else
			rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
			end) cnsv_ctr_sfx_cd,
			proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
			program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
			entity.prnt_enty_id prnt_enty_id,
			entity.prps_pymt_id prps_pymt_id,
			entity.bus_type_id bus_type_id,
			entity.agi_ind agi_ind,
			entity.pymt_shr_pct pymt_shr_pct,
			entity.pmit_enty_shr_amt pmit_enty_shr_amt,
			entity.enty_cmb_acct_nbr enty_cmb_acct_nbr,
			entity.prdr_elg_ind prdr_elg_ind,
			entity.agi_shr_pct agi_shr_pct,
			entity.pmit_shr_pct pmit_shr_pct,
			entity.cash_rent_cpld_fctr cash_rent_cpld_fctr,
			entity.note_text note_txt,
			entity.enty_nm enty_nm,
			entity.pymt_elg_ind pymt_elg_ind,
			entity.agi_pmit_rule_app agi_pmit_rule_app_shr_pct,
			entity.prv_anl_pymt_tot prv_anl_pymt_tot_amt,
			entity.prv_lump_pymt_tot prv_lump_sum_pymt_tot_amt,
			entity.cmb_enty_prv_pymt cmb_enty_prv_pymt_tot_amt,
			entity.est_pymt_amt est_pymt_amt,
			entity.pymt_lmt_rdn_amt pymt_lmt_rdn_amt,
			entity.mbr_shr_pct mbr_shr_pct,
			entity.ownshp_shr_pct ownshp_shr_pct,
			entity.app_mbr_ctrb_rqmt app_mbr_ctrb_rqmt_ind,
			entity.app_sbst_chg_rqmt app_sbst_chg_rqmt_ind,
			entity.fgn_prsn_ind fgn_prsn_ind,
			business_type.bus_type_desc bus_type_desc,
			entity_prnt.core_cust_id prnt_core_cust_id,
			(case when entity.prnt_enty_id is not null then entity_root.core_cust_id  
			else null   
			end )    root_core_cust_id,
			entity.data_stat_cd data_stat_cd,
			entity.cre_dt cre_dt,
			entity.last_chg_dt last_chg_dt,
			entity.last_chg_user_nm last_chg_user_nm,
			entity.op as cdc_oper_cd,
			current_timestamp() as load_dt,
			'CNSV_ENTY' as data_src_nm,
			'{etl_start_date}' as cdc_dt,
			11 as tbl_priority
			from  ( select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where cnsv_ctr_seq_nbr > 0 ) ewt40ofrsc                   
			left join `fsa-{env}-cnsv`.`proposed_payment` 
			on ( proposed_payment.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd  
			and proposed_payment.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd   
			and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as VARCHAR(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) 
			)   
			join  `fsa-{env}-cnsv-cdc`.`entity`    
			on (entity.prps_pymt_id = proposed_payment.prps_pymt_id)    
			left join `fsa-{env}-cnsv`.`payment_type`     
			on (proposed_payment.pymt_type_id = payment_type.pymt_type_id)  
			left join `fsa-{env}-cnsv`.`pymt_impl`    
			on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id)     
			left join `fsa-{env}-cnsv`.`program_hierarchy_level` 
			on ( pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)   
			left join `fsa-{env}-cnsv`.`business_type`     
			on (entity.bus_type_id = business_type.bus_type_id)     
			left join `fsa-{env}-cnsv-cdc`.`entity` entity_prnt   
			on (entity_prnt.enty_id = entity.prnt_enty_id)  
			left join   
			(select distinct 
			ltrim(rtrim(concat(cast(master_contract.contract_number as VARCHAR(10)),contract_detail.contract_suffix_number))) as cat,
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
			left join  `fsa-{env}-cnsv-cdc`.`entity` entity_root  
			on (entity.prps_pymt_id = entity_root.prps_pymt_id and entity_root.prnt_enty_id is null)  
			where entity.op <> 'D'
			
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
			entity.enty_id enty_id,
			business_type.bus_type_cd bus_type_cd,
			proposed_payment.acct_ref_1_nbr acct_ref_1_nbr,
			(case 
			-- JLU update for spark SQL
			when locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','[a-z]',1),proposed_payment.acct_ref_1_nbr)  =  1 
			then null 
			when LENGTH(rtrim(ltrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr)-1),'')))) > 2 
			then null 
			else
			rtrim(replace ( proposed_payment.acct_ref_1_nbr,left( proposed_payment.acct_ref_1_nbr,locate(regexp_extract(proposed_payment.acct_ref_1_nbr+'.','(\\d+)',1),proposed_payment.acct_ref_1_nbr) -1),'')) 
			end) cnsv_ctr_sfx_cd,
			proposed_payment.acct_ref_4_nbr acct_ref_4_nbr,
			program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
			entity.prnt_enty_id prnt_enty_id,
			entity.prps_pymt_id prps_pymt_id,
			entity.bus_type_id bus_type_id,
			entity.agi_ind agi_ind,
			entity.pymt_shr_pct pymt_shr_pct,
			entity.pmit_enty_shr_amt pmit_enty_shr_amt,
			entity.enty_cmb_acct_nbr enty_cmb_acct_nbr,
			entity.prdr_elg_ind prdr_elg_ind,
			entity.agi_shr_pct agi_shr_pct,
			entity.pmit_shr_pct pmit_shr_pct,
			entity.cash_rent_cpld_fctr cash_rent_cpld_fctr,
			entity.note_text note_txt,
			entity.enty_nm enty_nm,
			entity.pymt_elg_ind pymt_elg_ind,
			entity.agi_pmit_rule_app agi_pmit_rule_app_shr_pct,
			entity.prv_anl_pymt_tot prv_anl_pymt_tot_amt,
			entity.prv_lump_pymt_tot prv_lump_sum_pymt_tot_amt,
			entity.cmb_enty_prv_pymt cmb_enty_prv_pymt_tot_amt,
			entity.est_pymt_amt est_pymt_amt,
			entity.pymt_lmt_rdn_amt pymt_lmt_rdn_amt,
			entity.mbr_shr_pct mbr_shr_pct,
			entity.ownshp_shr_pct ownshp_shr_pct,
			entity.app_mbr_ctrb_rqmt app_mbr_ctrb_rqmt_ind,
			entity.app_sbst_chg_rqmt app_sbst_chg_rqmt_ind,
			entity.fgn_prsn_ind fgn_prsn_ind,
			business_type.bus_type_desc bus_type_desc,
			entity_prnt.core_cust_id prnt_core_cust_id,
			(case when entity.prnt_enty_id is not null then entity_root.core_cust_id  
			else null   
			end )    root_core_cust_id,
			entity.data_stat_cd data_stat_cd,
			entity.cre_dt cre_dt,
			entity.last_chg_dt last_chg_dt,
			entity.last_chg_user_nm last_chg_user_nm,
			entity.op as cdc_oper_cd,
			current_timestamp() as load_dt,
			'CNSV_ENTY' as data_src_nm,
			'{etl_start_date}' as cdc_dt,
			12 as tbl_priority
			from `fsa-{env}-cnsv`.`ewt14sgnp`            
			left join ( select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where cnsv_ctr_seq_nbr > 0 ) ewt40ofrsc    
			on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)   
			left join `fsa-{env}-cnsv`.`proposed_payment`  
			on ( proposed_payment.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd  
			and proposed_payment.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd   
			and ltrim(rtrim(proposed_payment.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as VARCHAR(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) 
			)   
			join `fsa-{env}-cnsv-cdc`.`entity`    
			on (entity.prps_pymt_id = proposed_payment.prps_pymt_id)    
			left join `fsa-{env}-cnsv`.`payment_type`     
			on (proposed_payment.pymt_type_id = payment_type.pymt_type_id)  
			left join `fsa-{env}-cnsv`.`pymt_impl`    
			on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id)     
			left join `fsa-{env}-cnsv`.`program_hierarchy_level`  
			on ( pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)   
			left join `fsa-{env}-cnsv`.`business_type`    
			on (entity.bus_type_id = business_type.bus_type_id)     
			left join `fsa-{env}-cnsv-cdc`.`entity` entity_prnt   
			on (entity_prnt.enty_id = entity.prnt_enty_id)  
			left join   
			(select distinct ltrim(rtrim(concat(cast(master_contract.contract_number as VARCHAR(10)),contract_detail.contract_suffix_number))) as cat, 		
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
			left join  `fsa-{env}-cnsv-cdc`.`entity` entity_root  
			on (entity.prps_pymt_id = entity_root.prps_pymt_id and entity_root.prnt_enty_id is null)      
			where entity.op <> 'D'
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
entity.core_cust_id core_cust_id,
entity.enty_id enty_id,
null bus_type_cd,
null acct_ref_1_nbr,
null cnsv_ctr_sfx_cd,
null acct_ref_4_nbr,
null hrch_lvl_nm,
entity.prnt_enty_id prnt_enty_id,
entity.prps_pymt_id prps_pymt_id,
entity.bus_type_id bus_type_id,
entity.agi_ind agi_ind,
entity.pymt_shr_pct pymt_shr_pct,
entity.pmit_enty_shr_amt pmit_enty_shr_amt,
entity.enty_cmb_acct_nbr enty_cmb_acct_nbr,
entity.prdr_elg_ind prdr_elg_ind,
entity.agi_shr_pct agi_shr_pct,
entity.pmit_shr_pct pmit_shr_pct,
entity.cash_rent_cpld_fctr cash_rent_cpld_fctr,
entity.note_text note_txt,
entity.enty_nm enty_nm,
entity.pymt_elg_ind pymt_elg_ind,
entity.agi_pmit_rule_app agi_pmit_rule_app_shr_pct,
entity.prv_anl_pymt_tot prv_anl_pymt_tot_amt,
entity.prv_lump_pymt_tot prv_lump_sum_pymt_tot_amt,
entity.cmb_enty_prv_pymt cmb_enty_prv_pymt_tot_amt,
entity.est_pymt_amt est_pymt_amt,
entity.pymt_lmt_rdn_amt pymt_lmt_rdn_amt,
entity.mbr_shr_pct mbr_shr_pct,
entity.ownshp_shr_pct ownshp_shr_pct,
entity.app_mbr_ctrb_rqmt app_mbr_ctrb_rqmt_ind,
entity.app_sbst_chg_rqmt app_sbst_chg_rqmt_ind,
entity.fgn_prsn_ind fgn_prsn_ind,
null bus_type_desc,
null prnt_core_cust_id,
null root_core_cust_id,
entity.data_stat_cd data_stat_cd,
entity.cre_dt cre_dt,
entity.last_chg_dt last_chg_dt,
entity.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_ENTY' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`entity` 
where entity.op = 'D'

 
