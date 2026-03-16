-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_EWT51OCHST (incremental)
-- Location: s3://c108-cert-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_EWT51OCHST/incremental/CNSV_EWT51OCHST.sql
-- =============================================================================

select * from
(
	select pgm_yr,
	adm_st_fsa_cd,
	adm_cnty_fsa_cd,
	tr_nbr,
	ofr_scnr_nm,
	sgnp_type_desc,
	sgnp_stype_desc,
	sgnp_nbr,
	sgnp_stype_agr_nm,
	crop_yr,
	fld_nbr,
	cnsv_prac_cd,
	fsa_mult_crop_cd,
	fsa_mult_crop_nm,
	farm_nbr,
	ofr_acrg,
	fsa_578_acrg,
	crp_crop_asgn_ind,
	crop_prod_qty,
	crop_hvst_dt,
	cnsv_ctr_seq_nbr,
	cnsv_ctr_sfx_cd,
	ofr_scnr_id,
	data_stat_cd,
	cre_dt,
	last_chg_dt,
	last_chg_user_nm,
	cdc_oper_cd,
	load_dt,
	data_src_nm,
	cdc_dt,
	row_number() over ( partition by 
	crop_yr,
	fld_nbr,
	cnsv_prac_cd,
	ofr_scnr_id
	order by tbl_priority asc, last_chg_dt desc) as row_num_part
	from
	(
		select 
			net_changes.pgm_yr,
			net_changes.adm_st_fsa_cd,
			net_changes.adm_cnty_fsa_cd,
			net_changes.tr_nbr,
			net_changes.ofr_scnr_nm,
			net_changes.sgnp_type_desc,
			net_changes.sgnp_stype_desc,
			net_changes.sgnp_nbr,
			net_changes.sgnp_stype_agr_nm,
			net_changes.crop_yr,
			net_changes.fld_nbr,
			net_changes.cnsv_prac_cd,
			net_changes.fsa_mult_crop_cd,
			net_changes.fsa_mult_crop_nm,
			net_changes.farm_nbr,
			net_changes.ofr_acrg,
			net_changes.fsa_578_acrg,
			net_changes.crp_crop_asgn_ind,
			net_changes.crop_prod_qty,
			net_changes.crop_hvst_dt,
			net_changes.cnsv_ctr_seq_nbr,
			net_changes.cnsv_ctr_sfx_cd,
			net_changes.ofr_scnr_id,
			net_changes.data_stat_cd,
			net_changes.cre_dt,
			net_changes.last_chg_dt,
			net_changes.last_chg_user_nm,
			net_changes.cdc_oper_cd,
			net_changes.load_dt,
			net_changes.data_src_nm,
			net_changes.cdc_dt,
			net_changes.tbl_priority
		from ( 
				select 
					ewt40ofrsc.pgm_yr pgm_yr,
					ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
					ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
					ewt40ofrsc.tr_nbr tr_nbr,
					ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
					ewt14sgnp.sgnp_type_desc sgnp_type_desc,
					ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
					ewt14sgnp.sgnp_nbr sgnp_nbr,
					ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
					ewt51ochst.crop_yr crop_yr,
					ewt51ochst.fld_nbr fld_nbr,
					ewt51ochst.cnsv_prac_cd cnsv_prac_cd,
					ewt51ochst.fsa_mult_crop_cd fsa_mult_crop_cd,
					ewt51ochst.fsa_mult_crop_nm fsa_mult_crop_nm,
					ewt40ofrsc.farm_nbr farm_nbr,
					ewt51ochst.ofr_acrg ofr_acrg,
					ewt51ochst.fsa_578_acrg fsa_578_acrg,
					ewt51ochst.crp_crop_asgn_ind crp_crop_asgn_ind,
					ewt51ochst.crop_prod_qty crop_prod_qty,
					ewt51ochst.crop_hvst_dt crop_hvst_dt,
					ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
					ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
					ewt51ochst.ofr_scnr_id ofr_scnr_id,
					ewt51ochst.data_stat_cd data_stat_cd,
					ewt51ochst.cre_dt cre_dt,
					ewt51ochst.last_chg_dt last_chg_dt,
					ewt51ochst.last_chg_user_nm last_chg_user_nm,
					ewt51ochst.op as cdc_oper_cd,
					current_timestamp() as load_dt,
					'CNSV_EWT51OCHST' as data_src_nm,
					'{etl_start_date}' as cdc_dt,
					1 as tbl_priority,
					row_number() over ( partition by 
					ewt51ochst.crop_yr,
					ewt51ochst.fld_nbr,
					ewt51ochst.cnsv_prac_cd,
					ewt51ochst.ofr_scnr_id
					order by ewt51ochst.dart_filedate desc) as row_num_part
				from `fsa-{env}-cnsv-cdc`.`ewt51ochst` 
					left join `fsa-{env}-cnsv`.`ewt40ofrsc` 
					on (ewt51ochst.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
					left join `fsa-{env}-cnsv`.`ewt14sgnp` 
					on (ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)
					where ewt51ochst.op <> 'D'
					  and ewt51ochst.dart_filedate BETWEEN '{etl_start_date}' AND '{etl_end_date}'
				) net_changes
		   where net_changes.row_num_part = 1

		union 
		select  
			ewt40ofrsc.pgm_yr pgm_yr,
			ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
			ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
			ewt40ofrsc.tr_nbr tr_nbr,
			ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
			ewt14sgnp.sgnp_type_desc sgnp_type_desc,
			ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
			ewt14sgnp.sgnp_nbr sgnp_nbr,
			ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
			ewt51ochst.crop_yr crop_yr,
			ewt51ochst.fld_nbr fld_nbr,
			ewt51ochst.cnsv_prac_cd cnsv_prac_cd,
			ewt51ochst.fsa_mult_crop_cd fsa_mult_crop_cd,
			ewt51ochst.fsa_mult_crop_nm fsa_mult_crop_nm,
			ewt40ofrsc.farm_nbr farm_nbr,
			ewt51ochst.ofr_acrg ofr_acrg,
			ewt51ochst.fsa_578_acrg fsa_578_acrg,
			ewt51ochst.crp_crop_asgn_ind crp_crop_asgn_ind,
			ewt51ochst.crop_prod_qty crop_prod_qty,
			ewt51ochst.crop_hvst_dt crop_hvst_dt,
			ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
			ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
			ewt51ochst.ofr_scnr_id ofr_scnr_id,
			ewt51ochst.data_stat_cd data_stat_cd,
			ewt51ochst.cre_dt cre_dt,
			ewt51ochst.last_chg_dt last_chg_dt,
			ewt51ochst.last_chg_user_nm last_chg_user_nm,
			ewt51ochst.op as cdc_oper_cd,
			current_timestamp() as load_dt,
			'CNSV_EWT51OCHST' as data_src_nm,
			'{etl_start_date}' as cdc_dt,
			2 as tbl_priority
			from `fsa-{env}-cnsv`.`ewt40ofrsc` 
			join `fsa-{env}-cnsv-cdc`.`ewt51ochst` 
			on (ewt51ochst.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
			left join `fsa-{env}-cnsv`.`ewt14sgnp` 
			on (ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)
			where ewt51ochst.op <> 'D'

		union
		select 
			ewt40ofrsc.pgm_yr pgm_yr,
			ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
			ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
			ewt40ofrsc.tr_nbr tr_nbr,
			ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
			ewt14sgnp.sgnp_type_desc sgnp_type_desc,
			ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
			ewt14sgnp.sgnp_nbr sgnp_nbr,
			ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
			ewt51ochst.crop_yr crop_yr,
			ewt51ochst.fld_nbr fld_nbr,
			ewt51ochst.cnsv_prac_cd cnsv_prac_cd,
			ewt51ochst.fsa_mult_crop_cd fsa_mult_crop_cd,
			ewt51ochst.fsa_mult_crop_nm fsa_mult_crop_nm,
			ewt40ofrsc.farm_nbr farm_nbr,
			ewt51ochst.ofr_acrg ofr_acrg,
			ewt51ochst.fsa_578_acrg fsa_578_acrg,
			ewt51ochst.crp_crop_asgn_ind crp_crop_asgn_ind,
			ewt51ochst.crop_prod_qty crop_prod_qty,
			ewt51ochst.crop_hvst_dt crop_hvst_dt,
			ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
			ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
			ewt51ochst.ofr_scnr_id ofr_scnr_id,
			ewt51ochst.data_stat_cd data_stat_cd,
			ewt51ochst.cre_dt cre_dt,
			ewt51ochst.last_chg_dt last_chg_dt,
			ewt51ochst.last_chg_user_nm last_chg_user_nm,
			ewt51ochst.op as cdc_oper_cd,
			current_timestamp() as load_dt,
			'CNSV_EWT51OCHST' as data_src_nm,
			'{etl_start_date}' as cdc_dt,
			3 as tbl_priority
			from `fsa-{env}-cnsv`.`ewt14sgnp` 
			left join `fsa-{env}-cnsv`.`ewt40ofrsc` 
			on (ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
			join `fsa-{env}-cnsv-cdc`.`ewt51ochst` 
			on (ewt51ochst.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id)
			where ewt51ochst.op <> 'D' 

		union 
		select  
			delete_net.pgm_yr,
			delete_net.adm_st_fsa_cd,
			delete_net.adm_cnty_fsa_cd,
			delete_net.tr_nbr,
			delete_net.ofr_scnr_nm,
			delete_net.sgnp_type_desc,
			delete_net.sgnp_stype_desc,
			delete_net.sgnp_nbr,
			delete_net.sgnp_stype_agr_nm,
			delete_net.crop_yr,
			delete_net.fld_nbr,
			delete_net.cnsv_prac_cd,
			delete_net.fsa_mult_crop_cd,
			delete_net.fsa_mult_crop_nm,
			delete_net.farm_nbr,
			delete_net.ofr_acrg,
			delete_net.fsa_578_acrg,
			delete_net.crp_crop_asgn_ind,
			delete_net.crop_prod_qty,
			delete_net.crop_hvst_dt,
			delete_net.cnsv_ctr_seq_nbr,
			delete_net.cnsv_ctr_sfx_cd,
			delete_net.ofr_scnr_id,
			delete_net.data_stat_cd,
			delete_net.cre_dt,
			delete_net.last_chg_dt,
			delete_net.last_chg_user_nm,
			delete_net.cdc_oper_cd,
			delete_net.load_dt,
			delete_net.data_src_nm,
			delete_net.cdc_dt,
			delete_net.tbl_priority
			from (
					select
					null pgm_yr,
					null adm_st_fsa_cd,
					null adm_cnty_fsa_cd,
					null tr_nbr,
					null ofr_scnr_nm,
					null sgnp_type_desc,
					null sgnp_stype_desc,
					null sgnp_nbr,
					null sgnp_stype_agr_nm,
					ewt51ochst.crop_yr crop_yr,
					ewt51ochst.fld_nbr fld_nbr,
					ewt51ochst.cnsv_prac_cd cnsv_prac_cd,
					ewt51ochst.fsa_mult_crop_cd fsa_mult_crop_cd,
					ewt51ochst.fsa_mult_crop_nm fsa_mult_crop_nm,
					null farm_nbr,
					ewt51ochst.ofr_acrg ofr_acrg,
					ewt51ochst.fsa_578_acrg fsa_578_acrg,
					ewt51ochst.crp_crop_asgn_ind crp_crop_asgn_ind,
					ewt51ochst.crop_prod_qty crop_prod_qty,
					ewt51ochst.crop_hvst_dt crop_hvst_dt,
					null cnsv_ctr_seq_nbr,
					null cnsv_ctr_sfx_cd,
					ewt51ochst.ofr_scnr_id ofr_scnr_id,
					ewt51ochst.data_stat_cd data_stat_cd,
					ewt51ochst.cre_dt cre_dt,
					ewt51ochst.last_chg_dt last_chg_dt,
					ewt51ochst.last_chg_user_nm last_chg_user_nm,
					ewt51ochst.op as cdc_oper_cd,
					current_timestamp() as load_dt,
					'CNSV_EWT51OCHST' as data_src_nm,
					'{etl_start_date}' as cdc_dt,
					1 as tbl_priority,
					row_number() over ( partition by 
					ewt51ochst.crop_yr,
					ewt51ochst.fld_nbr,
					ewt51ochst.cnsv_prac_cd,
					ewt51ochst.ofr_scnr_id
					order by ewt51ochst.dart_filedate desc) as row_num_part
					from `fsa-{env}-cnsv-cdc`.`ewt51ochst` 
					where ewt51ochst.op = 'D'
				) delete_net
			where delete_net.row_num_part = 1
	) stg_all
) stg_unq
where row_num_part = 1
