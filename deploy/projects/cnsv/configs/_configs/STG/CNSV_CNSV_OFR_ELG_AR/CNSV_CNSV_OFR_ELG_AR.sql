-- Julia Lu edition - will update this paragraph and query when (cnsv/ccms)-cdc is ready
-- Stage SQL: CNSV_CNSV_OFR_ELG_AR (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_CNSV_OFR_ELG_AR/incremental/CNSV_CNSV_OFR_ELG_AR.sql
-- =============================================================================
select * from
(
	select distinct pgm_yr,
		adm_st_fsa_cd,
		adm_cnty_fsa_cd,
		tr_nbr,
		ofr_scnr_nm,
		sgnp_nbr,
		sgnp_type_desc,
		sgnp_stype_desc,
		sgnp_stype_agr_nm,
		cnsv_elg_ar_nm,
		cnsv_ofr_elg_ar_pct,
		gis_ofr_cre_ind,
		cnsv_ofr_elg_ar_id,
		sgnp_id,
		cnsv_elg_ar_id,
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
		cnsv_ofr_elg_ar_id
		order by tbl_priority asc, last_chg_dt desc ) as row_num_part
		from
		(
			select 
			ewt40ofrsc.pgm_yr pgm_yr,
			ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
			ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
			ewt40ofrsc.tr_nbr tr_nbr,
			ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
			ewt14sgnp.sgnp_nbr sgnp_nbr,
			ewt14sgnp.sgnp_type_desc sgnp_type_desc,
			ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
			ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
			conservation_eligibility_area.cnsv_elg_area_nm cnsv_elg_ar_nm,
			conservation_offer_eligibility_area.cnsv_ofr_elg_area_pct cnsv_ofr_elg_ar_pct,
			conservation_offer_eligibility_area.gis_ofr_cre_ind gis_ofr_cre_ind,
			conservation_offer_eligibility_area.cnsv_ofr_elg_area_id cnsv_ofr_elg_ar_id,
			conservation_offer_eligibility_area.sgnp_id sgnp_id,
			conservation_offer_eligibility_area.cnsv_elg_area_id cnsv_elg_ar_id,
			conservation_offer_eligibility_area.ofr_scnr_id ofr_scnr_id,
			conservation_offer_eligibility_area.data_stat_cd data_stat_cd,
			conservation_offer_eligibility_area.cre_dt cre_dt,
			conservation_offer_eligibility_area.last_chg_dt last_chg_dt,
			conservation_offer_eligibility_area.last_chg_user_nm last_chg_user_nm,
			conservation_offer_eligibility_area.op as cdc_oper_cd,
			current_timestamp() as load_dt,
			'cnsv_cnsv_ofr_elg_ar' as data_src_nm,
			'{etl_start_date}' as cdc_dt,
			1 as tbl_priority
			from `fsa-{env}-cnsv-cdc`.`conservation_offer_eligibility_area`
			left join `fsa-{env}-cnsv`.`conservation_eligibility_area_association`
			on(conservation_offer_eligibility_area.sgnp_id = conservation_eligibility_area_association.sgnp_id 
			and conservation_offer_eligibility_area.cnsv_elg_area_id = conservation_eligibility_area_association.cnsv_elg_area_id) 
			left join `fsa-{env}-cnsv`.`conservation_eligibility_area`
			on(conservation_eligibility_area_association.cnsv_elg_area_id = conservation_eligibility_area.cnsv_elg_area_id) 
			left join `fsa-{env}-cnsv`.`ewt40ofrsc`
			on(conservation_offer_eligibility_area.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
			left join `fsa-{env}-cnsv`.`ewt14sgnp`
			on(ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
			where conservation_offer_eligibility_area.op <> 'D'
			  and conservation_offer_eligibility_area.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

			union
			select 
			ewt40ofrsc.pgm_yr pgm_yr,
			ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
			ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
			ewt40ofrsc.tr_nbr tr_nbr,
			ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
			ewt14sgnp.sgnp_nbr sgnp_nbr,
			ewt14sgnp.sgnp_type_desc sgnp_type_desc,
			ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
			ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
			conservation_eligibility_area.cnsv_elg_area_nm cnsv_elg_ar_nm,
			conservation_offer_eligibility_area.cnsv_ofr_elg_area_pct cnsv_ofr_elg_ar_pct,
			conservation_offer_eligibility_area.gis_ofr_cre_ind gis_ofr_cre_ind,
			conservation_offer_eligibility_area.cnsv_ofr_elg_area_id cnsv_ofr_elg_ar_id,
			conservation_offer_eligibility_area.sgnp_id sgnp_id,
			conservation_offer_eligibility_area.cnsv_elg_area_id cnsv_elg_ar_id,
			conservation_offer_eligibility_area.ofr_scnr_id ofr_scnr_id,
			conservation_offer_eligibility_area.data_stat_cd data_stat_cd,
			conservation_offer_eligibility_area.cre_dt cre_dt,
			conservation_offer_eligibility_area.last_chg_dt last_chg_dt,
			conservation_offer_eligibility_area.last_chg_user_nm last_chg_user_nm,
			conservation_offer_eligibility_area.op as cdc_oper_cd,
			current_timestamp() as load_dt,
			'cnsv_cnsv_ofr_elg_ar' as data_src_nm,
			'{etl_start_date}' as cdc_dt,
			2 as tbl_priority
			from `fsa-{env}-cnsv`.`conservation_eligibility_area_association`
			join `fsa-{env}-cnsv-cdc`.`conservation_offer_eligibility_area`
			on(conservation_offer_eligibility_area.sgnp_id = conservation_eligibility_area_association.sgnp_id 
			and conservation_offer_eligibility_area.cnsv_elg_area_id = conservation_eligibility_area_association.cnsv_elg_area_id) 
			left join `fsa-{env}-cnsv`.`conservation_eligibility_area`
			on(conservation_eligibility_area_association.cnsv_elg_area_id = conservation_eligibility_area.cnsv_elg_area_id) 
			left join `fsa-{env}-cnsv`.`ewt40ofrsc`
			on(conservation_offer_eligibility_area.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
			left join `fsa-{env}-cnsv`.`ewt14sgnp`
			on(ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
			where conservation_offer_eligibility_area.op <> 'D'

			union
			select 
			ewt40ofrsc.pgm_yr pgm_yr,
			ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
			ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
			ewt40ofrsc.tr_nbr tr_nbr,
			ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
			ewt14sgnp.sgnp_nbr sgnp_nbr,
			ewt14sgnp.sgnp_type_desc sgnp_type_desc,
			ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
			ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
			conservation_eligibility_area.cnsv_elg_area_nm cnsv_elg_ar_nm,
			conservation_offer_eligibility_area.cnsv_ofr_elg_area_pct cnsv_ofr_elg_ar_pct,
			conservation_offer_eligibility_area.gis_ofr_cre_ind gis_ofr_cre_ind,
			conservation_offer_eligibility_area.cnsv_ofr_elg_area_id cnsv_ofr_elg_ar_id,
			conservation_offer_eligibility_area.sgnp_id sgnp_id,
			conservation_offer_eligibility_area.cnsv_elg_area_id cnsv_elg_ar_id,
			conservation_offer_eligibility_area.ofr_scnr_id ofr_scnr_id,
			conservation_offer_eligibility_area.data_stat_cd data_stat_cd,
			conservation_offer_eligibility_area.cre_dt cre_dt,
			conservation_offer_eligibility_area.last_chg_dt last_chg_dt,
			conservation_offer_eligibility_area.last_chg_user_nm last_chg_user_nm,
			conservation_offer_eligibility_area.op as cdc_oper_cd,
			current_timestamp() as load_dt,
			'cnsv_cnsv_ofr_elg_ar' as data_src_nm,
			'{etl_start_date}' as cdc_dt,
			3 as tbl_priority
			from `fsa-{env}-cnsv`.`conservation_eligibility_area`
			left join  `fsa-{env}-cnsv`.`conservation_eligibility_area_association`
			on(conservation_eligibility_area_association.cnsv_elg_area_id = conservation_eligibility_area.cnsv_elg_area_id) 
			join `fsa-{env}-cnsv-cdc`.`conservation_offer_eligibility_area`
			on(conservation_offer_eligibility_area.sgnp_id = conservation_eligibility_area_association.sgnp_id 
			and conservation_offer_eligibility_area.cnsv_elg_area_id = conservation_eligibility_area_association.cnsv_elg_area_id) 
			left join `fsa-{env}-cnsv`.`ewt40ofrsc`
			on(conservation_offer_eligibility_area.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
			left join `fsa-{env}-cnsv`.`ewt14sgnp`
			on(ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
			where conservation_offer_eligibility_area.op <> 'D'

			union
			select 
			ewt40ofrsc.pgm_yr pgm_yr,
			ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
			ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
			ewt40ofrsc.tr_nbr tr_nbr,
			ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
			ewt14sgnp.sgnp_nbr sgnp_nbr,
			ewt14sgnp.sgnp_type_desc sgnp_type_desc,
			ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
			ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
			conservation_eligibility_area.cnsv_elg_area_nm cnsv_elg_ar_nm,
			conservation_offer_eligibility_area.cnsv_ofr_elg_area_pct cnsv_ofr_elg_ar_pct,
			conservation_offer_eligibility_area.gis_ofr_cre_ind gis_ofr_cre_ind,
			conservation_offer_eligibility_area.cnsv_ofr_elg_area_id cnsv_ofr_elg_ar_id,
			conservation_offer_eligibility_area.sgnp_id sgnp_id,
			conservation_offer_eligibility_area.cnsv_elg_area_id cnsv_elg_ar_id,
			conservation_offer_eligibility_area.ofr_scnr_id ofr_scnr_id,
			conservation_offer_eligibility_area.data_stat_cd data_stat_cd,
			conservation_offer_eligibility_area.cre_dt cre_dt,
			conservation_offer_eligibility_area.last_chg_dt last_chg_dt,
			conservation_offer_eligibility_area.last_chg_user_nm last_chg_user_nm,
			conservation_offer_eligibility_area.op as cdc_oper_cd,
			current_timestamp() as load_dt,
			'cnsv_cnsv_ofr_elg_ar' as data_src_nm,
			'{etl_start_date}' as cdc_dt,
			4 as tbl_priority
			from `fsa-{env}-cnsv`.`ewt40ofrsc`
			join `fsa-{env}-cnsv-cdc`.`conservation_offer_eligibility_area`
			on(conservation_offer_eligibility_area.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
			left join `fsa-{env}-cnsv`.`ewt14sgnp`
			on(ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
			left join `fsa-{env}-cnsv`.`conservation_eligibility_area_association`
			on(conservation_offer_eligibility_area.sgnp_id = conservation_eligibility_area_association.sgnp_id 
			and conservation_offer_eligibility_area.cnsv_elg_area_id = conservation_eligibility_area_association.cnsv_elg_area_id) 
			left join `fsa-{env}-cnsv`.`conservation_eligibility_area`
			on(conservation_eligibility_area_association.cnsv_elg_area_id = conservation_eligibility_area.cnsv_elg_area_id) 
			where conservation_offer_eligibility_area.op <> 'D'

			union
			select 
			ewt40ofrsc.pgm_yr pgm_yr,
			ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
			ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
			ewt40ofrsc.tr_nbr tr_nbr,
			ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
			ewt14sgnp.sgnp_nbr sgnp_nbr,
			ewt14sgnp.sgnp_type_desc sgnp_type_desc,
			ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
			ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
			conservation_eligibility_area.cnsv_elg_area_nm cnsv_elg_ar_nm,
			conservation_offer_eligibility_area.cnsv_ofr_elg_area_pct cnsv_ofr_elg_ar_pct,
			conservation_offer_eligibility_area.gis_ofr_cre_ind gis_ofr_cre_ind,
			conservation_offer_eligibility_area.cnsv_ofr_elg_area_id cnsv_ofr_elg_ar_id,
			conservation_offer_eligibility_area.sgnp_id sgnp_id,
			conservation_offer_eligibility_area.cnsv_elg_area_id cnsv_elg_ar_id,
			conservation_offer_eligibility_area.ofr_scnr_id ofr_scnr_id,
			conservation_offer_eligibility_area.data_stat_cd data_stat_cd,
			conservation_offer_eligibility_area.cre_dt cre_dt,
			conservation_offer_eligibility_area.last_chg_dt last_chg_dt,
			conservation_offer_eligibility_area.last_chg_user_nm last_chg_user_nm,
			conservation_offer_eligibility_area.op as cdc_oper_cd,
			current_timestamp() as load_dt,
			'cnsv_cnsv_ofr_elg_ar' as data_src_nm,
			'{etl_start_date}' as cdc_dt,
			5 as tbl_priority
			from `fsa-{env}-cnsv`.`ewt14sgnp`
			left join `fsa-{env}-cnsv`.`ewt40ofrsc`
			on(ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
			join `fsa-{env}-cnsv-cdc`.`conservation_offer_eligibility_area`
			on(conservation_offer_eligibility_area.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
			left join `fsa-{env}-cnsv`.`conservation_eligibility_area_association`
			on(conservation_offer_eligibility_area.sgnp_id = conservation_eligibility_area_association.sgnp_id 
			and conservation_offer_eligibility_area.cnsv_elg_area_id = conservation_eligibility_area_association.cnsv_elg_area_id) 
			left join `fsa-{env}-cnsv`.`conservation_eligibility_area`
			on(conservation_eligibility_area_association.cnsv_elg_area_id = conservation_eligibility_area.cnsv_elg_area_id) 
			where conservation_offer_eligibility_area.op <> 'D'
		) stg_all
	) stg_unq
	where row_num_part = 1

	union
	select distinct
	null pgm_yr,
	null adm_st_fsa_cd,
	null adm_cnty_fsa_cd,
	null tr_nbr,
	null ofr_scnr_nm,
	null sgnp_nbr,
	null sgnp_type_desc,
	null sgnp_stype_desc,
	null sgnp_stype_agr_nm,
	null cnsv_elg_ar_nm,
	conservation_offer_eligibility_area.cnsv_ofr_elg_area_pct cnsv_ofr_elg_ar_pct,
	conservation_offer_eligibility_area.gis_ofr_cre_ind gis_ofr_cre_ind,
	conservation_offer_eligibility_area.cnsv_ofr_elg_area_id cnsv_ofr_elg_ar_id,
	conservation_offer_eligibility_area.sgnp_id sgnp_id,
	conservation_offer_eligibility_area.cnsv_elg_area_id cnsv_elg_ar_id,
	conservation_offer_eligibility_area.ofr_scnr_id ofr_scnr_id,
	conservation_offer_eligibility_area.data_stat_cd data_stat_cd,
	conservation_offer_eligibility_area.cre_dt cre_dt,
	conservation_offer_eligibility_area.last_chg_dt last_chg_dt,
	conservation_offer_eligibility_area.last_chg_user_nm last_chg_user_nm,
	conservation_offer_eligibility_area.op as cdc_oper_cd,
	current_timestamp() as load_dt,
	'cnsv_cnsv_ofr_elg_ar' as data_src_nm,
	'{etl_start_date}' as cdc_dt,
	1 as row_num_part
	from `fsa-{env}-cnsv-cdc`.`conservation_offer_eligibility_area`
	where conservation_offer_eligibility_area.op = 'D'
