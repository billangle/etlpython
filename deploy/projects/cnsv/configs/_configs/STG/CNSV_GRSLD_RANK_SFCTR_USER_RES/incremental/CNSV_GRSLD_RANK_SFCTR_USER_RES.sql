-- author unknown edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: cnsv_grsld_rank_sfctr_user_res (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_grsld_rank_sfctr_user_res (incremental)/incremental/cnsv_grsld_rank_sfctr_user_res (incremental).sql
-- cynthia singh edited code with changes for athena and pyspark 20260204
-- =======================================================================================

select * from
(
select distinct 
pgm_yr,
adm_st_fsa_cd,
adm_cnty_fsa_cd,
sgnp_type_desc,
sgnp_stype_desc,
sgnp_nbr,
sgnp_stype_agr_nm,
tr_nbr,
ofr_scnr_nm,
grsld_rank_fctr_idn,
grsld_rank_sfctr_idn,
grsld_rank_sfctr_qstn_idn,
user_resp_ind,
grsld_rank_sfctr_user_resp_id,
grsld_rank_sfctr_qstn_id,
grsld_rank_sfctr_rslt_id,
data_stat_cd,
cre_dt,
last_chg_dt,
cre_user_nm,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
grsld_rank_sfctr_user_resp_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from (
select 
ewt40ofrsc.pgm_yr pgm_yr,
ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt40ofrsc.tr_nbr tr_nbr,
ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
grassland_ranking_factor.grsld_rank_fctr_idn grsld_rank_fctr_idn,
grassland_ranking_subfactor.grsld_rank_sfctr_idn grsld_rank_sfctr_idn,
grassland_ranking_subfactor_question.grsld_rank_sfctr_qstn_idn grsld_rank_sfctr_qstn_idn,
grassland_ranking_subfactor_user_response.user_resp_ind user_resp_ind,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_user_resp_id grsld_rank_sfctr_user_resp_id,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_qstn_id grsld_rank_sfctr_qstn_id,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_rslt_id grsld_rank_sfctr_rslt_id,
grassland_ranking_subfactor_user_response.data_stat_cd data_stat_cd,
grassland_ranking_subfactor_user_response.cre_dt cre_dt,
grassland_ranking_subfactor_user_response.last_chg_dt last_chg_dt,
grassland_ranking_subfactor_user_response.cre_user_nm cre_user_nm,
grassland_ranking_subfactor_user_response.last_chg_user_nm last_chg_user_nm,
grassland_ranking_subfactor_user_response.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_grsld_rank_sfctr_user_res' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`grassland_ranking_subfactor_user_response`
left join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_question`
on grassland_ranking_subfactor_user_response.grsld_rank_sfctr_qstn_id = grassland_ranking_subfactor_question.grsld_rank_sfctr_qstn_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor`
on grassland_ranking_subfactor.grsld_rank_sfctr_id = grassland_ranking_subfactor_question.grsld_rank_sfctr_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_factor`
on grassland_ranking_factor.grsld_rank_fctr_id = grassland_ranking_subfactor.grsld_rank_fctr_id
left join   `fsa-{env}-cnsv`.`ewt14sgnp`
on ewt14sgnp.sgnp_id = grassland_ranking_factor.sgnp_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_result` 
on grassland_ranking_subfactor_result.grsld_rank_sfctr_rslt_id = grassland_ranking_subfactor_user_response.grsld_rank_sfctr_rslt_id
left join   `fsa-{env}-cnsv`.`crp_grassland_offer_scenario`
on grassland_ranking_subfactor_result.ofr_scnr_id = crp_grassland_offer_scenario.ofr_scnr_id
left join   `fsa-{env}-cnsv`.`ewt40ofrsc`
on crp_grassland_offer_scenario.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id
where grassland_ranking_subfactor_user_response.dart_filedate between date '{etl_start_date}' and date '{etl_end_date}'
and grassland_ranking_subfactor_user_response.op <> 'D'

union
select 
ewt40ofrsc.pgm_yr pgm_yr,
ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt40ofrsc.tr_nbr tr_nbr,
ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
grassland_ranking_factor.grsld_rank_fctr_idn grsld_rank_fctr_idn,
grassland_ranking_subfactor.grsld_rank_sfctr_idn grsld_rank_sfctr_idn,
grassland_ranking_subfactor_question.grsld_rank_sfctr_qstn_idn grsld_rank_sfctr_qstn_idn,
grassland_ranking_subfactor_user_response.user_resp_ind user_resp_ind,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_user_resp_id grsld_rank_sfctr_user_resp_id,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_qstn_id grsld_rank_sfctr_qstn_id,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_rslt_id grsld_rank_sfctr_rslt_id,
grassland_ranking_subfactor_user_response.data_stat_cd data_stat_cd,
grassland_ranking_subfactor_user_response.cre_dt cre_dt,
grassland_ranking_subfactor_user_response.last_chg_dt last_chg_dt,
grassland_ranking_subfactor_user_response.cre_user_nm cre_user_nm,
grassland_ranking_subfactor_user_response.last_chg_user_nm last_chg_user_nm,
grassland_ranking_subfactor_question.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_grsld_rank_sfctr_user_res' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`grassland_ranking_subfactor_question`
join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_user_response`
on grassland_ranking_subfactor_user_response.grsld_rank_sfctr_qstn_id = grassland_ranking_subfactor_question.grsld_rank_sfctr_qstn_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor`
on grassland_ranking_subfactor.grsld_rank_sfctr_id = grassland_ranking_subfactor_question.grsld_rank_sfctr_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_factor`
on grassland_ranking_factor.grsld_rank_fctr_id= grassland_ranking_subfactor.grsld_rank_fctr_id
left join   `fsa-{env}-cnsv`.`ewt14sgnp`
on ewt14sgnp.sgnp_id = grassland_ranking_factor.sgnp_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_result` 
on grassland_ranking_subfactor_result.grsld_rank_sfctr_rslt_id = grassland_ranking_subfactor_user_response.grsld_rank_sfctr_rslt_id
left join   `fsa-{env}-cnsv`.`crp_grassland_offer_scenario`
on grassland_ranking_subfactor_result.ofr_scnr_id = crp_grassland_offer_scenario.ofr_scnr_id
left join   `fsa-{env}-cnsv`.`ewt40ofrsc`
on crp_grassland_offer_scenario.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id
where grassland_ranking_subfactor_question.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and grassland_ranking_subfactor_question.op <> 'D'

union
select 
ewt40ofrsc.pgm_yr pgm_yr,
ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt40ofrsc.tr_nbr tr_nbr,
ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
grassland_ranking_factor.grsld_rank_fctr_idn grsld_rank_fctr_idn,
grassland_ranking_subfactor.grsld_rank_sfctr_idn grsld_rank_sfctr_idn,
grassland_ranking_subfactor_question.grsld_rank_sfctr_qstn_idn grsld_rank_sfctr_qstn_idn,
grassland_ranking_subfactor_user_response.user_resp_ind user_resp_ind,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_user_resp_id grsld_rank_sfctr_user_resp_id,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_qstn_id grsld_rank_sfctr_qstn_id,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_rslt_id grsld_rank_sfctr_rslt_id,
grassland_ranking_subfactor_user_response.data_stat_cd data_stat_cd,
grassland_ranking_subfactor_user_response.cre_dt cre_dt,
grassland_ranking_subfactor_user_response.last_chg_dt last_chg_dt,
grassland_ranking_subfactor_user_response.cre_user_nm cre_user_nm,
grassland_ranking_subfactor_user_response.last_chg_user_nm last_chg_user_nm,
grassland_ranking_subfactor.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_grsld_rank_sfctr_user_res' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`grassland_ranking_subfactor`
left join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_question`
on grassland_ranking_subfactor.grsld_rank_sfctr_id = grassland_ranking_subfactor_question.grsld_rank_sfctr_id
join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_user_response`
on grassland_ranking_subfactor_user_response.grsld_rank_sfctr_qstn_id = grassland_ranking_subfactor_question.grsld_rank_sfctr_qstn_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_factor`
on grassland_ranking_factor.grsld_rank_fctr_id = grassland_ranking_subfactor.grsld_rank_fctr_id
left join   `fsa-{env}-cnsv`.`ewt14sgnp`
on ewt14sgnp.sgnp_id = grassland_ranking_factor.sgnp_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_result` 
on grassland_ranking_subfactor_result.grsld_rank_sfctr_rslt_id = grassland_ranking_subfactor_user_response.grsld_rank_sfctr_rslt_id
left join   `fsa-{env}-cnsv`.`crp_grassland_offer_scenario`
on grassland_ranking_subfactor_result.ofr_scnr_id = crp_grassland_offer_scenario.ofr_scnr_id
left join   `fsa-{env}-cnsv`.`ewt40ofrsc`
on crp_grassland_offer_scenario.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id
where grassland_ranking_subfactor.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and grassland_ranking_subfactor.op <> 'D'

union
select 
ewt40ofrsc.pgm_yr pgm_yr,
ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt40ofrsc.tr_nbr tr_nbr,
ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
grassland_ranking_factor.grsld_rank_fctr_idn grsld_rank_fctr_idn,
grassland_ranking_subfactor.grsld_rank_sfctr_idn grsld_rank_sfctr_idn,
grassland_ranking_subfactor_question.grsld_rank_sfctr_qstn_idn grsld_rank_sfctr_qstn_idn,
grassland_ranking_subfactor_user_response.user_resp_ind user_resp_ind,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_user_resp_id grsld_rank_sfctr_user_resp_id,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_qstn_id grsld_rank_sfctr_qstn_id,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_rslt_id grsld_rank_sfctr_rslt_id,
grassland_ranking_subfactor_user_response.data_stat_cd data_stat_cd,
grassland_ranking_subfactor_user_response.cre_dt cre_dt,
grassland_ranking_subfactor_user_response.last_chg_dt last_chg_dt,
grassland_ranking_subfactor_user_response.cre_user_nm cre_user_nm,
grassland_ranking_subfactor_user_response.last_chg_user_nm last_chg_user_nm,
grassland_ranking_factor.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_grsld_rank_sfctr_user_res' as data_src_nm,
'{etl_start_date}' as cdc_dt,
4 as tbl_priority
from   `fsa-{env}-cnsv-cdc`.`grassland_ranking_factor`
left join   `fsa-{env}-cnsv`.grassland_ranking_subfactor grassland_ranking_subfactor 
on grassland_ranking_factor.grsld_rank_fctr_id = grassland_ranking_subfactor.grsld_rank_fctr_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_question`
on grassland_ranking_subfactor.grsld_rank_sfctr_id = grassland_ranking_subfactor_question.grsld_rank_sfctr_id
join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_user_response` 
on grassland_ranking_subfactor_user_response.grsld_rank_sfctr_qstn_id = grassland_ranking_subfactor_question.grsld_rank_sfctr_qstn_id
left join   `fsa-{env}-cnsv`.`ewt14sgnp`
on ewt14sgnp.sgnp_id = grassland_ranking_factor.sgnp_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_result` 
on grassland_ranking_subfactor_result.grsld_rank_sfctr_rslt_id = grassland_ranking_subfactor_user_response.grsld_rank_sfctr_rslt_id
left join   `fsa-{env}-cnsv`.`crp_grassland_offer_scenario`
on grassland_ranking_subfactor_result.ofr_scnr_id = crp_grassland_offer_scenario.ofr_scnr_id
left join   `fsa-{env}-cnsv`.`ewt40ofrsc`
on crp_grassland_offer_scenario.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id
where grassland_ranking_factor.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and grassland_ranking_factor.op <> 'D'

union
select 
ewt40ofrsc.pgm_yr pgm_yr,
ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt40ofrsc.tr_nbr tr_nbr,
ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
grassland_ranking_factor.grsld_rank_fctr_idn grsld_rank_fctr_idn,
grassland_ranking_subfactor.grsld_rank_sfctr_idn grsld_rank_sfctr_idn,
grassland_ranking_subfactor_question.grsld_rank_sfctr_qstn_idn grsld_rank_sfctr_qstn_idn,
grassland_ranking_subfactor_user_response.user_resp_ind user_resp_ind,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_user_resp_id grsld_rank_sfctr_user_resp_id,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_qstn_id grsld_rank_sfctr_qstn_id,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_rslt_id grsld_rank_sfctr_rslt_id,
grassland_ranking_subfactor_user_response.data_stat_cd data_stat_cd,
grassland_ranking_subfactor_user_response.cre_dt cre_dt,
grassland_ranking_subfactor_user_response.last_chg_dt last_chg_dt,
grassland_ranking_subfactor_user_response.cre_user_nm cre_user_nm,
grassland_ranking_subfactor_user_response.last_chg_user_nm last_chg_user_nm,
ewt14sgnp.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_grsld_rank_sfctr_user_res' as data_src_nm,
'{etl_start_date}' as cdc_dt,
5 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`ewt14sgnp`
left join   `fsa-{env}-cnsv`.`grassland_ranking_factor`
on ewt14sgnp.sgnp_id = grassland_ranking_factor.sgnp_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor`
on grassland_ranking_factor.grsld_rank_fctr_id = grassland_ranking_subfactor.grsld_rank_fctr_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_question`
on grassland_ranking_subfactor.grsld_rank_sfctr_id = grassland_ranking_subfactor_question.grsld_rank_sfctr_id
join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_user_response` 
on grassland_ranking_subfactor_user_response.grsld_rank_sfctr_qstn_id = grassland_ranking_subfactor_question.grsld_rank_sfctr_qstn_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_result` 
on grassland_ranking_subfactor_result.grsld_rank_sfctr_rslt_id = grassland_ranking_subfactor_user_response.grsld_rank_sfctr_rslt_id
left join   `fsa-{env}-cnsv`.`crp_grassland_offer_scenario`
on grassland_ranking_subfactor_result.ofr_scnr_id = crp_grassland_offer_scenario.ofr_scnr_id
left join   `fsa-{env}-cnsv`.`ewt40ofrsc`
on crp_grassland_offer_scenario.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id
where ewt14sgnp.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and ewt14sgnp.op <> 'D'

union
select 
ewt40ofrsc.pgm_yr pgm_yr,
ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt40ofrsc.tr_nbr tr_nbr,
ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
grassland_ranking_factor.grsld_rank_fctr_idn grsld_rank_fctr_idn,
grassland_ranking_subfactor.grsld_rank_sfctr_idn grsld_rank_sfctr_idn,
grassland_ranking_subfactor_question.grsld_rank_sfctr_qstn_idn grsld_rank_sfctr_qstn_idn,
grassland_ranking_subfactor_user_response.user_resp_ind user_resp_ind,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_user_resp_id grsld_rank_sfctr_user_resp_id,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_qstn_id grsld_rank_sfctr_qstn_id,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_rslt_id grsld_rank_sfctr_rslt_id,
grassland_ranking_subfactor_user_response.data_stat_cd data_stat_cd,
grassland_ranking_subfactor_user_response.cre_dt cre_dt,
grassland_ranking_subfactor_user_response.last_chg_dt last_chg_dt,
grassland_ranking_subfactor_user_response.cre_user_nm cre_user_nm,
grassland_ranking_subfactor_user_response.last_chg_user_nm last_chg_user_nm,
grassland_ranking_subfactor_result.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_grsld_rank_sfctr_user_res' as data_src_nm,
'{etl_start_date}' as cdc_dt,
6 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`grassland_ranking_subfactor_result`
left join   `fsa-{env}-cnsv`.`crp_grassland_offer_scenario`
on grassland_ranking_subfactor_result.ofr_scnr_id = crp_grassland_offer_scenario.ofr_scnr_id
left join   `fsa-{env}-cnsv`.`ewt40ofrsc`
on crp_grassland_offer_scenario.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id
join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_user_response` 
on grassland_ranking_subfactor_user_response.grsld_rank_sfctr_rslt_id = grassland_ranking_subfactor_result.grsld_rank_sfctr_rslt_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_question`
on grassland_ranking_subfactor_question.grsld_rank_sfctr_qstn_id = grassland_ranking_subfactor_user_response.grsld_rank_sfctr_qstn_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor`
on grassland_ranking_subfactor.grsld_rank_sfctr_id = grassland_ranking_subfactor_question.grsld_rank_sfctr_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_factor`
on grassland_ranking_factor.grsld_rank_fctr_id= grassland_ranking_subfactor.grsld_rank_fctr_id
left join   `fsa-{env}-cnsv`.`ewt14sgnp`
on ewt14sgnp.sgnp_id = grassland_ranking_factor.sgnp_id
where grassland_ranking_subfactor_result.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and grassland_ranking_subfactor_result.op <> 'D'

union
select 
ewt40ofrsc.pgm_yr pgm_yr,
ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt40ofrsc.tr_nbr tr_nbr,
ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
grassland_ranking_factor.grsld_rank_fctr_idn grsld_rank_fctr_idn,
grassland_ranking_subfactor.grsld_rank_sfctr_idn grsld_rank_sfctr_idn,
grassland_ranking_subfactor_question.grsld_rank_sfctr_qstn_idn grsld_rank_sfctr_qstn_idn,
grassland_ranking_subfactor_user_response.user_resp_ind user_resp_ind,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_user_resp_id grsld_rank_sfctr_user_resp_id,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_qstn_id grsld_rank_sfctr_qstn_id,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_rslt_id grsld_rank_sfctr_rslt_id,
grassland_ranking_subfactor_user_response.data_stat_cd data_stat_cd,
grassland_ranking_subfactor_user_response.cre_dt cre_dt,
grassland_ranking_subfactor_user_response.last_chg_dt last_chg_dt,
grassland_ranking_subfactor_user_response.cre_user_nm cre_user_nm,
grassland_ranking_subfactor_user_response.last_chg_user_nm last_chg_user_nm,
crp_grassland_offer_scenario.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_grsld_rank_sfctr_user_res' as data_src_nm,
'{etl_start_date}' as cdc_dt,
7 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`crp_grassland_offer_scenario`
left join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_result` 
on grassland_ranking_subfactor_result.ofr_scnr_id = crp_grassland_offer_scenario.ofr_scnr_id
left join   `fsa-{env}-cnsv`.`ewt40ofrsc`
on crp_grassland_offer_scenario.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id
join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_user_response` 
on grassland_ranking_subfactor_user_response.grsld_rank_sfctr_rslt_id = grassland_ranking_subfactor_result.grsld_rank_sfctr_rslt_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_question`
on grassland_ranking_subfactor_question.grsld_rank_sfctr_qstn_id = grassland_ranking_subfactor_user_response.grsld_rank_sfctr_qstn_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor`
on grassland_ranking_subfactor.grsld_rank_sfctr_id = grassland_ranking_subfactor_question.grsld_rank_sfctr_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_factor`
on grassland_ranking_factor.grsld_rank_fctr_id= grassland_ranking_subfactor.grsld_rank_fctr_id
left join   `fsa-{env}-cnsv`.`ewt14sgnp`
on ewt14sgnp.sgnp_id = grassland_ranking_factor.sgnp_id
where crp_grassland_offer_scenario.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and crp_grassland_offer_scenario.op <> 'D'

union
select 
ewt40ofrsc.pgm_yr pgm_yr,
ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt40ofrsc.tr_nbr tr_nbr,
ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
grassland_ranking_factor.grsld_rank_fctr_idn grsld_rank_fctr_idn,
grassland_ranking_subfactor.grsld_rank_sfctr_idn grsld_rank_sfctr_idn,
grassland_ranking_subfactor_question.grsld_rank_sfctr_qstn_idn grsld_rank_sfctr_qstn_idn,
grassland_ranking_subfactor_user_response.user_resp_ind user_resp_ind,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_user_resp_id grsld_rank_sfctr_user_resp_id,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_qstn_id grsld_rank_sfctr_qstn_id,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_rslt_id grsld_rank_sfctr_rslt_id,
grassland_ranking_subfactor_user_response.data_stat_cd data_stat_cd,
grassland_ranking_subfactor_user_response.cre_dt cre_dt,
grassland_ranking_subfactor_user_response.last_chg_dt last_chg_dt,
grassland_ranking_subfactor_user_response.cre_user_nm cre_user_nm,
grassland_ranking_subfactor_user_response.last_chg_user_nm last_chg_user_nm,
ewt40ofrsc.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_grsld_rank_sfctr_user_res' as data_src_nm,
'{etl_start_date}' as cdc_dt,
8 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`ewt40ofrsc`
left join   `fsa-{env}-cnsv`.`crp_grassland_offer_scenario`
on crp_grassland_offer_scenario.ofr_scnr_id= ewt40ofrsc.ofr_scnr_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_result` 
on grassland_ranking_subfactor_result.ofr_scnr_id = crp_grassland_offer_scenario.ofr_scnr_id
join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_user_response` 
on grassland_ranking_subfactor_user_response.grsld_rank_sfctr_rslt_id = grassland_ranking_subfactor_result.grsld_rank_sfctr_rslt_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_question`
on grassland_ranking_subfactor_question.grsld_rank_sfctr_qstn_id = grassland_ranking_subfactor_user_response.grsld_rank_sfctr_qstn_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor`
on grassland_ranking_subfactor.grsld_rank_sfctr_id = grassland_ranking_subfactor_question.grsld_rank_sfctr_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_factor`
on grassland_ranking_factor.grsld_rank_fctr_id= grassland_ranking_subfactor.grsld_rank_fctr_id
left join   `fsa-{env}-cnsv`.`ewt14sgnp`
on ewt14sgnp.sgnp_id = grassland_ranking_factor.sgnp_id
where ewt40ofrsc.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and ewt40ofrsc.op <> 'D'

) stg_all
) stg_unq
where row_num_part = 1
	and coalesce(try_cast(cre_dt as date), date '1900-01-01') <= date '{etl_start_date}'
    and coalesce(try_cast(last_chg_dt as date), date '1900-01-01') <= date '{etl_start_date}'
union
select distinct
null pgm_yr,
null adm_st_fsa_cd,
null adm_cnty_fsa_cd,
null sgnp_type_desc,
null sgnp_stype_desc,
null sgnp_nbr,
null sgnp_stype_agr_nm,
null tr_nbr,
null ofr_scnr_nm,
null grsld_rank_fctr_idn,
null grsld_rank_sfctr_idn,
null grsld_rank_sfctr_qstn_idn,
grassland_ranking_subfactor_user_response.user_resp_ind user_resp_ind,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_user_resp_id grsld_rank_sfctr_user_resp_id,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_qstn_id grsld_rank_sfctr_qstn_id,
grassland_ranking_subfactor_user_response.grsld_rank_sfctr_rslt_id grsld_rank_sfctr_rslt_id,
grassland_ranking_subfactor_user_response.data_stat_cd data_stat_cd,
grassland_ranking_subfactor_user_response.cre_dt cre_dt,
grassland_ranking_subfactor_user_response.last_chg_dt last_chg_dt,
grassland_ranking_subfactor_user_response.cre_user_nm cre_user_nm,
grassland_ranking_subfactor_user_response.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_grsld_rank_sfctr_user_res' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`grassland_ranking_subfactor_user_response`
where grassland_ranking_subfactor_user_response.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and grassland_ranking_subfactor_user_response.op = 'D'




