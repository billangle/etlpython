-- author unknown edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: cnsv_grsld_rank_sfctr (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_grsld_rank_sfctr (incremental)/incremental/cnsv_grsld_rank_sfctr (incremental).sql
-- cynthia singh edited code with changes for athena and pyspark 20260204
-- =============================================================================

select * from
(
select sgnp_type_desc,
sgnp_stype_desc,
sgnp_nbr,
sgnp_stype_agr_nm,
grsld_rank_fctr_idn,
grsld_rank_sfctr_idn,
grsld_rank_sfctr_type_cd,
grsld_rank_sfctr_dter_mthd_cd,
awrd_each_qlfy_resp_ind,
qstn_ct,
rank_pnt_ct,
grsld_rank_fctr_id,
grsld_rank_sfctr_id,
data_stat_cd,
cre_dt,
last_chg_dt,
cre_user_nm,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
''  hash_dif,
row_number() over ( partition by 
grsld_rank_sfctr_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
grassland_ranking_factor.grsld_rank_fctr_idn grsld_rank_fctr_idn,
grassland_ranking_subfactor.grsld_rank_sfctr_idn grsld_rank_sfctr_idn,
grassland_ranking_subfactor.grsld_rank_sfctr_type_cd grsld_rank_sfctr_type_cd,
grassland_ranking_subfactor.grsld_rank_sfctr_dter_mthd_cd grsld_rank_sfctr_dter_mthd_cd,
grassland_ranking_subfactor.awrd_each_qlfy_resp_ind awrd_each_qlfy_resp_ind,
grassland_ranking_subfactor.qstn_ct qstn_ct,
grassland_ranking_subfactor.rank_pnt_ct rank_pnt_ct,
grassland_ranking_subfactor.grsld_rank_fctr_id grsld_rank_fctr_id,
grassland_ranking_subfactor.grsld_rank_sfctr_id grsld_rank_sfctr_id,
grassland_ranking_subfactor.data_stat_cd data_stat_cd,
grassland_ranking_subfactor.cre_dt cre_dt,
grassland_ranking_subfactor.last_chg_dt last_chg_dt,
grassland_ranking_subfactor.cre_user_nm cre_user_nm,
grassland_ranking_subfactor.last_chg_user_nm last_chg_user_nm,
grassland_ranking_subfactor.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_grsld_rank_sfctr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from   `fsa-{env}-cnsv-cdc`.`grassland_ranking_subfactor`
left join   `fsa-{env}-cnsv`.`grassland_ranking_factor`
on (grassland_ranking_factor.grsld_rank_fctr_id = grassland_ranking_subfactor.grsld_rank_fctr_id)
left join   `fsa-{env}-cnsv`.`ewt14sgnp`
on (grassland_ranking_factor.sgnp_id = ewt14sgnp.sgnp_id)
where grassland_ranking_subfactor.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and grassland_ranking_subfactor.op <> 'D'

union
select 
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
grassland_ranking_factor.grsld_rank_fctr_idn grsld_rank_fctr_idn,
grassland_ranking_subfactor.grsld_rank_sfctr_idn grsld_rank_sfctr_idn,
grassland_ranking_subfactor.grsld_rank_sfctr_type_cd grsld_rank_sfctr_type_cd,
grassland_ranking_subfactor.grsld_rank_sfctr_dter_mthd_cd grsld_rank_sfctr_dter_mthd_cd,
grassland_ranking_subfactor.awrd_each_qlfy_resp_ind awrd_each_qlfy_resp_ind,
grassland_ranking_subfactor.qstn_ct qstn_ct,
grassland_ranking_subfactor.rank_pnt_ct rank_pnt_ct,
grassland_ranking_subfactor.grsld_rank_fctr_id grsld_rank_fctr_id,
grassland_ranking_subfactor.grsld_rank_sfctr_id grsld_rank_sfctr_id,
grassland_ranking_subfactor.data_stat_cd data_stat_cd,
grassland_ranking_subfactor.cre_dt cre_dt,
grassland_ranking_subfactor.last_chg_dt last_chg_dt,
grassland_ranking_subfactor.cre_user_nm cre_user_nm,
grassland_ranking_subfactor.last_chg_user_nm last_chg_user_nm,
grassland_ranking_factor.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_grsld_rank_sfctr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from    `fsa-{env}-cnsv-cdc`.`grassland_ranking_factor`
join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor` 
on (grassland_ranking_factor.grsld_rank_fctr_id = grassland_ranking_subfactor.grsld_rank_fctr_id)
left join   `fsa-{env}-cnsv`.`ewt14sgnp`
on (grassland_ranking_factor.sgnp_id = ewt14sgnp.sgnp_id)
where grassland_ranking_factor.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and grassland_ranking_factor.op <> 'D'

union
select 
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
grassland_ranking_factor.grsld_rank_fctr_idn grsld_rank_fctr_idn,
grassland_ranking_subfactor.grsld_rank_sfctr_idn grsld_rank_sfctr_idn,
grassland_ranking_subfactor.grsld_rank_sfctr_type_cd grsld_rank_sfctr_type_cd,
grassland_ranking_subfactor.grsld_rank_sfctr_dter_mthd_cd grsld_rank_sfctr_dter_mthd_cd,
grassland_ranking_subfactor.awrd_each_qlfy_resp_ind awrd_each_qlfy_resp_ind,
grassland_ranking_subfactor.qstn_ct qstn_ct,
grassland_ranking_subfactor.rank_pnt_ct rank_pnt_ct,
grassland_ranking_subfactor.grsld_rank_fctr_id grsld_rank_fctr_id,
grassland_ranking_subfactor.grsld_rank_sfctr_id grsld_rank_sfctr_id,
grassland_ranking_subfactor.data_stat_cd data_stat_cd,
grassland_ranking_subfactor.cre_dt cre_dt,
grassland_ranking_subfactor.last_chg_dt last_chg_dt,
grassland_ranking_subfactor.cre_user_nm cre_user_nm,
grassland_ranking_subfactor.last_chg_user_nm last_chg_user_nm,
ewt14sgnp.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_grsld_rank_sfctr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from    `fsa-{env}-cnsv-cdc`.`ewt14sgnp`
left join   `fsa-{env}-cnsv`.`grassland_ranking_factor`
on (ewt14sgnp.sgnp_id = grassland_ranking_factor.sgnp_id)
join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor`
on (grassland_ranking_factor.grsld_rank_fctr_id = grassland_ranking_subfactor.grsld_rank_fctr_id)
where ewt14sgnp.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and ewt14sgnp.op <> 'D'

) stg_all
) stg_unq

where row_num_part = 1
	and coalesce(try_cast(cre_dt as date), date '1900-01-01') <= date '{etl_start_date}'
    and coalesce(try_cast(last_chg_dt as date), date '1900-01-01') <= date '{etl_start_date}'
union
select distinct
null sgnp_type_desc,
null sgnp_stype_desc,
null sgnp_nbr,
null sgnp_stype_agr_nm,
null grsld_rank_fctr_idn,
grassland_ranking_subfactor.grsld_rank_sfctr_idn grsld_rank_sfctr_idn,
grassland_ranking_subfactor.grsld_rank_sfctr_type_cd grsld_rank_sfctr_type_cd,
grassland_ranking_subfactor.grsld_rank_sfctr_dter_mthd_cd grsld_rank_sfctr_dter_mthd_cd,
grassland_ranking_subfactor.awrd_each_qlfy_resp_ind awrd_each_qlfy_resp_ind,
grassland_ranking_subfactor.qstn_ct qstn_ct,
grassland_ranking_subfactor.rank_pnt_ct rank_pnt_ct,
grassland_ranking_subfactor.grsld_rank_fctr_id grsld_rank_fctr_id,
grassland_ranking_subfactor.grsld_rank_sfctr_id grsld_rank_sfctr_id,
grassland_ranking_subfactor.data_stat_cd data_stat_cd,
grassland_ranking_subfactor.cre_dt cre_dt,
grassland_ranking_subfactor.last_chg_dt last_chg_dt,
grassland_ranking_subfactor.cre_user_nm cre_user_nm,
grassland_ranking_subfactor.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_grsld_rank_sfctr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`grassland_ranking_subfactor`
where grassland_ranking_subfactor.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and grassland_ranking_subfactor.op = 'D'
