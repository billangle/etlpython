-- Author Unknown edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_GRSLD_RANK_SFCTR_PRAC_USE (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/STG/CNSV_GRSLD_RANK_SFCTR_PRAC_USE (Incremental)/incremental/CNSV_GRSLD_RANK_SFCTR_PRAC_USE (Incremental).sql
-- Cynthia Singh edited code with changes for Athena and PySpark 20260204
-- =======================================================================================

select * from
(
select distinct
sgnp_type_desc,
sgnp_stype_desc,
sgnp_nbr,
sgnp_stype_agr_nm,
grsld_rank_fctr_idn,
grsld_rank_sfctr_idn,
cnsv_prac_cd,
grsld_use_type_nm,
rank_pnt_ct,
grsld_rank_sfctr_prac_use_id,
grsld_rank_sfctr_id,
cnsv_prac_use_type_id,
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
row_number() over (
partition by ofr_scnr_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from(
select 
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
grassland_ranking_factor.grsld_rank_fctr_idn grsld_rank_fctr_idn,
grassland_ranking_subfactor.grsld_rank_sfctr_idn grsld_rank_sfctr_idn,
conservation_practice_usage_type.cnsv_prac_cd cnsv_prac_cd,
grassland_usage_type.grsld_use_type_nm grsld_use_type_nm,
grassland_ranking_subfactor_practice_usage.rank_pnt_ct rank_pnt_ct,
grassland_ranking_subfactor_practice_usage.grsld_rank_sfctr_prac_use_id grsld_rank_sfctr_prac_use_id,
grassland_ranking_subfactor_practice_usage.grsld_rank_sfctr_id grsld_rank_sfctr_id,
grassland_ranking_subfactor_practice_usage.cnsv_prac_use_type_id cnsv_prac_use_type_id,
grassland_ranking_subfactor_practice_usage.data_stat_cd data_stat_cd,
grassland_ranking_subfactor_practice_usage.cre_dt cre_dt,
grassland_ranking_subfactor_practice_usage.last_chg_dt last_chg_dt,
grassland_ranking_subfactor_practice_usage.cre_user_nm cre_user_nm,
grassland_ranking_subfactor_practice_usage.last_chg_user_nm last_chg_user_nm,
grassland_ranking_subfactor_practice_usage.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_grsld_rank_sfctr_prac_use' as data_src_nm,
'{etl_start_date}' as cdc_dt,
'' hash_dif,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`ewt14sgnp`
left join    `fsa-{env}-cnsv`.`grassland_ranking_subfactor`
on grassland_ranking_subfactor_practice_usage.grsld_rank_sfctr_id = grassland_ranking_subfactor.grsld_rank_sfctr_id
left join   `fsa-{env}-cnsv`.`conservation_practice_usage_type`
on grassland_ranking_subfactor_practice_usage.cnsv_prac_use_type_id = conservation_practice_usage_type.cnsv_prac_use_type_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_factor` 
on grassland_ranking_subfactor.grsld_rank_fctr_id = grassland_ranking_factor.grsld_rank_fctr_id
left join   `fsa-{env}-cnsv`.`grassland_usage_type`
on conservation_practice_usage_type.grsld_use_type_id = grassland_usage_type.grsld_use_type_id
left join   `fsa-{env}-cnsv`.`ewt14sgnp`
on grassland_ranking_factor.sgnp_id = ewt14sgnp.sgnp_id
where grassland_ranking_subfactor_practice_usage.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and grassland_ranking_subfactor_practice_usage.op <> 'D'

union
select 
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
grassland_ranking_factor.grsld_rank_fctr_idn grsld_rank_fctr_idn,
grassland_ranking_subfactor.grsld_rank_sfctr_idn grsld_rank_sfctr_idn,
conservation_practice_usage_type.cnsv_prac_cd cnsv_prac_cd,
grassland_usage_type.grsld_use_type_nm grsld_use_type_nm,
grassland_ranking_subfactor_practice_usage.rank_pnt_ct rank_pnt_ct,
grassland_ranking_subfactor_practice_usage.grsld_rank_sfctr_prac_use_id grsld_rank_sfctr_prac_use_id,
grassland_ranking_subfactor_practice_usage.grsld_rank_sfctr_id grsld_rank_sfctr_id,
grassland_ranking_subfactor_practice_usage.cnsv_prac_use_type_id cnsv_prac_use_type_id,
grassland_ranking_subfactor_practice_usage.data_stat_cd data_stat_cd,
grassland_ranking_subfactor_practice_usage.cre_dt cre_dt,
grassland_ranking_subfactor_practice_usage.last_chg_dt last_chg_dt,
grassland_ranking_subfactor_practice_usage.cre_user_nm cre_user_nm,
grassland_ranking_subfactor_practice_usage.last_chg_user_nm last_chg_user_nm,
grassland_ranking_subfactor.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_grsld_rank_sfctr_prac_use' as data_src_nm,
'{etl_start_date}' as cdc_dt,
'' hash_dif,
2 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`grassland_ranking_subfactor_practice_usage`
join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor`
on grassland_ranking_subfactor_practice_usage.grsld_rank_sfctr_id = grassland_ranking_subfactor.grsld_rank_sfctr_id
left join   `fsa-{env}-cnsv`.`conservation_practice_usage_type`
on grassland_ranking_subfactor_practice_usage.cnsv_prac_use_type_id= conservation_practice_usage_type.cnsv_prac_use_type_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_factor` 
on grassland_ranking_subfactor.grsld_rank_fctr_id = grassland_ranking_factor.grsld_rank_fctr_id
left join   `fsa-{env}-cnsv`.`grassland_usage_type`
on conservation_practice_usage_type.grsld_use_type_id = grassland_usage_type.grsld_use_type_id
left join   `fsa-{env}-cnsv`.`ewt14sgnp`
on grassland_ranking_factor.sgnp_id = ewt14sgnp.sgnp_id
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
conservation_practice_usage_type.cnsv_prac_cd cnsv_prac_cd,
grassland_usage_type.grsld_use_type_nm grsld_use_type_nm,
grassland_ranking_subfactor_practice_usage.rank_pnt_ct rank_pnt_ct,
grassland_ranking_subfactor_practice_usage.grsld_rank_sfctr_prac_use_id grsld_rank_sfctr_prac_use_id,
grassland_ranking_subfactor_practice_usage.grsld_rank_sfctr_id grsld_rank_sfctr_id,
grassland_ranking_subfactor_practice_usage.cnsv_prac_use_type_id cnsv_prac_use_type_id,
grassland_ranking_subfactor_practice_usage.data_stat_cd data_stat_cd,
grassland_ranking_subfactor_practice_usage.cre_dt cre_dt,
grassland_ranking_subfactor_practice_usage.last_chg_dt last_chg_dt,
grassland_ranking_subfactor_practice_usage.cre_user_nm cre_user_nm,
grassland_ranking_subfactor_practice_usage.last_chg_user_nm last_chg_user_nm,
conservation_practice_usage_type.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_grsld_rank_sfctr_prac_use' as data_src_nm,
'{etl_start_date}' as cdc_dt,
'' hash_dif,
3 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`conservation_practice_usage_type`
join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_practice_usage` 
on conservation_practice_usage_type.cnsv_prac_use_type_id = grassland_ranking_subfactor_practice_usage.cnsv_prac_use_type_id  
left join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor` 
on grassland_ranking_subfactor_practice_usage.grsld_rank_sfctr_id = grassland_ranking_subfactor.grsld_rank_sfctr_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_factor`  
on grassland_ranking_subfactor.grsld_rank_fctr_id = grassland_ranking_factor.grsld_rank_fctr_id
left join   `fsa-{env}-cnsv`.`grassland_usage_type` 
on conservation_practice_usage_type.grsld_use_type_id = grassland_usage_type.grsld_use_type_id
left join   `fsa-{env}-cnsv`.`ewt14sgnp` 
on grassland_ranking_factor.sgnp_id = ewt14sgnp.sgnp_id
where conservation_practice_usage_type.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and conservation_practice_usage_type.op <> 'D'

union
select 
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
grassland_ranking_factor.grsld_rank_fctr_idn grsld_rank_fctr_idn,
grassland_ranking_subfactor.grsld_rank_sfctr_idn grsld_rank_sfctr_idn,
conservation_practice_usage_type.cnsv_prac_cd cnsv_prac_cd,
grassland_usage_type.grsld_use_type_nm grsld_use_type_nm,
grassland_ranking_subfactor_practice_usage.rank_pnt_ct rank_pnt_ct,
grassland_ranking_subfactor_practice_usage.grsld_rank_sfctr_prac_use_id grsld_rank_sfctr_prac_use_id,
grassland_ranking_subfactor_practice_usage.grsld_rank_sfctr_id grsld_rank_sfctr_id,
grassland_ranking_subfactor_practice_usage.cnsv_prac_use_type_id cnsv_prac_use_type_id,
grassland_ranking_subfactor_practice_usage.data_stat_cd data_stat_cd,
grassland_ranking_subfactor_practice_usage.cre_dt cre_dt,
grassland_ranking_subfactor_practice_usage.last_chg_dt last_chg_dt,
grassland_ranking_subfactor_practice_usage.cre_user_nm cre_user_nm,
grassland_ranking_subfactor_practice_usage.last_chg_user_nm last_chg_user_nm,
grassland_ranking_factor.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_grsld_rank_sfctr_prac_use' as data_src_nm,
'{etl_start_date}' as cdc_dt,
'' hash_dif,
4 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`grassland_ranking_factor` 
left join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor`
on grassland_ranking_factor.grsld_rank_fctr_id = grassland_ranking_subfactor.grsld_rank_fctr_id 
join   `fsa-{env}-cnsv`.`grassland_ranking_subfactor_practice_usage`
on grassland_ranking_subfactor.grsld_rank_sfctr_id = grassland_ranking_subfactor_practice_usage.grsld_rank_sfctr_id
left join    `fsa-{env}-cnsv`.`conservation_practice_usage_type`
on grassland_ranking_subfactor_practice_usage.cnsv_prac_use_type_id = conservation_practice_usage_type.cnsv_prac_use_type_id  
left join   `fsa-{env}-cnsv`.`grassland_usage_type`
on conservation_practice_usage_type.grsld_use_type_id = grassland_usage_type.grsld_use_type_id
left join   `fsa-{env}-cnsv`.`ewt14sgnp`
on grassland_ranking_factor.sgnp_id = ewt14sgnp.sgnp_id
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
conservation_practice_usage_type.cnsv_prac_cd cnsv_prac_cd,
grassland_usage_type.grsld_use_type_nm grsld_use_type_nm,
grassland_ranking_subfactor_practice_usage.rank_pnt_ct rank_pnt_ct,
grassland_ranking_subfactor_practice_usage.grsld_rank_sfctr_prac_use_id grsld_rank_sfctr_prac_use_id,
grassland_ranking_subfactor_practice_usage.grsld_rank_sfctr_id grsld_rank_sfctr_id,
grassland_ranking_subfactor_practice_usage.cnsv_prac_use_type_id cnsv_prac_use_type_id,
grassland_ranking_subfactor_practice_usage.data_stat_cd data_stat_cd,
grassland_ranking_subfactor_practice_usage.cre_dt cre_dt,
grassland_ranking_subfactor_practice_usage.last_chg_dt last_chg_dt,
grassland_ranking_subfactor_practice_usage.cre_user_nm cre_user_nm,
grassland_ranking_subfactor_practice_usage.last_chg_user_nm last_chg_user_nm,
grassland_usage_type.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_grsld_rank_sfctr_prac_use' as data_src_nm,
'{etl_start_date}' as cdc_dt,
'' hash_dif,
5 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`grassland_usage_type`
left join   `fsa-{env}-cnsv`.`conservation_practice_usage_type` 
on grassland_usage_type.grsld_use_type_id = conservation_practice_usage_type.grsld_use_type_id
join    `fsa-{env}-cnsv`.`grassland_ranking_subfactor_practice_usage` 
on conservation_practice_usage_type.cnsv_prac_use_type_id = grassland_ranking_subfactor_practice_usage.cnsv_prac_use_type_id
left join    `fsa-{env}-cnsv`.`grassland_ranking_subfactor`
on grassland_ranking_subfactor_practice_usage.grsld_rank_sfctr_id = grassland_ranking_subfactor.grsld_rank_sfctr_id
left join   `fsa-{env}-cnsv`.`grassland_ranking_factor`  
on grassland_ranking_subfactor.grsld_rank_fctr_id = grassland_ranking_factor.grsld_rank_fctr_id
left join   `fsa-{env}-cnsv`.`ewt14sgnp`
on grassland_ranking_factor.sgnp_id = ewt14sgnp.sgnp_id
where grassland_usage_type.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and grassland_usage_type.op <> 'D'

union
select 
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
grassland_ranking_factor.grsld_rank_fctr_idn grsld_rank_fctr_idn,
grassland_ranking_subfactor.grsld_rank_sfctr_idn grsld_rank_sfctr_idn,
conservation_practice_usage_type.cnsv_prac_cd cnsv_prac_cd,
grassland_usage_type.grsld_use_type_nm grsld_use_type_nm,
grassland_ranking_subfactor_practice_usage.rank_pnt_ct rank_pnt_ct,
grassland_ranking_subfactor_practice_usage.grsld_rank_sfctr_prac_use_id grsld_rank_sfctr_prac_use_id,
grassland_ranking_subfactor_practice_usage.grsld_rank_sfctr_id grsld_rank_sfctr_id,
grassland_ranking_subfactor_practice_usage.cnsv_prac_use_type_id cnsv_prac_use_type_id,
grassland_ranking_subfactor_practice_usage.data_stat_cd data_stat_cd,
grassland_ranking_subfactor_practice_usage.cre_dt cre_dt,
grassland_ranking_subfactor_practice_usage.last_chg_dt last_chg_dt,
grassland_ranking_subfactor_practice_usage.cre_user_nm cre_user_nm,
grassland_ranking_subfactor_practice_usage.last_chg_user_nm last_chg_user_nm,
ewt14sgnp.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_grsld_rank_sfctr_prac_use' as data_src_nm,
'{etl_start_date}' as cdc_dt,
'' hash_dif,
6 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`ewt14sgnp`
left join   `fsa-{env}-cnsv`.`grassland_ranking_factor`  
on ewt14sgnp.sgnp_id = grassland_ranking_factor.sgnp_id
left join    `fsa-{env}-cnsv`.`grassland_ranking_subfactor` 
on grassland_ranking_factor.grsld_rank_fctr_id = grassland_ranking_subfactor.grsld_rank_fctr_id
join    `fsa-{env}-cnsv`.`grassland_ranking_subfactor_practice_usage` 
on grassland_ranking_subfactor.grsld_rank_sfctr_id = grassland_ranking_subfactor_practice_usage.grsld_rank_sfctr_id
left join   `fsa-{env}-cnsv`.`conservation_practice_usage_type` 
on grassland_ranking_subfactor_practice_usage.cnsv_prac_use_type_id = conservation_practice_usage_type.cnsv_prac_use_type_id     
left join   `fsa-{env}-cnsv`.`grassland_usage_type` 
on conservation_practice_usage_type.grsld_use_type_id = grassland_usage_type.grsld_use_type_id
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
null grsld_rank_sfctr_idn,
null cnsv_prac_cd,
null grsld_use_type_nm,
grassland_ranking_subfactor_practice_usage.rank_pnt_ct rank_pnt_ct,
grassland_ranking_subfactor_practice_usage.grsld_rank_sfctr_prac_use_id grsld_rank_sfctr_prac_use_id,
grassland_ranking_subfactor_practice_usage.grsld_rank_sfctr_id grsld_rank_sfctr_id,
grassland_ranking_subfactor_practice_usage.cnsv_prac_use_type_id cnsv_prac_use_type_id,
grassland_ranking_subfactor_practice_usage.data_stat_cd data_stat_cd,
grassland_ranking_subfactor_practice_usage.cre_dt cre_dt,
grassland_ranking_subfactor_practice_usage.last_chg_dt last_chg_dt,
grassland_ranking_subfactor_practice_usage.cre_user_nm cre_user_nm,
grassland_ranking_subfactor_practice_usage.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_grsld_rank_sfctr_prac_use' as data_src_nm,
'{etl_start_date}' as cdc_dt,
'' hash_dif,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`grassland_ranking_subfactor_practice_usage`
where grassland_ranking_subfactor_practice_usage.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and grassland_ranking_subfactor_practice_usage.op = 'D'
