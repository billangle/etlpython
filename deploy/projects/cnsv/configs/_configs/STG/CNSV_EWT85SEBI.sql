-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_EWT85SEBI (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_EWT85SEBI/incremental/CNSV_EWT85SEBI.sql
-- =============================================================================

select * from
(
select distinct sgnp_nbr,
sgnp_type_desc,
sgnp_stype_desc,
sgnp_stype_agr_nm,
ntl_ebi_nm,
ntl_ebi_fctr_id,
ntl_ebi_sfctr_id,
ebi_sfctr_nm,
sgnp_id,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
ntl_ebi_fctr_id,
ntl_ebi_sfctr_id,
sgnp_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt94nebi.ntl_ebi_nm ntl_ebi_nm,
ewt85sebi.ntl_ebi_fctr_id ntl_ebi_fctr_id,
ewt85sebi.ntl_ebi_sfctr_id ntl_ebi_sfctr_id,
ewt85sebi.ebi_sfctr_nm ebi_sfctr_nm,
ewt85sebi.sgnp_id sgnp_id,
ewt85sebi.data_stat_cd data_stat_cd,
ewt85sebi.cre_dt cre_dt,
ewt85sebi.last_chg_dt last_chg_dt,
ewt85sebi.last_chg_user_nm last_chg_user_nm,
ewt85sebi.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt85sebi' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`ewt85sebi`
left join `fsa-{env}-cnsv`.`ewt94nebi`
     on (ewt85sebi.ntl_ebi_fctr_id = ewt94nebi.ntl_ebi_fctr_id 
     and ewt85sebi.ntl_ebi_sfctr_id = ewt94nebi.ntl_ebi_sfctr_id) 
left join `fsa-{env}-cnsv`.`ewt14sgnp` 
     on (ewt85sebi.sgnp_id = ewt14sgnp.sgnp_id)
where ewt85sebi.op <> 'D'
  and ewt85sebi.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt94nebi.ntl_ebi_nm ntl_ebi_nm,
ewt85sebi.ntl_ebi_fctr_id ntl_ebi_fctr_id,
ewt85sebi.ntl_ebi_sfctr_id ntl_ebi_sfctr_id,
ewt85sebi.ebi_sfctr_nm ebi_sfctr_nm,
ewt85sebi.sgnp_id sgnp_id,
ewt85sebi.data_stat_cd data_stat_cd,
ewt85sebi.cre_dt cre_dt,
ewt85sebi.last_chg_dt last_chg_dt,
ewt85sebi.last_chg_user_nm last_chg_user_nm,
ewt85sebi.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt85sebi' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`ewt94nebi`
   join `fsa-{env}-cnsv-cdc`.`ewt85sebi`
     on (ewt94nebi.ntl_ebi_fctr_id = ewt85sebi.ntl_ebi_fctr_id and ewt94nebi.ntl_ebi_sfctr_id = ewt85sebi.ntl_ebi_sfctr_id) 
   left join `fsa-{env}-cnsv`.`ewt14sgnp`
     on (ewt85sebi.sgnp_id = ewt14sgnp.sgnp_id)
where ewt85sebi.op <> 'D'

union
select 
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt94nebi.ntl_ebi_nm ntl_ebi_nm,
ewt85sebi.ntl_ebi_fctr_id ntl_ebi_fctr_id,
ewt85sebi.ntl_ebi_sfctr_id ntl_ebi_sfctr_id,
ewt85sebi.ebi_sfctr_nm ebi_sfctr_nm,
ewt85sebi.sgnp_id sgnp_id,
ewt85sebi.data_stat_cd data_stat_cd,
ewt85sebi.cre_dt cre_dt,
ewt85sebi.last_chg_dt last_chg_dt,
ewt85sebi.last_chg_user_nm last_chg_user_nm,
'D' as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt85sebi' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv`.`ewt14sgnp`
   join `fsa-{env}-cnsv-cdc`.`ewt85sebi` on (ewt14sgnp.sgnp_id = ewt85sebi.sgnp_id) 
   left join `fsa-{env}-cnsv`.`ewt94nebi` on (ewt85sebi.ntl_ebi_fctr_id = ewt94nebi.ntl_ebi_fctr_id 
                           and ewt85sebi.ntl_ebi_sfctr_id = ewt94nebi.ntl_ebi_sfctr_id)
where ewt85sebi.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
null sgnp_nbr,
null sgnp_type_desc,
null sgnp_stype_desc,
null sgnp_stype_agr_nm,
null ntl_ebi_nm,
ewt85sebi.ntl_ebi_fctr_id ntl_ebi_fctr_id,
ewt85sebi.ntl_ebi_sfctr_id ntl_ebi_sfctr_id,
ewt85sebi.ebi_sfctr_nm ebi_sfctr_nm,
ewt85sebi.sgnp_id sgnp_id,
ewt85sebi.data_stat_cd data_stat_cd,
ewt85sebi.cre_dt cre_dt,
ewt85sebi.last_chg_dt last_chg_dt,
ewt85sebi.last_chg_user_nm last_chg_user_nm,
ewt85sebi.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt85sebi' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`ewt85sebi`
where ewt85sebi.op = 'D'
