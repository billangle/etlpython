-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_EWT94NEBI (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_EWT94NEBI/incremental/CNSV_EWT94NEBI.sql
-- =============================================================================

select distinct
ewt94nebi.ntl_ebi_nm ntl_ebi_nm,
ewt94nebi.ntl_ebi_fctr_id ntl_ebi_fctr_id,
ewt94nebi.ntl_ebi_sfctr_id ntl_ebi_sfctr_id,
ewt94nebi.ntl_ebi_desc ntl_ebi_desc,
ewt94nebi.prac_base_ind prac_base_ind,
ewt94nebi.data_stat_cd data_stat_cd,
ewt94nebi.cre_dt cre_dt,
ewt94nebi.last_chg_dt last_chg_dt,
ewt94nebi.last_chg_user_nm last_chg_user_nm,
ewt94nebi.op,
current_timestamp() as load_dt,
'CNSV_EWT94NEBI' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`ewt94nebi`
where ewt94nebi.op <> 'D'
and ewt94nebi.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select distinct
ewt94nebi.ntl_ebi_nm ntl_ebi_nm,
ewt94nebi.ntl_ebi_fctr_id ntl_ebi_fctr_id,
ewt94nebi.ntl_ebi_sfctr_id ntl_ebi_sfctr_id,
ewt94nebi.ntl_ebi_desc ntl_ebi_desc,
ewt94nebi.prac_base_ind prac_base_ind,
ewt94nebi.data_stat_cd data_stat_cd,
ewt94nebi.cre_dt cre_dt,
ewt94nebi.last_chg_dt last_chg_dt,
ewt94nebi.last_chg_user_nm last_chg_user_nm,
ewt94nebi.op,
current_timestamp() as load_dt,
'CNSV_EWT94NEBI' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv`.`ewt94nebi`
where ewt94nebi.op = 'D'
