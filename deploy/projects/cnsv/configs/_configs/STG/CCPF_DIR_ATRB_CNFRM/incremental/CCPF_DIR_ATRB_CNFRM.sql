/** CCPF_DIR_ATRB_CNFRM SRC_INCR_SQL **/-- Julia Lu made edition - convert to spark sql for Glue job purpose
-- Stage SQL: CCPF_DIR_ATRB_CNFRM (incremental)
-- Location: s3://c108-cert-fpacfsa-final-zone/cnsv/_configs/stg/CCPF_DIR_ATRB_CNFRM/incremental/CCPF_DIR_ATRB_CNFRM.sql
-- =============================================================================

select * from
(
 select 
 pgm_yr,
 st_fsa_cd,
 cnty_fsa_cd,
 sgnp_sub_cat_nm,
 app_id,
 core_cust_id,
 dir_atrb_cnfrm_id,
 pymt_pgm_type_shrt_nm,
 dir_atrb_cnfrm_nbr,
 pymt_evnt_id,
 data_stat_cd,
 cre_dt,
 last_chg_dt,
 last_chg_user_nm,
 ctr_id,
 supl_ctr_type_abr,
 cdc_oper_cd,
 load_dt,
 data_src_nm,
 cdc_dt,
 row_number() over ( partition by
 dir_atrb_cnfrm_id
 order by tbl_priority asc, last_chg_dt desc ) as row_num_part
 from 
 (
select payment_entity.program_year pgm_yr,
payment_entity.state_fsa_code st_fsa_cd,
payment_entity.county_fsa_code cnty_fsa_cd,
case
when payment_entity.payment_program_type_short_name like 'crp%' then signup_sub_category.signup_sub_category_name
when payment_entity.payment_program_type_short_name like 'efcrp%' then ewt14sgnp.sgnp_stype_desc
end sgnp_sub_cat_nm,
payment_entity.application_identifier app_id,
payment_entity.core_customer_identifier core_cust_id,
direct_attribution_confirmation.direct_attribution_confirmation_identifier dir_atrb_cnfrm_id,
payment_entity.payment_program_type_short_name pymt_pgm_type_shrt_nm,
direct_attribution_confirmation.direct_attribution_confirmation_number dir_atrb_cnfrm_nbr,
direct_attribution_confirmation.payment_event_identifier pymt_evnt_id,
direct_attribution_confirmation.data_status_code data_stat_cd,
direct_attribution_confirmation.creation_date cre_dt,
direct_attribution_confirmation.last_change_date last_chg_dt,
direct_attribution_confirmation.last_change_user_name last_chg_user_nm,
master_contract.contract_identifier ctr_id,
master_contract.supplemental_contract_type_abbreviation supl_ctr_type_abr,
direct_attribution_confirmation.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CCPF_DIR_ATRB_CNFRM' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-ccpf-cdc`.`direct_attribution_confirmation`
left join `fsa-{env}-ccpf`.`payment_event`
on (direct_attribution_confirmation.payment_event_identifier=payment_event.payment_event_identifier )
left join `fsa-{env}-ccpf`.`payment_event`
on (payment_event.payment_entity_identifier= payment_entity.payment_entity_identifier)
left join (select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where ewt40ofrsc.cnsv_ctr_seq_nbr > 0 and ewt40ofrsc.cnsv_ctr_seq_nbr is not null) ewt40ofrsc
on (payment_entity.state_fsa_code = ewt40ofrsc.adm_st_fsa_cd
and payment_entity.county_fsa_code = ewt40ofrsc.adm_cnty_fsa_cd
and payment_entity.application_identifier = ltrim(rtrim(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as string))))
left join `fsa-{env}-cnsv`.`ewt14sgnp`
on ( ewt40ofrsc.sgnp_id= ewt14sgnp.sgnp_id )
left join `fsa-{env}-ccms`.`master_contract` master_contract 
on (payment_entity.state_fsa_code = master_contract.administrative_state_fsa_code 
and payment_entity.county_fsa_code = master_contract.administrative_county_fsa_code 
and (( payment_entity.payment_program_type_short_name in ( 'ecp-csreg' , 'efrp-csreg' )
and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
and master_contract.contract_number is null
)
or
(ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) in ( 'crp-csreg', 'crp-cspip', 'crp-csfmi' )
 and ltrim(rtrim(if(regexp_instr('%/%', payment_entity.application_identifier) = 0, payment_entity.application_identifier, left(payment_entity.application_identifier, regexp_instr('%/%', payment_entity.application_identifier) - 2)))) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.supplemental_contract_type_abbreviation is null
)
or
(ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) = 'crp-tip'
 and coalesce(master_contract.supplemental_contract_type_abbreviation, '') = 'tip'
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
)
or
(ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) not in ( 'ecp-csreg' , 'efrp-csreg', 'crp-csreg', 'crp-cspip', 'crp-csfmi', 'crp-tip' )
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.supplemental_contract_type_abbreviation is null
)
)
)
left join `fsa-{env}-ccms`.`signup` on ( master_contract.signup_identifier= signup.signup_identifier )
left join `fsa-{env}-ccms`.`signup_sub_category` on ( signup.signup_sub_category_code= signup_sub_category.signup_sub_category_code )
where direct_attribution_confirmation.op <> 'D'
AND direct_attribution_confirmation.dart_filedate BETWEEN '{ETL_START_DATE}' AND '{ETL_END_DATE}'

union
select payment_entity.program_year pgm_yr,
payment_entity.state_fsa_code st_fsa_cd,
payment_entity.county_fsa_code cnty_fsa_cd,
case
when payment_entity.payment_program_type_short_name like 'crp%' then signup_sub_category.signup_sub_category_name
when payment_entity.payment_program_type_short_name like 'efcrp%' then ewt14sgnp.sgnp_stype_desc
end sgnp_sub_cat_nm,
payment_entity.application_identifier app_id,
payment_entity.core_customer_identifier core_cust_id,
direct_attribution_confirmation.direct_attribution_confirmation_identifier dir_atrb_cnfrm_id,
payment_entity.payment_program_type_short_name pymt_pgm_type_shrt_nm,
direct_attribution_confirmation.direct_attribution_confirmation_number dir_atrb_cnfrm_nbr,
direct_attribution_confirmation.payment_event_identifier pymt_evnt_id,
direct_attribution_confirmation.data_status_code data_stat_cd,
direct_attribution_confirmation.creation_date cre_dt,
direct_attribution_confirmation.last_change_date last_chg_dt,
direct_attribution_confirmation.last_change_user_name last_chg_user_nm,
master_contract.contract_identifier ctr_id,
master_contract.supplemental_contract_type_abbreviation supl_ctr_type_abr,
direct_attribution_confirmation.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CCPF_DIR_ATRB_CNFRM' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-ccpf`.`payment_event`
left join `fsa-{env}-ccpf`.`payment_event` on (payment_event.payment_entity_identifier= payment_entity.payment_entity_identifier)
join `fsa-{env}-ccpf-cdc`.`direct_attribution_confirmation` on (direct_attribution_confirmation.payment_event_identifier=payment_event.payment_event_identifier )
left join (select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where ewt40ofrsc.cnsv_ctr_seq_nbr > 0 and ewt40ofrsc.cnsv_ctr_seq_nbr is not null) ewt40ofrsc
on (payment_entity.state_fsa_code = ewt40ofrsc.adm_st_fsa_cd
and payment_entity.county_fsa_code = ewt40ofrsc.adm_cnty_fsa_cd
and payment_entity.application_identifier = ltrim(rtrim(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as string))))
left join `fsa-{env}-cnsv`.`ewt14sgnp` on ( ewt40ofrsc.sgnp_id= ewt14sgnp.sgnp_id )
left join `fsa-{env}-ccms`.`master_contract` master_contract
on 
(
 payment_entity.state_fsa_code = master_contract.administrative_state_fsa_code
 and payment_entity.county_fsa_code = master_contract.administrative_county_fsa_code
 and (( payment_entity.payment_program_type_short_name in ( 'ecp-csreg' , 'efrp-csreg' )
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.contract_number is null
)
or
(
 ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) in ( 'crp-csreg', 'crp-cspip', 'crp-csfmi' )
 and ltrim(rtrim(if(regexp_instr('%/%', payment_entity.application_identifier) = 0, payment_entity.application_identifier, left(payment_entity.application_identifier, regexp_instr('%/%', payment_entity.application_identifier) - 2)))) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.supplemental_contract_type_abbreviation is null
)
or
(
 ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) = 'crp-tip' 
 and coalesce(master_contract.supplemental_contract_type_abbreviation, '') = 'tip'
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
)
or
(
 ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) not in ( 'ecp-csreg' , 'efrp-csreg', 'crp-csreg', 'crp-cspip', 'crp-csfmi', 'crp-tip' )
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.supplemental_contract_type_abbreviation is null))
)
left join `fsa-{env}-ccms`.`signup`
on ( master_contract.signup_identifier= signup.signup_identifier )
left join `fsa-{env}-ccms`.`signup_sub_category`
on ( signup.signup_sub_category_code= signup_sub_category.signup_sub_category_code )
where direct_attribution_confirmation.op <> 'D'

union
select payment_entity.program_year pgm_yr,
payment_entity.state_fsa_code st_fsa_cd,
payment_entity.county_fsa_code cnty_fsa_cd,
case
when payment_entity.payment_program_type_short_name like 'crp%' then signup_sub_category.signup_sub_category_name
when payment_entity.payment_program_type_short_name like 'efcrp%' then ewt14sgnp.sgnp_stype_desc
end sgnp_sub_cat_nm,
payment_entity.application_identifier app_id,
payment_entity.core_customer_identifier core_cust_id,
direct_attribution_confirmation.direct_attribution_confirmation_identifier dir_atrb_cnfrm_id,
payment_entity.payment_program_type_short_name pymt_pgm_type_shrt_nm,
direct_attribution_confirmation.direct_attribution_confirmation_number dir_atrb_cnfrm_nbr,
direct_attribution_confirmation.payment_event_identifier pymt_evnt_id,
direct_attribution_confirmation.data_status_code data_stat_cd,
direct_attribution_confirmation.creation_date cre_dt,
direct_attribution_confirmation.last_change_date last_chg_dt,
direct_attribution_confirmation.last_change_user_name last_chg_user_nm,
master_contract.contract_identifier ctr_id,
master_contract.supplemental_contract_type_abbreviation supl_ctr_type_abr,
direct_attribution_confirmation.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CCPF_DIR_ATRB_CNFRM' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-ccpf`.`payment_event`
join `fsa-{env}-ccpf-cdc`.`direct_attribution_confirmation`
on (direct_attribution_confirmation.payment_event_identifier=payment_event.payment_event_identifier )
left join `fsa-{env}-ccpf`.`payment_event`
on (payment_event.payment_entity_identifier= payment_entity.payment_entity_identifier)
left join (select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where ewt40ofrsc.cnsv_ctr_seq_nbr > 0 and ewt40ofrsc.cnsv_ctr_seq_nbr is not null) ewt40ofrsc
on (payment_entity.state_fsa_code = ewt40ofrsc.adm_st_fsa_cd
and payment_entity.county_fsa_code = ewt40ofrsc.adm_cnty_fsa_cd
and payment_entity.application_identifier = ltrim(rtrim(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as string))))
left join `fsa-{env}-cnsv`.`ewt14sgnp`
on ( ewt40ofrsc.sgnp_id= ewt14sgnp.sgnp_id )
left join `fsa-{env}-ccms`.`master_contract` master_contract
on 
(
 payment_entity.state_fsa_code = master_contract.administrative_state_fsa_code
 and payment_entity.county_fsa_code = master_contract.administrative_county_fsa_code
 and
 (
(
 payment_entity.payment_program_type_short_name in ( 'ecp-csreg' , 'efrp-csreg' )
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.contract_number is null
)
or
(
 ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) in ( 'crp-csreg', 'crp-cspip', 'crp-csfmi' )
 and ltrim(rtrim(if(regexp_instr('%/%', payment_entity.application_identifier) = 0, payment_entity.application_identifier, left(payment_entity.application_identifier, regexp_instr('%/%', payment_entity.application_identifier) - 2)))) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.supplemental_contract_type_abbreviation is null
)
or
(
 ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) = 'crp-tip'
 and coalesce(master_contract.supplemental_contract_type_abbreviation, '') = 'tip'
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
)
or
(
 ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) not in ( 'ecp-csreg' , 'efrp-csreg', 'crp-csreg', 'crp-cspip', 'crp-csfmi', 'crp-tip' )
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.supplemental_contract_type_abbreviation is null
)
)
)
left join `fsa-{env}-ccms`.`signup`
on ( master_contract.signup_identifier= signup.signup_identifier )
left join `fsa-{env}-ccms`.`signup_sub_category`
on ( signup.signup_sub_category_code= signup_sub_category.signup_sub_category_code )
where direct_attribution_confirmation.op <> 'D'

union
select payment_entity.program_year pgm_yr,
payment_entity.state_fsa_code st_fsa_cd,
payment_entity.county_fsa_code cnty_fsa_cd,
case
when payment_entity.payment_program_type_short_name like 'crp%' then signup_sub_category.signup_sub_category_name
when payment_entity.payment_program_type_short_name like 'efcrp%' then ewt14sgnp.sgnp_stype_desc
end sgnp_sub_cat_nm,
payment_entity.application_identifier app_id,
payment_entity.core_customer_identifier core_cust_id,
direct_attribution_confirmation.direct_attribution_confirmation_identifier dir_atrb_cnfrm_id,
payment_entity.payment_program_type_short_name pymt_pgm_type_shrt_nm,
direct_attribution_confirmation.direct_attribution_confirmation_number dir_atrb_cnfrm_nbr,
direct_attribution_confirmation.payment_event_identifier pymt_evnt_id,
direct_attribution_confirmation.data_status_code data_stat_cd,
direct_attribution_confirmation.creation_date cre_dt,
direct_attribution_confirmation.last_change_date last_chg_dt,
direct_attribution_confirmation.last_change_user_name last_chg_user_nm,
master_contract.contract_identifier ctr_id,
master_contract.supplemental_contract_type_abbreviation supl_ctr_type_abr,
direct_attribution_confirmation.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CCPF_DIR_ATRB_CNFRM' as data_src_nm,
'{etl_start_date}' as cdc_dt,
4 as tbl_priority
from (select * from `fsa-{env}-cnsv`.`ewt40ofrsc_inner` where ewt40ofrsc_inner.cnsv_ctr_seq_nbr > 0 and ewt40ofrsc_inner.cnsv_ctr_seq_nbr is not null) ewt40ofrsc
left join `fsa-{env}-ccpf`.`payment_event`
on (payment_entity.state_fsa_code = ewt40ofrsc.adm_st_fsa_cd
and payment_entity.county_fsa_code = ewt40ofrsc.adm_cnty_fsa_cd
and payment_entity.application_identifier = ltrim(rtrim(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as string))))
left join `fsa-{env}-ccpf`.`payment_event`
on (payment_event.payment_entity_identifier= payment_entity.payment_entity_identifier)
join `fsa-{env}-ccpf-cdc`.`direct_attribution_confirmation`
on (direct_attribution_confirmation.payment_event_identifier=payment_event.payment_event_identifier )
left join `fsa-{env}-cnsv`.`ewt14sgnp`
on ( ewt40ofrsc.sgnp_id= ewt14sgnp.sgnp_id )
left join `fsa-{env}-ccms`.`master_contract` master_contract
on 
(
 payment_entity.state_fsa_code = master_contract.administrative_state_fsa_code
 and payment_entity.county_fsa_code = master_contract.administrative_county_fsa_code
 and
 (
(
 payment_entity.payment_program_type_short_name in ( 'ecp-csreg' , 'efrp-csreg' )
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.contract_number is null
)
or
(
 ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) in ( 'crp-csreg', 'crp-cspip', 'crp-csfmi' )
 and ltrim(rtrim(if(regexp_instr('%/%', payment_entity.application_identifier) = 0, payment_entity.application_identifier, left(payment_entity.application_identifier, regexp_instr('%/%', payment_entity.application_identifier) - 2)))) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.supplemental_contract_type_abbreviation is null
)
or
(
 ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) = 'crp-tip'
 and coalesce(master_contract.supplemental_contract_type_abbreviation, '') = 'tip'
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
)
or
(
 ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) not in ( 'ecp-csreg' , 'efrp-csreg', 'crp-csreg', 'crp-cspip', 'crp-csfmi', 'crp-tip' )
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.supplemental_contract_type_abbreviation is null
)
)
)
left join `fsa-{env}-ccms`.`signup`
on ( master_contract.signup_identifier= signup.signup_identifier )
left join `fsa-{env}-ccms`.`signup_sub_category`
on ( signup.signup_sub_category_code= signup_sub_category.signup_sub_category_code )
where direct_attribution_confirmation.op <> 'D'

union
select payment_entity.program_year pgm_yr,
payment_entity.state_fsa_code st_fsa_cd,
payment_entity.county_fsa_code cnty_fsa_cd,
case
when payment_entity.payment_program_type_short_name like 'crp%' then signup_sub_category.signup_sub_category_name
when payment_entity.payment_program_type_short_name like 'efcrp%' then ewt14sgnp.sgnp_stype_desc
end sgnp_sub_cat_nm,
payment_entity.application_identifier app_id,
payment_entity.core_customer_identifier core_cust_id,
direct_attribution_confirmation.direct_attribution_confirmation_identifier dir_atrb_cnfrm_id,
payment_entity.payment_program_type_short_name pymt_pgm_type_shrt_nm,
direct_attribution_confirmation.direct_attribution_confirmation_number dir_atrb_cnfrm_nbr,
direct_attribution_confirmation.payment_event_identifier pymt_evnt_id,
direct_attribution_confirmation.data_status_code data_stat_cd,
direct_attribution_confirmation.creation_date cre_dt,
direct_attribution_confirmation.last_change_date last_chg_dt,
direct_attribution_confirmation.last_change_user_name last_chg_user_nm,
master_contract.contract_identifier ctr_id,
master_contract.supplemental_contract_type_abbreviation supl_ctr_type_abr,
direct_attribution_confirmation.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CCPF_DIR_ATRB_CNFRM' as data_src_nm,
'{etl_start_date}' as cdc_dt,
5 as tbl_priority
from `fsa-{env}-cnsv`.`ewt14sgnp`
left join (select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where ewt40ofrsc.cnsv_ctr_seq_nbr > 0 and ewt40ofrsc.cnsv_ctr_seq_nbr is not null) ewt40ofrsc
on ( ewt40ofrsc.sgnp_id= ewt14sgnp.sgnp_id )
left join `fsa-{env}-ccpf`.`payment_event`
on (payment_entity.state_fsa_code = ewt40ofrsc.adm_st_fsa_cd
and payment_entity.county_fsa_code = ewt40ofrsc.adm_cnty_fsa_cd
and payment_entity.application_identifier = ltrim(rtrim(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as string))))
left join `fsa-{env}-ccpf`.`payment_event`
on (payment_event.payment_entity_identifier= payment_entity.payment_entity_identifier)
join `fsa-{env}-ccpf-cdc`.`direct_attribution_confirmation`
on (direct_attribution_confirmation.payment_event_identifier=payment_event.payment_event_identifier )
left join `fsa-{env}-ccms`.`master_contract` master_contract
on 
(
 payment_entity.state_fsa_code = master_contract.administrative_state_fsa_code
 and payment_entity.county_fsa_code = master_contract.administrative_county_fsa_code
 and
 (
(
 payment_entity.payment_program_type_short_name in ( 'ecp-csreg' , 'efrp-csreg' )
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.contract_number is null
)
or
(
 ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) in ( 'crp-csreg', 'crp-cspip', 'crp-csfmi' )
 and ltrim(rtrim(if(regexp_instr('%/%', payment_entity.application_identifier) = 0, payment_entity.application_identifier, left(payment_entity.application_identifier, regexp_instr('%/%', payment_entity.application_identifier) - 2)))) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.supplemental_contract_type_abbreviation is null
)
or
(
 ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) = 'crp-tip'
 and coalesce(master_contract.supplemental_contract_type_abbreviation, '') = 'tip'
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
)
or
(
 ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) not in ( 'ecp-csreg' , 'efrp-csreg', 'crp-csreg', 'crp-cspip', 'crp-csfmi', 'crp-tip' )
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.supplemental_contract_type_abbreviation is null
)
)
)
left join `fsa-{env}-ccms`.`signup`
on ( master_contract.signup_identifier= signup.signup_identifier )
left join `fsa-{env}-ccms`.`signup_sub_category`
on ( signup.signup_sub_category_code= signup_sub_category.signup_sub_category_code )
where direct_attribution_confirmation.op <> 'D'

union
select payment_entity.program_year pgm_yr,
payment_entity.state_fsa_code st_fsa_cd,
payment_entity.county_fsa_code cnty_fsa_cd,
case
when payment_entity.payment_program_type_short_name like 'crp%' then signup_sub_category.signup_sub_category_name
when payment_entity.payment_program_type_short_name like 'efcrp%' then ewt14sgnp.sgnp_stype_desc
end sgnp_sub_cat_nm,
payment_entity.application_identifier app_id,
payment_entity.core_customer_identifier core_cust_id,
direct_attribution_confirmation.direct_attribution_confirmation_identifier dir_atrb_cnfrm_id,
payment_entity.payment_program_type_short_name pymt_pgm_type_shrt_nm,
direct_attribution_confirmation.direct_attribution_confirmation_number dir_atrb_cnfrm_nbr,
direct_attribution_confirmation.payment_event_identifier pymt_evnt_id,
direct_attribution_confirmation.data_status_code data_stat_cd,
direct_attribution_confirmation.creation_date cre_dt,
direct_attribution_confirmation.last_change_date last_chg_dt,
direct_attribution_confirmation.last_change_user_name last_chg_user_nm,
master_contract.contract_identifier ctr_id,
master_contract.supplemental_contract_type_abbreviation supl_ctr_type_abr,
direct_attribution_confirmation.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CCPF_DIR_ATRB_CNFRM' as data_src_nm,
'{etl_start_date}' as cdc_dt,
6 as tbl_priority
from `fsa-{env}-ccms`.`master_contract`
left join `fsa-{env}-ccpf`.`payment_event`
on 
(
 payment_entity.state_fsa_code = master_contract.administrative_state_fsa_code
 and payment_entity.county_fsa_code = master_contract.administrative_county_fsa_code
 and
 (
(
 payment_entity.payment_program_type_short_name in ( 'ecp-csreg' , 'efrp-csreg' )
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.contract_number is null
)
or
(
 ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) in ( 'crp-csreg', 'crp-cspip', 'crp-csfmi' )
 and ltrim(rtrim(if(regexp_instr('%/%', payment_entity.application_identifier) = 0, payment_entity.application_identifier, left(payment_entity.application_identifier, regexp_instr('%/%', payment_entity.application_identifier) - 2)))) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.supplemental_contract_type_abbreviation is null
)
or
(
 ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) = 'crp-tip'
 and coalesce(master_contract.supplemental_contract_type_abbreviation, '') = 'tip'
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
)
or
(
 ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) not in ( 'ecp-csreg' , 'efrp-csreg', 'crp-csreg', 'crp-cspip', 'crp-csfmi', 'crp-tip' )
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.supplemental_contract_type_abbreviation is null
)
)
)
left join `fsa-{env}-ccpf`.`payment_event`
on (payment_event.payment_entity_identifier= payment_entity.payment_entity_identifier)
join `fsa-{env}-ccpf-cdc`.`direct_attribution_confirmation`
on (direct_attribution_confirmation.payment_event_identifier=payment_event.payment_event_identifier )
left join `fsa-{env}-ccms`.`signup`
on ( master_contract.signup_identifier= signup.signup_identifier )
left join `fsa-{env}-ccms`.`signup_sub_category`
on ( signup.signup_sub_category_code= signup_sub_category.signup_sub_category_code )
left join (select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where ewt40ofrsc.cnsv_ctr_seq_nbr > 0 and ewt40ofrsc.cnsv_ctr_seq_nbr is not null) ewt40ofrsc
on (payment_entity.state_fsa_code = ewt40ofrsc.adm_st_fsa_cd
and payment_entity.county_fsa_code = ewt40ofrsc.adm_cnty_fsa_cd
and payment_entity.application_identifier = ltrim(rtrim(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as string))))
left join `fsa-{env}-cnsv`.`ewt14sgnp`
on ( ewt40ofrsc.sgnp_id= ewt14sgnp.sgnp_id )
where direct_attribution_confirmation.op <> 'D'

union
select payment_entity.program_year pgm_yr,
payment_entity.state_fsa_code st_fsa_cd,
payment_entity.county_fsa_code cnty_fsa_cd,
case
when payment_entity.payment_program_type_short_name like 'crp%' then signup_sub_category.signup_sub_category_name
when payment_entity.payment_program_type_short_name like 'efcrp%' then ewt14sgnp.sgnp_stype_desc
end sgnp_sub_cat_nm,
payment_entity.application_identifier app_id,
payment_entity.core_customer_identifier core_cust_id,
direct_attribution_confirmation.direct_attribution_confirmation_identifier dir_atrb_cnfrm_id,
payment_entity.payment_program_type_short_name pymt_pgm_type_shrt_nm,
direct_attribution_confirmation.direct_attribution_confirmation_number dir_atrb_cnfrm_nbr,
direct_attribution_confirmation.payment_event_identifier pymt_evnt_id,
direct_attribution_confirmation.data_status_code data_stat_cd,
direct_attribution_confirmation.creation_date cre_dt,
direct_attribution_confirmation.last_change_date last_chg_dt,
direct_attribution_confirmation.last_change_user_name last_chg_user_nm,
master_contract.contract_identifier ctr_id,
master_contract.supplemental_contract_type_abbreviation supl_ctr_type_abr,
direct_attribution_confirmation.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CCPF_DIR_ATRB_CNFRM' as data_src_nm,
'{etl_start_date}' as cdc_dt,
7 as tbl_priority
from `fsa-{env}-ccms`.`signup`
left join `fsa-{env}-ccms`.`master_contract` master_contract
on ( master_contract.signup_identifier= signup.signup_identifier )
left join `fsa-{env}-ccms`.`signup_sub_category`
on ( signup.signup_sub_category_code= signup_sub_category.signup_sub_category_code )
left join `fsa-{env}-ccpf`.`payment_event`
on 
(
 payment_entity.state_fsa_code = master_contract.administrative_state_fsa_code
 and payment_entity.county_fsa_code = master_contract.administrative_county_fsa_code
 and
 (
(
 payment_entity.payment_program_type_short_name in ( 'ecp-csreg' , 'efrp-csreg' )
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.contract_number is null
)
or
(
 ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) in ( 'crp-csreg', 'crp-cspip', 'crp-csfmi' )
 and ltrim(rtrim(if(regexp_instr('%/%', payment_entity.application_identifier) = 0, payment_entity.application_identifier, left(payment_entity.application_identifier, regexp_instr('%/%', payment_entity.application_identifier) - 2)))) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.supplemental_contract_type_abbreviation is null
)
or
(
 ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) = 'crp-tip'
 and coalesce(master_contract.supplemental_contract_type_abbreviation, '') = 'tip'
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
)
or
(
 ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) not in ( 'ecp-csreg' , 'efrp-csreg', 'crp-csreg', 'crp-cspip', 'crp-csfmi', 'crp-tip' )
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.supplemental_contract_type_abbreviation is null
)
 )
)
left join `fsa-{env}-ccpf`.`payment_event`
on (payment_event.payment_entity_identifier= payment_entity.payment_entity_identifier)
join `fsa-{env}-ccpf-cdc`.`direct_attribution_confirmation`
on (direct_attribution_confirmation.payment_event_identifier=payment_event.payment_event_identifier )
left join (select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where ewt40ofrsc.cnsv_ctr_seq_nbr > 0 and ewt40ofrsc.cnsv_ctr_seq_nbr is not null) ewt40ofrsc
on (payment_entity.state_fsa_code = ewt40ofrsc.adm_st_fsa_cd
and payment_entity.county_fsa_code = ewt40ofrsc.adm_cnty_fsa_cd
and payment_entity.application_identifier = ltrim(rtrim(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as string))))
left join `fsa-{env}-cnsv`.`ewt14sgnp`
on ( ewt40ofrsc.sgnp_id= ewt14sgnp.sgnp_id )
where direct_attribution_confirmation.op <> 'D'

union
select payment_entity.program_year pgm_yr,
payment_entity.state_fsa_code st_fsa_cd,
payment_entity.county_fsa_code cnty_fsa_cd,
case
when payment_entity.payment_program_type_short_name like 'crp%' then signup_sub_category.signup_sub_category_name
when payment_entity.payment_program_type_short_name like 'efcrp%' then ewt14sgnp.sgnp_stype_desc
end sgnp_sub_cat_nm,
payment_entity.application_identifier app_id,
payment_entity.core_customer_identifier core_cust_id,
direct_attribution_confirmation.direct_attribution_confirmation_identifier dir_atrb_cnfrm_id,
payment_entity.payment_program_type_short_name pymt_pgm_type_shrt_nm,
direct_attribution_confirmation.direct_attribution_confirmation_number dir_atrb_cnfrm_nbr,
direct_attribution_confirmation.payment_event_identifier pymt_evnt_id,
direct_attribution_confirmation.data_status_code data_stat_cd,
direct_attribution_confirmation.creation_date cre_dt,
direct_attribution_confirmation.last_change_date last_chg_dt,
direct_attribution_confirmation.last_change_user_name last_chg_user_nm,
master_contract.contract_identifier ctr_id,
master_contract.supplemental_contract_type_abbreviation supl_ctr_type_abr,
direct_attribution_confirmation.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CCPF_DIR_ATRB_CNFRM' as data_src_nm,
'{etl_start_date}' as cdc_dt,
8 as tbl_priority
from `fsa-{env}-ccms`.`signup_sub_category`
left join `fsa-{env}-ccms`.`signup`
on ( signup.signup_sub_category_code= signup_sub_category.signup_sub_category_code )
left join `fsa-{env}-ccms`.`master_contract`
on ( master_contract.signup_identifier= signup.signup_identifier )
left join `fsa-{env}-ccpf`.`payment_event`
on 
(
 payment_entity.state_fsa_code = master_contract.administrative_state_fsa_code
 and payment_entity.county_fsa_code = master_contract.administrative_county_fsa_code
 and
 (
(
 payment_entity.payment_program_type_short_name in ( 'ecp-csreg' , 'efrp-csreg' )
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.contract_number is null
)
or
(
 ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) in ( 'crp-csreg', 'crp-cspip', 'crp-csfmi' )
 and ltrim(rtrim(if(regexp_instr('%/%', payment_entity.application_identifier) = 0, payment_entity.application_identifier, left(payment_entity.application_identifier, regexp_instr('%/%', payment_entity.application_identifier) - 2)))) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.supplemental_contract_type_abbreviation is null
)
or
(
 ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) = 'crp-tip'
 and coalesce(master_contract.supplemental_contract_type_abbreviation, '') = 'tip'
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
)
or
(
 ltrim(rtrim(coalesce(payment_entity.payment_program_type_short_name, ''))) not in ( 'ecp-csreg' , 'efrp-csreg', 'crp-csreg', 'crp-cspip', 'crp-csfmi', 'crp-tip' )
 and ltrim(rtrim(payment_entity.application_identifier)) = ltrim(rtrim(cast(master_contract.contract_number as string)))
 and master_contract.supplemental_contract_type_abbreviation is null
)
 )
)
left join `fsa-{env}-ccpf`.`payment_event`
on (payment_event.payment_entity_identifier= payment_entity.payment_entity_identifier)
join `fsa-{env}-ccpf-cdc`.`direct_attribution_confirmation`
on (direct_attribution_confirmation.payment_event_identifier=payment_event.payment_event_identifier )
left join (select * from `fsa-{env}-cnsv`.`ewt40ofrsc` where ewt40ofrsc.cnsv_ctr_seq_nbr > 0 and ewt40ofrsc.cnsv_ctr_seq_nbr is not null) ewt40ofrsc
on (payment_entity.state_fsa_code = ewt40ofrsc.adm_st_fsa_cd
and payment_entity.county_fsa_code = ewt40ofrsc.adm_cnty_fsa_cd
and payment_entity.application_identifier = ltrim(rtrim(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as string))))
left join `fsa-{env}-cnsv`.`ewt14sgnp`
on ( ewt40ofrsc.sgnp_id= ewt14sgnp.sgnp_id )
where direct_attribution_confirmation.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select null pgm_yr,
null st_fsa_cd,
null cnty_fsa_cd,
null sgnp_sub_cat_nm,
null app_id,
null core_cust_id,
direct_attribution_confirmation.direct_attribution_confirmation_identifier dir_atrb_cnfrm_id,
null pymt_pgm_type_shrt_nm,
direct_attribution_confirmation.direct_attribution_confirmation_number dir_atrb_cnfrm_nbr,
direct_attribution_confirmation.payment_event_identifier pymt_evnt_id,
direct_attribution_confirmation.data_status_code data_stat_cd,
direct_attribution_confirmation.creation_date cre_dt,
direct_attribution_confirmation.last_change_date last_chg_dt,
direct_attribution_confirmation.last_change_user_name last_chg_user_nm,
null ctr_id,
null supl_ctr_type_abr,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'CCPF_DIR_ATRB_CNFRM' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-ccpf-cdc`. `direct_attribution_confirmation`
where direct_attribution_confirmation.op = 'D'
