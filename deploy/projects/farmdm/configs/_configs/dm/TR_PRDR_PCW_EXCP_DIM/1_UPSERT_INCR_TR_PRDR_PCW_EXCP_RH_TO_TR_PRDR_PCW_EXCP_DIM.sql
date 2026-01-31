with incr_TR_PRDR_PCW_EXCP_DIM  as
        ( SELECT TR_PRDR_PCW_EXCP_CD dr_id
            FROM EDV.TR_PRDR_PCW_EXCP_RH
            WHERE   cast(LOAD_DT as date)-1 = TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
            AND data_src_nm <> 'SYSGEN'                       
            
			UNION            
            
			SELECT TR_PRDR_PCW_EXCP_CD dr_id   
            FROM  EDV.TR_PRDR_PCW_EXCP_RS
            WHERE  cast(DATA_EFF_STRT_DT as date) = TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
            AND TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') < cast(data_eff_end_dt as date)            
        )
, upsert_data as (
select 
dv_dr.durb_id AS TR_PRDR_PCW_EXCP_DURB_ID
,dv_dr.TR_PRDR_PCW_EXCP_CD AS  PCW_EXCP_CD
,RS.PCW_EXCP_NM
,RS.PCW_EXCP_DESC
,coalesce(RS.DATA_STAT_CD,'A') DATA_STAT_CD
,CASE WHEN dv_dr.durb_id > 0 THEN 0 ELSE 1 END DEF_ERR_IND,
dv_dr.TR_PRDR_PCW_EXCP_CD BUS_KEY, 
('TR_PRDR_PCW_EXCP_CD:'||dv_dr.TR_PRDR_PCW_EXCP_CD) BUS_KEY_ERR
from (
select * from edv.TR_PRDR_PCW_EXCP_RH 
where data_src_nm <> 'SYSGEN' AND TR_PRDR_PCW_EXCP_CD  in (select dr_id from incr_TR_PRDR_PCW_EXCP_DIM )) dv_dr
LEFT JOIN 
(
select 
PCW_EXCP_CD,
PCW_EXCP_NM,
PCW_EXCP_DESC,
'A' DATA_STAT_CD
from
(  select 
    TR_PRDR_PCW_EXCP_CD PCW_EXCP_CD ,
    DMN_VAL_NM PCW_EXCP_NM,
    DMN_VAL_DESC PCW_EXCP_DESC
    from EDV.TR_PRDR_PCW_EXCP_RS
    where TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') >= cast(data_eff_strt_dt as date)
    AND TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') < cast(data_eff_end_dt as date) 
) sub_1
) RS
ON (coalesce(dv_dr.TR_PRDR_PCW_EXCP_CD,'--') = coalesce(RS.PCW_EXCP_CD,'--') )
)
select 	tr_prdr_pcw_excp_durb_id,
		(current_date) as cre_dt,
		(current_date) as last_chg_dt,
		'A' as data_stat_cd,
		pcw_excp_cd,
		pcw_excp_nm,
		pcw_excp_desc
from upsert_data
where tr_prdr_pcw_excp_durb_id > 0
order by tr_prdr_pcw_excp_durb_id asc
