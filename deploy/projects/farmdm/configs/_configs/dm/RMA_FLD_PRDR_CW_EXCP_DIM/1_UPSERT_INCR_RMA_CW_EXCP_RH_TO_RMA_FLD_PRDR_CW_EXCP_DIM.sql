with incr_FLD_PRDR_CW_EXCP_DIM  as
         (  SELECT FLD_PRDR_CW_EXCP_CD dr_id
            FROM EDV.FLD_PRDR_CW_EXCP_RH
            WHERE   cast(LOAD_DT as date)-1 = TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
            AND data_src_nm <> 'SYSGEN' 
			
            UNION     
			
            SELECT FLD_PRDR_CW_EXCP_CD dr_id   
            FROM  EDV.FLD_PRDR_CW_EXCP_RS
            WHERE  cast(DATA_EFF_STRT_DT as date) = TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
            AND TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') < cast(data_eff_end_dt as date)      
            AND DMN_ID=56      
        )
, upsert_data as (
select 
dv_dr.durb_id AS RMA_FLD_PRDR_CW_EXCP_DURB_ID
,dv_dr.RMA_CW_EXCP_CD AS  RMA_CW_EXCP_CD
,RS.RMA_CW_EXCP_NM
,RS.RMA_CW_EXCP_DESC
,coalesce(RS.DATA_STAT_CD,'A') DATA_STAT_CD
,CASE WHEN dv_dr.durb_id > 0 THEN 0 ELSE 1 END DEF_ERR_IND,
dv_dr.RMA_CW_EXCP_CD BUS_KEY, 
('RMA_CW_EXCP_CD:'||dv_dr.RMA_CW_EXCP_CD) BUS_KEY_ERR
from (
select * from edv.RMA_CW_EXCP_RH 
where data_src_nm <> 'SYSGEN' AND RMA_CW_EXCP_CD in (select dr_id from incr_FLD_PRDR_CW_EXCP_DIM )) dv_dr
LEFT JOIN 
(
select 
RMA_CW_EXCP_CD,
RMA_CW_EXCP_NM,
RMA_CW_EXCP_DESC,
'A' DATA_STAT_CD
from
(  select 
    RMA_CW_EXCP_CD RMA_CW_EXCP_CD ,
    RMA_CW_EXCP_NM RMA_CW_EXCP_NM,
    RMA_CW_EXCP_DESC RMA_CW_EXCP_DESC
    from EDV.RMA_CW_EXCP_RS
    where TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') >= cast(data_eff_strt_dt as date)
    AND TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') < cast(data_eff_end_dt as date) 
    AND DMN_ID=56 
) sub_1
) RS
ON (coalesce(dv_dr.RMA_CW_EXCP_CD,'--') = coalesce(RS.RMA_CW_EXCP_CD,'--') )
)
select 	rma_fld_prdr_cw_excp_durb_id,
		(current_date) as cre_dt,
		(current_date) as last_chg_dt,
		'A' as data_stat_cd,
		rma_cw_excp_cd,
		rma_cw_excp_nm,
		rma_cw_excp_desc
from upsert_data
where rma_fld_prdr_cw_excp_durb_id > 0
order by rma_fld_prdr_cw_excp_durb_id asc
