with incr_FLD_PRDR_CW_EXCP_DIM  as
            (             
            SELECT FLD_PRDR_CW_EXCP_CD dr_id   
            FROM  EDV.FLD_PRDR_CW_EXCP_RS
            WHERE  CAST(DATA_EFF_STRT_DT AS DATE) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
            AND TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') = CAST(data_eff_end_dt AS DATE)            
            )
, insert_data as (
select 
dv_dr.durb_id AS FLD_PRDR_CW_EXCP_DURB_ID
,dv_dr.FLD_PRDR_CW_EXCP_CD AS  CW_EXCP_CD
,RS.CW_EXCP_NM
,RS.CW_EXCP_DESC
,RS.data_eff_strt_dt
,COALESCE(RS.DATA_STAT_CD,'I') DATA_STAT_CD
,CASE WHEN dv_dr.durb_id > 0 THEN 0 ELSE 1 END DEF_ERR_IND,
dv_dr.FLD_PRDR_CW_EXCP_CD BUS_KEY, 
('FLD_PRDR_CW_EXCP_CD:'||dv_dr.FLD_PRDR_CW_EXCP_CD) BUS_KEY_ERR
from (
select * from edv.FLD_PRDR_CW_EXCP_RH 
where data_src_nm <> 'SYSGEN' AND FLD_PRDR_CW_EXCP_CD  in (select dr_id from incr_FLD_PRDR_CW_EXCP_DIM )) dv_dr
LEFT JOIN 
(
select 
CW_EXCP_CD,
CW_EXCP_NM,
CW_EXCP_DESC,
'I' DATA_STAT_CD,
data_eff_strt_dt
from
(  select 
    FLD_PRDR_CW_EXCP_CD CW_EXCP_CD ,
    DMN_VAL_NM CW_EXCP_NM,
    DMN_VAL_DESC CW_EXCP_DESC,
	data_eff_strt_dt
    from EDV.FLD_PRDR_CW_EXCP_RS
    where TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') >= CAST(data_eff_strt_dt AS DATE)
    AND TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') = CAST(data_eff_end_dt AS DATE) 
) sub
) RS
ON (COALESCE(dv_dr.FLD_PRDR_CW_EXCP_CD,'--') = COALESCE(RS.CW_EXCP_CD,'--') )
)
SELECT fld_prdr_cw_excp_durb_id, 
       (current_date) as cre_dt, 
	   (current_date) as last_chg_dt,   
	   data_stat_cd, 
	   cw_excp_cd, 
	   cw_excp_nm, 
	   cw_excp_desc
FROM insert_data