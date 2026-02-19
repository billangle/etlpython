with incr_FLD_PRDR_PCW_EXCP_DIM  as
            (            
            SELECT FLD_PRDR_PCW_EXCP_CD dr_id   
            FROM  EDV.FLD_PRDR_PCW_EXCP_RS
            WHERE  CAST(DATA_EFF_STRT_DT AS DATE) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
            AND TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') = CAST(data_eff_end_dt AS DATE)            
            )
, del_data as (
select sub1.*
from 
(
select 
dv_dr.durb_id AS FLD_PRDR_PCW_EXCP_DURB_ID
,dv_dr.FLD_PRDR_PCW_EXCP_CD AS  PCW_EXCP_CD
,RS.PCW_EXCP_NM
,RS.PCW_EXCP_DESC
,RS.DATA_EFF_STRT_DT
,COALESCE(RS.DATA_STAT_CD,'I') DATA_STAT_CD
,CASE WHEN dv_dr.durb_id > 0 THEN 0 ELSE 1 END DEF_ERR_IND,
dv_dr.FLD_PRDR_PCW_EXCP_CD BUS_KEY, 
('FLD_PRDR_PCW_EXCP_CD:'||dv_dr.FLD_PRDR_PCW_EXCP_CD) BUS_KEY_ERR
from (
select * from edv.FLD_PRDR_PCW_EXCP_RH 
where data_src_nm <> 'SYSGEN' AND FLD_PRDR_PCW_EXCP_CD  in (select dr_id from incr_FLD_PRDR_PCW_EXCP_DIM )) dv_dr
LEFT JOIN 
(
select 
PCW_EXCP_CD,
PCW_EXCP_NM,
PCW_EXCP_DESC,
'I' DATA_STAT_CD,
DATA_EFF_STRT_DT
from
(  select 
    FLD_PRDR_PCW_EXCP_CD PCW_EXCP_CD ,
    DMN_VAL_NM PCW_EXCP_NM,
    DMN_VAL_DESC PCW_EXCP_DESC,
	DATA_EFF_STRT_DT
    from EDV.FLD_PRDR_PCW_EXCP_RS
    where TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') >= CAST(data_eff_strt_dt AS DATE)
    AND TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') = CAST(data_eff_end_dt AS DATE) 
) sub
) RS
ON (COALESCE(dv_dr.FLD_PRDR_PCW_EXCP_CD,'--') = COALESCE(RS.PCW_EXCP_CD,'--') )
) sub1
 
)				 
select fld_prdr_pcw_excp_durb_id, 
       (current_date) AS cre_dt, 
	   (current_date) AS last_chg_dt,  
	   'I' AS data_stat_cd, 
	   pcw_excp_cd, 
	   pcw_excp_nm, 
	   pcw_excp_desc
from del_data;