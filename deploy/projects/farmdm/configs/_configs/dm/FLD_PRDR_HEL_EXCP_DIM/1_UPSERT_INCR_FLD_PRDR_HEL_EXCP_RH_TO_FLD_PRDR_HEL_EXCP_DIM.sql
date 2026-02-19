with incr_FLD_PRDR_HEL_EXCP_DIM  as
          ( SELECT FLD_PRDR_HEL_EXCP_CD dr_id
            FROM EDV.FLD_PRDR_HEL_EXCP_RH
            WHERE   CAST(LOAD_DT AS DATE)-1 = TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
            AND data_src_nm <> 'SYSGEN'  
			
            UNION        
			
            SELECT FLD_PRDR_HEL_EXCP_CD dr_id   
            FROM  EDV.FLD_PRDR_HEL_EXCP_RS
            WHERE  CAST(DATA_EFF_STRT_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
            AND TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') < CAST(data_eff_end_dt AS DATE)            
          )
, insert_data as (
select sub1.*
from
(
select 
dv_dr.durb_id AS FLD_PRDR_HEL_EXCP_DURB_ID
,dv_dr.FLD_PRDR_HEL_EXCP_CD AS  HEL_EXCP_CD
,RS.HEL_EXCP_NM
,RS.HEL_EXCP_DESC
,COALESCE(RS.DATA_STAT_CD,'A') DATA_STAT_CD
,CASE WHEN dv_dr.durb_id > 0 THEN 0 ELSE 1 END DEF_ERR_IND,
dv_dr.FLD_PRDR_HEL_EXCP_CD BUS_KEY, 
('FLD_PRDR_HEL_EXCP_CD:'||dv_dr.FLD_PRDR_HEL_EXCP_CD) BUS_KEY_ERR
from (
select * from edv.FLD_PRDR_HEL_EXCP_RH 
where data_src_nm <> 'SYSGEN' AND FLD_PRDR_HEL_EXCP_CD  in (select dr_id from incr_FLD_PRDR_HEL_EXCP_DIM )) dv_dr
LEFT JOIN 
(
select 
HEL_EXCP_CD,
HEL_EXCP_NM,
HEL_EXCP_DESC,
'A' DATA_STAT_CD
from
(  select 
    FLD_PRDR_HEL_EXCP_CD HEL_EXCP_CD ,
    DMN_VAL_NM HEL_EXCP_NM,
    DMN_VAL_DESC HEL_EXCP_DESC
    from EDV.FLD_PRDR_HEL_EXCP_RS
    where TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') >= CAST(data_eff_strt_dt AS DATE)
    AND TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') < CAST(data_eff_end_dt AS DATE) 
) sub
) RS
ON (COALESCE(dv_dr.FLD_PRDR_HEL_EXCP_CD,'--') = COALESCE(RS.HEL_EXCP_CD,'--') )
) sub1
)
select fld_prdr_hel_excp_durb_id, 
       (current_date) AS cre_dt, 
	   (current_date) AS last_chg_dt,    
	   data_stat_cd, 
	   hel_excp_cd, 
	   hel_excp_nm, 
	   hel_excp_desc
from insert_data;
