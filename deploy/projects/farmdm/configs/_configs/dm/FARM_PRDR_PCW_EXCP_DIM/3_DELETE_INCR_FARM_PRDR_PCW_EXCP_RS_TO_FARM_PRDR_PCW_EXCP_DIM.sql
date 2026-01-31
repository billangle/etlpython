with incr_FARM_PRDR_PCW_EXCP_DIM  as
          (             
            SELECT FARM_PRDR_PCW_EXCP_CD dr_id   
            FROM  EDV.FARM_PRDR_PCW_EXCP_RS
            WHERE  CAST(DATA_EFF_STRT_DT AS DATE) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
            AND TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') = CAST(data_eff_end_dt AS DATE)            
          )
, del_data as (
SELECT sub1.*
from
(
select 
dv_dr.durb_id AS FARM_PRDR_PCW_EXCP_DURB_ID
,dv_dr.FARM_PRDR_PCW_EXCP_CD AS  PCW_EXCP_CD
,RS.PCW_EXCP_NM
,RS.PCW_EXCP_DESC
,RS.data_eff_strt_dt
,COALESCE(RS.DATA_STAT_CD,'I') DATA_STAT_CD
,CASE WHEN dv_dr.durb_id > 0 THEN 0 ELSE 1 END DEF_ERR_IND,
dv_dr.FARM_PRDR_PCW_EXCP_CD BUS_KEY, 
('FARM_PRDR_PCW_EXCP_CD:'||dv_dr.FARM_PRDR_PCW_EXCP_CD) BUS_KEY_ERR
from (
select * from edv.FARM_PRDR_PCW_EXCP_RH 
where data_src_nm <> 'SYSGEN' AND FARM_PRDR_PCW_EXCP_CD 
in (select dr_id from incr_FARM_PRDR_PCW_EXCP_DIM )) dv_dr
LEFT JOIN 
(
select 
PCW_EXCP_CD,
PCW_EXCP_NM,
PCW_EXCP_DESC,
'I' DATA_STAT_CD,
data_eff_strt_dt
from
(  select 
    FARM_PRDR_PCW_EXCP_CD PCW_EXCP_CD ,
    DMN_VAL_NM PCW_EXCP_NM,
    DMN_VAL_DESC PCW_EXCP_DESC,
	data_eff_strt_dt
    from EDV.FARM_PRDR_PCW_EXCP_RS
    where TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') >= CAST(data_eff_strt_dt AS DATE)
    AND TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') = CAST(data_eff_end_dt AS DATE) 
) sub
) RS
ON (COALESCE(dv_dr.FARM_PRDR_PCW_EXCP_CD,'--') = COALESCE(RS.PCW_EXCP_CD,'--') )
) sub1
)
select farm_prdr_pcw_excp_durb_id, 
	(current_date) AS cre_dt, 
	(current_date) AS last_chg_dt,  
	data_stat_cd, 
	pcw_excp_cd, 
	pcw_excp_nm, 
	pcw_excp_desc
from del_data

