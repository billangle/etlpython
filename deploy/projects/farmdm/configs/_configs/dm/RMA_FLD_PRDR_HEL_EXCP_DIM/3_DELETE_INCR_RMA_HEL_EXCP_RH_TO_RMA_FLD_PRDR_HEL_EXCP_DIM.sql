with incr_RMA_HEL_EXCP_DIM  as
         (  
			SELECT RMA_HEL_EXCP_CD dr_id   
            FROM  EDV.RMA_HEL_EXCP_RS
            WHERE  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') = cast(data_eff_end_dt as date)    
            AND DMN_ID=55        
        )
, del_data as ( 
select 
dv_dr.durb_id AS RMA_FLD_PRDR_HEL_EXCP_DURB_ID
,dv_dr.RMA_HEL_EXCP_CD AS  RMA_HEL_EXCP_CD
,RS.RMA_HEL_EXCP_NM
,RS.RMA_HEL_EXCP_DESC
,coalesce(RS.DATA_STAT_CD,'I') DATA_STAT_CD
,CASE WHEN dv_dr.durb_id > 0 THEN 0 ELSE 1 END DEF_ERR_IND,
dv_dr.RMA_HEL_EXCP_CD BUS_KEY, 
('RMA_HEL_EXCP_CD:'||dv_dr.RMA_HEL_EXCP_CD) BUS_KEY_ERR
from (
select * from edv.RMA_HEL_EXCP_RH 
where data_src_nm <> 'SYSGEN' AND RMA_HEL_EXCP_CD  in (select dr_id from incr_RMA_HEL_EXCP_DIM )) dv_dr
LEFT JOIN 
(
select 
RMA_HEL_EXCP_CD,
RMA_HEL_EXCP_NM,
RMA_HEL_EXCP_DESC,
'I' DATA_STAT_CD
from
(  select 
    RMA_HEL_EXCP_CD RMA_HEL_EXCP_CD ,
    RMA_HEL_EXCP_NM RMA_HEL_EXCP_NM,
    RMA_HEL_DESC RMA_HEL_EXCP_DESC
    from EDV.RMA_HEL_EXCP_RS
    where TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') >= cast(data_eff_strt_dt as date)
    AND TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') = cast(data_eff_end_dt as date) 
    AND DMN_ID =55
) sub_1
) RS
ON (coalesce(dv_dr.RMA_HEL_EXCP_CD,'--') = coalesce(RS.RMA_HEL_EXCP_CD,'--') )
)
select 	rma_fld_prdr_hel_excp_durb_id,
		(current_date) as cre_dt,
		(current_date) as last_chg_dt,
		'I' as data_stat_cd,
		rma_hel_excp_cd,
		rma_hel_excp_nm,
		rma_hel_excp_desc
from del_data 
where rma_fld_prdr_hel_excp_durb_id > 0
order by rma_fld_prdr_hel_excp_durb_id asc