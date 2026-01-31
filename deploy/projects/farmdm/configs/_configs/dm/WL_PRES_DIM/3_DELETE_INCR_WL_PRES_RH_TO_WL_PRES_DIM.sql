with del_data as(
SELECT DISTINCT
DM.WL_PRES_DURB_ID,
DM.DATA_EFF_STRT_DT,
DM.WL_PRES_CD,
DM.WL_PRES_NM,
DM.WL_PRES_DESC
FROM (
SELECT
WH.DURB_ID WL_PRES_DURB_ID,
coalesce(WP.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) DATA_EFF_STRT_DT,
WP.WL_PRES_CD  WL_PRES_CD,
WP.DMN_VAL_NM AS WL_PRES_NM,
WP.DMN_VAL_DESC AS WL_PRES_DESC,
ROW_NUMBER() OVER ( PARTITION BY coalesce(WP.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')), WP.WL_PRES_CD ORDER BY WP.DATA_EFF_STRT_DT DESC) AS ROW_NUM_PART
FROM EDV.WL_PRES_RS WP
LEFT JOIN EDV.WL_PRES_RH WH  ON (WH.WL_PRES_CD = coalesce(WP.WL_PRES_CD,'[NULL IN SOURCE]'))
WHERE cast(WP.DATA_EFF_END_DT as date) = TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
AND NOT EXISTS ( SELECT '1' FROM EDV.WL_PRES_RS WP_IN
				 WHERE WP_IN.WL_PRES_CD = WP.WL_PRES_CD
                 AND cast(WP_IN.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
			)
AND WH.DURB_ID IS NOT NULL              
) DM
WHERE DM.ROW_NUM_PART = 1
)
select ud.wl_pres_durb_id,
	'0' as cur_rcd_ind,
	(current_date) as data_eff_strt_dt,
	(current_date) as data_eff_end_dt, 
	(current_date) as cre_dt, 
	ud.wl_pres_cd,
	ud.wl_pres_nm,
	ud.wl_pres_desc
from del_data ud
where wl_pres_durb_id > 0
order by wl_pres_durb_id asc








