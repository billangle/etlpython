INSERT  into
EDV.FARM_TR_L(FARM_TR_L_ID ,
FARM_H_ID,
TR_H_ID,
LOAD_DT,
DATA_SRC_NM)
(
select 
stg.FARM_TR_L_ID,
stg.FARM_H_ID,
stg.TR_H_ID,
stg.LOAD_DT,
stg.DATA_SRC_NM
from (
SELECT DISTINCT
(CASE WHEN FH.FARM_H_ID IS NOT NULL THEN MD5(upper(coalesce(TR_YR.ST_FSA_CD,'--')||'~~'||coalesce(TR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce(TR_YR.FARM_NBR,'--'))) ELSE MD5('--'||'~~'||'--'||'~~'||'--') END) FARM_H_ID,
(CASE WHEN TH.TR_H_ID IS NOT NULL THEN MD5(upper(coalesce(TR_YR.ST_FSA_CD ,'--')||'~~'||coalesce(TR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce(TR_YR.TR_NBR, '-1'))) ELSE MD5('--'||'~~'||'--'||'~~'||'-1') END) TR_H_ID,
TR_YR.LOAD_DT LOAD_DT,
TR_YR.DATA_SRC_NM DATA_SRC_NM,
ROW_NUMBER() OVER (PARTITION BY MD5(upper(coalesce(TR_YR.ST_FSA_CD ,'--')||'~~'||coalesce(TR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce(TR_YR.FARM_NBR ,'--'))),
MD5(upper(coalesce(TR_YR.ST_FSA_CD ,'--')||'~~'||coalesce(TR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce(TR_YR.TR_NBR, '-1'))) ORDER BY TR_YR.CDC_DT desc, TR_YR.LOAD_DT desc) STG_EFF_DT_RANK,
MD5(MD5(upper(coalesce(TR_YR.ST_FSA_CD ,'--')||'~~'||coalesce(TR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce(TR_YR.FARM_NBR ,'--')))||'~~'||MD5(upper(coalesce(TR_YR.ST_FSA_CD ,'--')||'~~'||coalesce(TR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce(TR_YR.TR_NBR, '-1')))) FARM_TR_L_ID
FROM SQL_FARM_RCD_STG.TR_YR
LEFT JOIN EDV.FARM_H FH
ON (MD5(upper(coalesce(TR_YR.ST_FSA_CD,'--')||'~~'||coalesce(TR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce(TR_YR.FARM_NBR,'--'))) = FARM_H_ID)
LEFT JOIN EDV.TR_H TH
ON (MD5(upper(coalesce(TR_YR.ST_FSA_CD ,'--')||'~~'||coalesce(TR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce(TR_YR.TR_NBR, '-1'))) = TR_H_ID)
where TR_YR.cdc_oper_cd IN ('I','UN','D')
) stg 
LEFT JOIN  EDV.FARM_TR_L dv
 ON (
stg.FARM_TR_L_ID = dv.FARM_TR_L_ID
)
where (
dv.FARM_TR_L_ID IS NULL
)
and stg.STG_EFF_DT_RANK = 1
)
;