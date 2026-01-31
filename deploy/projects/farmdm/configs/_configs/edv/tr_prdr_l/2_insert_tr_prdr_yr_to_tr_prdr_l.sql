INSERT into
EDV.TR_PRDR_L(TR_PRDR_L_ID ,
CORE_CUST_ID,
FARM_H_ID,
TR_H_ID,
LOAD_DT,
DATA_SRC_NM)
(
select 
stg.TR_PRDR_L_ID,
stg.CORE_CUST_ID,
stg.FARM_H_ID,
stg.TR_H_ID,
stg.LOAD_DT,
stg.DATA_SRC_NM
from (
SELECT DISTINCT
coalesce(TR_PRDR_YR.CORE_CUST_ID, '-1') CORE_CUST_ID,
(CASE WHEN FH.FARM_H_ID IS NOT NULL THEN MD5(upper(coalesce(TR_PRDR_YR.ST_FSA_CD,'--')||'~~'||coalesce(TR_PRDR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce(TR_PRDR_YR.FARM_NBR,'--'))) ELSE MD5('--'||'~~'||'--'||'~~'||'--') END) FARM_H_ID,
(CASE WHEN TH.TR_H_ID IS NOT NULL THEN MD5(upper(coalesce(TR_PRDR_YR.ST_FSA_CD ,'--')||'~~'||coalesce(TR_PRDR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce(TR_PRDR_YR.TR_NBR, '-1'))) ELSE MD5('--'||'~~'||'--'||'~~'||'-1') END) TR_H_ID,
TR_PRDR_YR.LOAD_DT LOAD_DT,
TR_PRDR_YR.DATA_SRC_NM DATA_SRC_NM,
ROW_NUMBER() OVER (PARTITION BY coalesce(TR_PRDR_YR.CORE_CUST_ID, '-1') ,
MD5(upper(coalesce(TR_PRDR_YR.ST_FSA_CD,'--')||'~~'||coalesce(TR_PRDR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce(TR_PRDR_YR.FARM_NBR,'--'))),
MD5(upper(coalesce(TR_PRDR_YR.ST_FSA_CD,'--')||'~~'||coalesce(TR_PRDR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce(TR_PRDR_YR.TR_NBR, '-1'))) ORDER BY TR_PRDR_YR.CDC_DT desc, TR_PRDR_YR.LOAD_DT desc) STG_EFF_DT_RANK,
MD5(coalesce(TR_PRDR_YR.CORE_CUST_ID, '-1') ||'~~'||MD5(upper(coalesce(TR_PRDR_YR.ST_FSA_CD,'--')||'~~'||coalesce(TR_PRDR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce(TR_PRDR_YR.FARM_NBR,'--')))||'~~'||MD5(upper(coalesce(TR_PRDR_YR.ST_FSA_CD,'--')||'~~'||coalesce(TR_PRDR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce(TR_PRDR_YR.TR_NBR, '-1')))) TR_PRDR_L_ID
FROM SQL_FARM_RCD_STG.TR_PRDR_YR
LEFT JOIN EDV.FARM_H FH
ON (MD5(upper(coalesce(TR_PRDR_YR.ST_FSA_CD,'--')||'~~'||coalesce(TR_PRDR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce(TR_PRDR_YR.FARM_NBR,'--'))) = FARM_H_ID)
LEFT JOIN EDV.TR_H TH
ON (MD5(upper(coalesce(TR_PRDR_YR.ST_FSA_CD ,'--')||'~~'||coalesce(TR_PRDR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce(TR_PRDR_YR.TR_NBR, '-1'))) = TR_H_ID)
where TR_PRDR_YR.cdc_oper_cd IN ('I','UN','D')
) stg 
LEFT JOIN  EDV.TR_PRDR_L dv
 ON (
stg.TR_PRDR_L_ID = dv.TR_PRDR_L_ID
)
where (
dv.TR_PRDR_L_ID IS NULL
)
and stg.STG_EFF_DT_RANK = 1
)
;