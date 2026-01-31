INSERT  into
EDV.FARM_PRDR_L(FARM_PRDR_L_ID ,
FARM_H_ID,
CORE_CUST_ID,
LOAD_DT,
DATA_SRC_NM)
(
select 
stg.FARM_PRDR_L_ID,
stg.FARM_H_ID,
stg.CORE_CUST_ID,
stg.LOAD_DT,
stg.DATA_SRC_NM
from (
SELECT DISTINCT
(CASE WHEN FH.FARM_H_ID IS NOT NULL THEN MD5(upper(coalesce(FARM_PRDR_YR.ST_FSA_CD ,'--')||'~~'||coalesce(FARM_PRDR_YR.CNTY_FSA_CD,'--')
    ||'~~'||coalesce(FARM_PRDR_YR.FARM_NBR ,'--'))) ELSE MD5('--'||'~~'||'--'||'~~'||'--') END) FARM_H_ID,
coalesce(FARM_PRDR_YR.CORE_CUST_ID, '-1') CORE_CUST_ID,
FARM_PRDR_YR.LOAD_DT LOAD_DT,
FARM_PRDR_YR.DATA_SRC_NM DATA_SRC_NM,
ROW_NUMBER() OVER (PARTITION BY MD5(upper(coalesce(FARM_PRDR_YR.ST_FSA_CD ,'--')||'~~'||coalesce(FARM_PRDR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce(FARM_PRDR_YR.FARM_NBR ,'--'))),
coalesce(FARM_PRDR_YR.CORE_CUST_ID, '-1')  ORDER BY FARM_PRDR_YR.CDC_DT desc, FARM_PRDR_YR.LOAD_DT desc) STG_EFF_DT_RANK,
MD5(MD5(upper(coalesce(FARM_PRDR_YR.ST_FSA_CD ,'--')||'~~'||coalesce(FARM_PRDR_YR.CNTY_FSA_CD,'--')||'~~'||coalesce(FARM_PRDR_YR.FARM_NBR ,'--')))||'~~'||coalesce(FARM_PRDR_YR.CORE_CUST_ID, '-1') ) FARM_PRDR_L_ID
FROM SQL_FARM_RCD_STG.FARM_PRDR_YR
LEFT JOIN EDV.FARM_H FH
ON (MD5(upper(coalesce(FARM_PRDR_YR.ST_FSA_CD ,'--')||'~~'||coalesce(FARM_PRDR_YR.CNTY_FSA_CD,'--')
    ||'~~'||coalesce(FARM_PRDR_YR.FARM_NBR ,'--'))) = FARM_H_ID)
where FARM_PRDR_YR.cdc_oper_cd IN ('I','UN','D')
) stg 
LEFT JOIN  EDV.FARM_PRDR_L dv
 ON (
stg.FARM_PRDR_L_ID = dv.FARM_PRDR_L_ID
)
where (
dv.FARM_PRDR_L_ID IS NULL
)
and stg.STG_EFF_DT_RANK = 1
)
;