INSERT into
EDV.CROP_TR_CTR_L(CROP_TR_CTR_L_ID ,
TR_H_ID,
PGM_ABR,
FSA_CROP_CD,
FSA_CROP_TYPE_CD,
LOAD_DT,
DATA_SRC_NM)
(
select sub.*
from edv.tr_h th 
inner join (
select 
stg.CROP_TR_CTR_L_ID,
stg.TR_H_ID,
stg.PGM_ABR,
stg.FSA_CROP_CD,
stg.FSA_CROP_TYPE_CD,
stg.LOAD_DT,
stg.DATA_SRC_NM
from (
SELECT DISTINCT
MD5(upper(coalesce(CROP_TR_CTR.ST_FSA_CD,'--')||'~~'||coalesce(CROP_TR_CTR.CNTY_FSA_CD,'--')||'~~'||coalesce((CROP_TR_CTR.TR_NBR)::numeric ,-1))) TR_H_ID,
coalesce(CROP_TR_CTR.PGM_ABR, '--') PGM_ABR,
coalesce(CROP_TR_CTR.FSA_CROP_CD, '--') FSA_CROP_CD,
coalesce(CROP_TR_CTR.FSA_CROP_TYPE_CD, '--') FSA_CROP_TYPE_CD,
CROP_TR_CTR.LOAD_DT LOAD_DT,
CROP_TR_CTR.DATA_SRC_NM DATA_SRC_NM,
ROW_NUMBER() OVER (PARTITION BY MD5(upper(coalesce(CROP_TR_CTR.ST_FSA_CD,'--')||'~~'||coalesce(CROP_TR_CTR.CNTY_FSA_CD,'--')||'~~'||coalesce((CROP_TR_CTR.TR_NBR)::numeric ,-1))),
coalesce(CROP_TR_CTR.PGM_ABR, '--') ,
coalesce(CROP_TR_CTR.FSA_CROP_CD, '--') ,
coalesce(CROP_TR_CTR.FSA_CROP_TYPE_CD, '--')  ORDER BY CROP_TR_CTR.CDC_DT desc, CROP_TR_CTR.LOAD_DT desc) STG_EFF_DT_RANK,
MD5(MD5(upper(coalesce(CROP_TR_CTR.ST_FSA_CD,'--')||'~~'||coalesce(CROP_TR_CTR.CNTY_FSA_CD,'--')||'~~'||coalesce((CROP_TR_CTR.TR_NBR)::numeric ,-1)))||'~~'||upper(coalesce(trim(both CROP_TR_CTR.PGM_ABR), '--')) ||'~~'||upper(coalesce(trim(both CROP_TR_CTR.FSA_CROP_CD), '--')) ||'~~'||upper(coalesce(trim(both CROP_TR_CTR.FSA_CROP_TYPE_CD), '--')) ) CROP_TR_CTR_L_ID
FROM SQL_FARM_RCD_STG.CROP_TR_CTR
where CROP_TR_CTR.cdc_oper_cd IN ('I','UN','D')
) stg 
LEFT JOIN  EDV.CROP_TR_CTR_L dv
 ON (
stg.CROP_TR_CTR_L_ID = dv.CROP_TR_CTR_L_ID
)
where (
dv.CROP_TR_CTR_L_ID IS NULL
)
and stg.STG_EFF_DT_RANK = 1
) sub
on th.TR_H_ID = sub.TR_H_ID
)
;