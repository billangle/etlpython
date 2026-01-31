INSERT into
EDV.PGM_CROP_L(PGM_CROP_L_ID ,
PGM_ABR,
FSA_CROP_CD,
FSA_CROP_TYPE_CD,
LOAD_DT,
DATA_SRC_NM)
(
select 
stg.PGM_CROP_L_ID,
stg.PGM_ABR,
stg.FSA_CROP_CD,
stg.FSA_CROP_TYPE_CD,
stg.LOAD_DT,
stg.DATA_SRC_NM
from (
SELECT DISTINCT
coalesce(CROP.PGM_ABR, '--') PGM_ABR,
coalesce(CROP.FSA_CROP_CD, '--') FSA_CROP_CD,
coalesce(CROP.FSA_CROP_TYPE_CD, '--') FSA_CROP_TYPE_CD,
CROP.LOAD_DT LOAD_DT,
CROP.DATA_SRC_NM DATA_SRC_NM,
ROW_NUMBER() OVER (PARTITION BY coalesce(CROP.PGM_ABR, '--') ,
coalesce(CROP.FSA_CROP_CD, '--') ,
coalesce(CROP.FSA_CROP_TYPE_CD, '--')  ORDER BY CROP.CDC_DT desc, CROP.LOAD_DT desc) STG_EFF_DT_RANK,
MD5(upper(coalesce(trim(both CROP.PGM_ABR), '--')) ||'~~'||upper(coalesce(trim(both CROP.FSA_CROP_CD), '--')) ||'~~'||upper(coalesce(trim(both CROP.FSA_CROP_TYPE_CD), '--')) ) PGM_CROP_L_ID
FROM SQL_FARM_RCD_STG.CROP

where CROP.cdc_oper_cd IN ('I','UN','D')
) stg 
LEFT JOIN  EDV.PGM_CROP_L dv
 ON (
stg.PGM_CROP_L_ID = dv.PGM_CROP_L_ID
)
where (
dv.PGM_CROP_L_ID IS NULL
)
and stg.STG_EFF_DT_RANK = 1
);