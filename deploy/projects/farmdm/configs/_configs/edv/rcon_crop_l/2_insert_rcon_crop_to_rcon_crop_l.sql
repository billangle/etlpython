INSERT into
EDV.RCON_CROP_L(RCON_CROP_L_ID ,
PGM_ABR,
FSA_CROP_CD,
FSA_CROP_TYPE_CD,
ST_FSA_CD,
CNTY_FSA_CD,
LOAD_DT,
DATA_SRC_NM)
(
select 
stg.RCON_CROP_L_ID,
stg.PGM_ABR,
stg.FSA_CROP_CD,
stg.FSA_CROP_TYPE_CD,
stg.ST_FSA_CD,
stg.CNTY_FSA_CD,
stg.LOAD_DT,
stg.DATA_SRC_NM
from (
SELECT DISTINCT
coalesce(RCON_CROP.PGM_ABR, '--') PGM_ABR,
coalesce(RCON_CROP.FSA_CROP_CD, '--') FSA_CROP_CD,
coalesce(RCON_CROP.FSA_CROP_TYPE_CD, '--') FSA_CROP_TYPE_CD,
coalesce(RCON_CROP.ST_FSA_CD, '--') ST_FSA_CD,
coalesce(RCON_CROP.CNTY_FSA_CD, '--') CNTY_FSA_CD,
RCON_CROP.LOAD_DT LOAD_DT,
RCON_CROP.DATA_SRC_NM DATA_SRC_NM,
ROW_NUMBER() OVER (PARTITION BY coalesce(RCON_CROP.PGM_ABR, '--') ,
coalesce(RCON_CROP.FSA_CROP_CD, '--') ,
coalesce(RCON_CROP.FSA_CROP_TYPE_CD, '--') ,
coalesce(RCON_CROP.ST_FSA_CD, '--') ,
coalesce(RCON_CROP.CNTY_FSA_CD, '--')  ORDER BY RCON_CROP.CDC_DT desc, RCON_CROP.LOAD_DT desc) STG_EFF_DT_RANK,
MD5(upper(coalesce(trim(both RCON_CROP.PGM_ABR), '--')) ||'~~'||upper(coalesce(trim(both RCON_CROP.FSA_CROP_CD), '--')) ||'~~'||upper(coalesce(trim(both RCON_CROP.FSA_CROP_TYPE_CD), '--')) ||'~~'||upper(coalesce(trim(both RCON_CROP.ST_FSA_CD), '--')) ||'~~'||upper(coalesce(trim(both RCON_CROP.CNTY_FSA_CD), '--')) ) RCON_CROP_L_ID
FROM SQL_FARM_RCD_STG.RCON_CROP
where RCON_CROP.cdc_oper_cd IN ('I','UN','D')
) stg 
LEFT JOIN  EDV.RCON_CROP_L dv
 ON (
stg.RCON_CROP_L_ID = dv.RCON_CROP_L_ID
)
where (
dv.RCON_CROP_L_ID IS NULL
)
and stg.STG_EFF_DT_RANK = 1
)
;