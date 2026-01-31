INSERT  INTO EDV.CROP_FARM_L(CROP_FARM_L_ID,
                             FARM_H_ID,
                             PGM_ABR,
                             FSA_CROP_CD,
                             FSA_CROP_TYPE_CD,
                             LOAD_DT,
                             DATA_SRC_NM)
(Select CROP_FARM_L_ID,
FARM_H_ID,
PGM_ABR,
FSA_CROP_CD,
FSA_CROP_TYPE_CD,
LOAD_DT,
DATA_SRC_NM FROM (
select  DISTINCT
stg.CROP_FARM_L_ID,
stg.FARM_H_ID,
stg.PGM_ABR,
stg.FSA_CROP_CD,
stg.FSA_CROP_TYPE_CD,
stg.LOAD_DT,
stg.DATA_SRC_NM,
Rank() OVER (PARTITION BY stg.CROP_FARM_L_ID,stg.FARM_H_ID,
stg.PGM_ABR,
stg.FSA_CROP_CD,
stg.FSA_CROP_TYPE_CD Order by stg.LOAD_DT desc) Rank
from (
SELECT DISTINCT
MD5(upper(coalesce(CROP_FARM_YR_DCP.ST_FSA_CD,'--')||'~~'||coalesce(CROP_FARM_YR_DCP.CNTY_FSA_CD,'--')||'~~'||coalesce(CROP_FARM_YR_DCP.FARM_NBR,'--'))) FARM_H_ID,
coalesce(CROP_FARM_YR_DCP.PGM_ABR, '--') PGM_ABR,
coalesce(CROP_FARM_YR_DCP.FSA_CROP_CD, '--') FSA_CROP_CD,
coalesce(CROP_FARM_YR_DCP.FSA_CROP_TYPE_CD, '--') FSA_CROP_TYPE_CD,
CROP_FARM_YR_DCP.LOAD_DT LOAD_DT,
CROP_FARM_YR_DCP.DATA_SRC_NM DATA_SRC_NM,
ROW_NUMBER() OVER (PARTITION BY MD5(upper(coalesce(CROP_FARM_YR_DCP.ST_FSA_CD,'--')||'~~'||coalesce(CROP_FARM_YR_DCP.CNTY_FSA_CD,'--')||'~~'||coalesce(CROP_FARM_YR_DCP.FARM_NBR,'--'))),
coalesce(CROP_FARM_YR_DCP.PGM_ABR, '--') ,
coalesce(CROP_FARM_YR_DCP.FSA_CROP_CD, '--') ,
coalesce(CROP_FARM_YR_DCP.FSA_CROP_TYPE_CD, '--')  ORDER BY CROP_FARM_YR_DCP.CDC_DT desc, CROP_FARM_YR_DCP.LOAD_DT desc) STG_EFF_DT_RANK,
MD5(MD5(upper(coalesce(CROP_FARM_YR_DCP.ST_FSA_CD,'--')||'~~'||coalesce(CROP_FARM_YR_DCP.CNTY_FSA_CD,'--')||'~~'||coalesce(CROP_FARM_YR_DCP.FARM_NBR,'--')))||'~~'||upper(coalesce(trim(both CROP_FARM_YR_DCP.PGM_ABR), '--')) ||'~~'||upper(coalesce(trim(both CROP_FARM_YR_DCP.FSA_CROP_CD), '--')) ||'~~'||upper(coalesce(trim(both CROP_FARM_YR_DCP.FSA_CROP_TYPE_CD), '--')) ) CROP_FARM_L_ID
FROM SQL_FARM_RCD_STG.CROP_FARM_YR_DCP
where CROP_FARM_YR_DCP.cdc_oper_cd IN ('I','UN','D')

UNION

SELECT DISTINCT
MD5(upper(coalesce(YR_ARC_PLC_PTCP_ELCT.ST_FSA_CD,'--')||'~~'||coalesce(YR_ARC_PLC_PTCP_ELCT.CNTY_FSA_CD,'--')||'~~'||coalesce(YR_ARC_PLC_PTCP_ELCT.FARM_NBR,'--'))) FARM_H_ID,
coalesce(YR_ARC_PLC_PTCP_ELCT.PGM_ABR, '--') PGM_ABR,
coalesce(YR_ARC_PLC_PTCP_ELCT.FSA_CROP_CD, '--') FSA_CROP_CD,
coalesce(YR_ARC_PLC_PTCP_ELCT.FSA_CROP_TYPE_CD, '--') FSA_CROP_TYPE_CD,
YR_ARC_PLC_PTCP_ELCT.LOAD_DT LOAD_DT,
YR_ARC_PLC_PTCP_ELCT.DATA_SRC_NM DATA_SRC_NM,
ROW_NUMBER() OVER (PARTITION BY MD5(upper(coalesce(YR_ARC_PLC_PTCP_ELCT.ST_FSA_CD,'--')||'~~'||coalesce(YR_ARC_PLC_PTCP_ELCT.CNTY_FSA_CD,'--')||'~~'||coalesce(YR_ARC_PLC_PTCP_ELCT.FARM_NBR,'--'))),
coalesce(YR_ARC_PLC_PTCP_ELCT.PGM_ABR, '--') ,
coalesce(YR_ARC_PLC_PTCP_ELCT.FSA_CROP_CD, '--') ,
coalesce(YR_ARC_PLC_PTCP_ELCT.FSA_CROP_TYPE_CD, '--')  ORDER BY YR_ARC_PLC_PTCP_ELCT.CDC_DT desc, YR_ARC_PLC_PTCP_ELCT.LOAD_DT desc) STG_EFF_DT_RANK,
MD5(MD5(upper(coalesce(YR_ARC_PLC_PTCP_ELCT.ST_FSA_CD,'--')||'~~'||coalesce(YR_ARC_PLC_PTCP_ELCT.CNTY_FSA_CD,'--')||'~~'||coalesce(YR_ARC_PLC_PTCP_ELCT.FARM_NBR,'--')))||'~~'||upper(coalesce(trim(both YR_ARC_PLC_PTCP_ELCT.PGM_ABR), '--')) ||'~~'||upper(coalesce(trim(both YR_ARC_PLC_PTCP_ELCT.FSA_CROP_CD), '--')) ||'~~'||upper(coalesce(trim(both YR_ARC_PLC_PTCP_ELCT.FSA_CROP_TYPE_CD), '--')) ) CROP_FARM_L_ID
FROM SQL_FARM_RCD_STG.YR_ARC_PLC_PTCP_ELCT
where YR_ARC_PLC_PTCP_ELCT.cdc_oper_cd IN ('I','UN','D')
) stg
LEFT JOIN  EDV.CROP_FARM_L dv
ON (
stg.CROP_FARM_L_ID = dv.CROP_FARM_L_ID
)
where (
dv.CROP_FARM_L_ID IS NULL)
) alias74 
where RANK=1)
;