INSERT into
EDV.TR_RCON_L(TR_RCON_L_ID ,
PRNT_TR_H_ID,
RSLT_TR_H_ID,
LOAD_DT,
DATA_SRC_NM)
(
select 
stg.TR_RCON_L_ID,
stg.PRNT_TR_H_ID,
stg.RSLT_TR_H_ID,
stg.LOAD_DT,
stg.DATA_SRC_NM
from (
SELECT DISTINCT
MD5(upper(coalesce(TR_RCON.PRNT_ST_FSA_CD ,'--')||'~~'||coalesce(TR_RCON.PRNT_CNTY_FSA_CD,'--')||'~~'||coalesce(TR_RCON.PRNT_TR_NBR, '-1'))) PRNT_TR_H_ID,
MD5(upper(coalesce(TR_RCON.RSLT_ST_FSA_CD ,'--')||'~~'||coalesce(TR_RCON.RSLT_CNTY_FSA_CD,'--')||'~~'||coalesce(TR_RCON.RSLT_TR_NBR, '-1'))) RSLT_TR_H_ID,
TR_RCON.LOAD_DT LOAD_DT,
TR_RCON.DATA_SRC_NM DATA_SRC_NM,
ROW_NUMBER() OVER (PARTITION BY MD5(upper(coalesce(TR_RCON.PRNT_ST_FSA_CD ,'--')||'~~'||coalesce(TR_RCON.PRNT_CNTY_FSA_CD,'--')||'~~'||coalesce(TR_RCON.PRNT_TR_NBR, '-1'))),
MD5(upper(coalesce(TR_RCON.RSLT_ST_FSA_CD ,'--')||'~~'||coalesce(TR_RCON.RSLT_CNTY_FSA_CD,'--')||'~~'||coalesce(TR_RCON.RSLT_TR_NBR, '-1'))) ORDER BY TR_RCON.CDC_DT desc, TR_RCON.LOAD_DT desc) STG_EFF_DT_RANK,
MD5(MD5(upper(coalesce(TR_RCON.PRNT_ST_FSA_CD ,'--')||'~~'||coalesce(TR_RCON.PRNT_CNTY_FSA_CD,'--')||'~~'||coalesce((TR_RCON.PRNT_TR_NBR)::numeric , -1)))||'~~'||MD5(upper(coalesce(TR_RCON.RSLT_ST_FSA_CD ,'--')||'~~'||coalesce(TR_RCON.RSLT_CNTY_FSA_CD,'--')||'~~'||coalesce(TR_RCON.RSLT_TR_NBR, '-1')))) TR_RCON_L_ID
FROM SQL_FARM_RCD_STG.TR_RCON 
where TR_RCON.cdc_oper_cd IN ('I','UN','D')
) stg 
LEFT JOIN  EDV.TR_RCON_L dv
 ON (
stg.TR_RCON_L_ID = dv.TR_RCON_L_ID
)
where (
dv.TR_RCON_L_ID IS NULL
)
and stg.STG_EFF_DT_RANK = 1 

UNION
 
select 
stg.TR_RCON_L_ID,
stg.PRNT_TR_H_ID,
stg.RSLT_TR_H_ID,
stg.LOAD_DT,
stg.DATA_SRC_NM
from (
SELECT DISTINCT
(CASE WHEN TH1.TR_H_ID IS NOT NULL THEN MD5(upper(coalesce(MIDAS_FARM_TR_CHG.PRNT_ST_FSA_CD ,'--')||'~~'||coalesce(MIDAS_FARM_TR_CHG.PRNT_CNTY_FSA_CD,'--')||'~~'||coalesce(MIDAS_FARM_TR_CHG.PRNT_TR_NBR, '-1'))) ELSE MD5('--'||'~~'||'--'||'~~'||'-1') END) PRNT_TR_H_ID,
(CASE WHEN TH2.TR_H_ID IS NOT NULL THEN MD5(upper(coalesce(MIDAS_FARM_TR_CHG.RSLT_ST_FSA_CD ,'--')||'~~'||coalesce(MIDAS_FARM_TR_CHG.RSLT_CNTY_FSA_CD,'--')||'~~'||coalesce(MIDAS_FARM_TR_CHG.RSLT_TR_NBR, '-1'))) ELSE MD5('--'||'~~'||'--'||'~~'||'-1') END) RSLT_TR_H_ID,
MIDAS_FARM_TR_CHG.LOAD_DT LOAD_DT,
MIDAS_FARM_TR_CHG.DATA_SRC_NM DATA_SRC_NM,
ROW_NUMBER() OVER (PARTITION BY MD5(upper(coalesce(MIDAS_FARM_TR_CHG.PRNT_ST_FSA_CD ,'--')||'~~'||coalesce(MIDAS_FARM_TR_CHG.PRNT_CNTY_FSA_CD,'--')||'~~'||coalesce(MIDAS_FARM_TR_CHG.PRNT_TR_NBR, '-1'))),
MD5(upper(coalesce(MIDAS_FARM_TR_CHG.RSLT_ST_FSA_CD ,'--')||'~~'||coalesce(MIDAS_FARM_TR_CHG.RSLT_CNTY_FSA_CD,'--')||'~~'||coalesce(MIDAS_FARM_TR_CHG.RSLT_TR_NBR, '-1'))) ORDER BY MIDAS_FARM_TR_CHG.CDC_DT desc, MIDAS_FARM_TR_CHG.LOAD_DT desc) STG_EFF_DT_RANK,
MD5(MD5(upper(coalesce(MIDAS_FARM_TR_CHG.PRNT_ST_FSA_CD ,'--')||'~~'||coalesce(MIDAS_FARM_TR_CHG.PRNT_CNTY_FSA_CD,'--')||'~~'||coalesce(MIDAS_FARM_TR_CHG.PRNT_TR_NBR, '-1')))||'~~'||MD5(upper(coalesce(MIDAS_FARM_TR_CHG.RSLT_ST_FSA_CD ,'--')||'~~'||coalesce(MIDAS_FARM_TR_CHG.RSLT_CNTY_FSA_CD,'--')||'~~'||coalesce(MIDAS_FARM_TR_CHG.RSLT_TR_NBR, '-1')))) TR_RCON_L_ID
FROM SQL_FARM_RCD_STG.MIDAS_FARM_TR_CHG 
LEFT JOIN EDV.TR_H TH1
ON (MD5(upper(coalesce(MIDAS_FARM_TR_CHG.PRNT_ST_FSA_CD ,'--')||'~~'||coalesce(MIDAS_FARM_TR_CHG.PRNT_CNTY_FSA_CD,'--')||'~~'||coalesce(MIDAS_FARM_TR_CHG.PRNT_TR_NBR, '-1'))) = TH1.TR_H_ID)
LEFT JOIN EDV.TR_H TH2
ON (MD5(upper(coalesce(MIDAS_FARM_TR_CHG.RSLT_ST_FSA_CD ,'--')||'~~'||coalesce(MIDAS_FARM_TR_CHG.RSLT_CNTY_FSA_CD,'--')||'~~'||coalesce(MIDAS_FARM_TR_CHG.RSLT_TR_NBR, '-1'))) = TH2.TR_H_ID)
where MIDAS_FARM_TR_CHG.cdc_oper_cd IN ('I','UN','D')
AND MIDAS_FARM_TR_CHG.FARM_CHG_TYPE_ID IN (1143)
) stg 
LEFT JOIN  EDV.TR_RCON_L dv
 ON (
stg.TR_RCON_L_ID = dv.TR_RCON_L_ID
)
where (
dv.TR_RCON_L_ID IS NULL
)
and stg.STG_EFF_DT_RANK = 1
)
;