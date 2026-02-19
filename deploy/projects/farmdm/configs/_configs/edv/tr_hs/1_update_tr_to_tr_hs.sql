Merge into
EDV.TR_HS dv
using(select DISTINCT MD5(upper(coalesce(trim(both TR.ST_FSA_CD), '--')) ||'~~'||upper(coalesce(trim(both TR.CNTY_FSA_CD), '--')) ||'~~'||coalesce((trim(both TR.TR_NBR))::numeric , -1) ) as TR_H_ID,coalesce(TR.TR_ID, '-1') TR_ID,TR.LOAD_DT LOAD_DT,TR.CDC_DT DATA_EFF_STRT_DT,TR.DATA_SRC_NM DATA_SRC_NM,TR.DATA_STAT_CD DATA_STAT_CD,TR.CRE_DT SRC_CRE_DT,TR.LAST_CHG_DT SRC_LAST_CHG_DT,TR.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM,TR.CNTY_OFC_CTL_ID CNTY_OFC_CTL_ID,TR.TR_DESC TR_DESC,TR.BIA_RNG_UNIT_NBR BIA_RNG_UNIT_NBR,TR.LOC_ST_FSA_CD LOC_ST_FSA_CD,TR.LOC_CNTY_FSA_CD LOC_CNTY_FSA_CD,TR.CONG_DIST_CD CONG_DIST_CD,TR.WL_CERT_CPLT_CD SRC_WL_CERT_CPLT_CD,WL_CERT_CPLT_RS.DMN_VAL_NM WL_CERT_CPLT_NM,TR.WL_CERT_CPLT_YR WL_CERT_CPLT_YR,TR.TR_NBR SRC_TR_NBR ,
TR.CDC_OPER_CD,
MD5(trim(both TR.ST_FSA_CD)||'~~'||trim(both TR.CNTY_FSA_CD)||'~~'||(trim(both TR.TR_NBR))::numeric ||'~~'||TR.TR_ID||'~~'||trim(both TR.DATA_STAT_CD)||'~~'||to_char(TR.CRE_DT,'YYYY-MM-DD HH24:MI:SS.US')||'~~'||to_char(TR.LAST_CHG_DT,'YYYY-MM-DD HH24:MI:SS.US')||'~~'||trim(both TR.LAST_CHG_USER_NM)||'~~'||TR.CNTY_OFC_CTL_ID||'~~'||trim(both TR.TR_DESC)||'~~'||trim(both TR.BIA_RNG_UNIT_NBR)||'~~'||trim(both TR.LOC_ST_FSA_CD)||'~~'||trim(both TR.LOC_CNTY_FSA_CD)||'~~'||trim(both TR.CONG_DIST_CD)||'~~'||TR.WL_CERT_CPLT_CD||'~~'||trim(both WL_CERT_CPLT_RS.DMN_VAL_NM)||'~~'||TR.WL_CERT_CPLT_YR||'~~'||(trim(both TR.TR_NBR))::numeric ) HASH_DIF
FROM SQL_FARM_RCD_STG.TR 
LEFT JOIN(select * from EDV.WL_CERT_CPLT_RS
where load_end_dt = TO_TIMESTAMP('9999-12-31','YYYY-MM-DD')) WL_CERT_CPLT_RS 
ON ( TR.WL_CERT_CPLT_CD = WL_CERT_CPLT_RS.DMN_VAL_ID )
 where TR.CDC_OPER_CD IN ('I','UN')
order by TR.CDC_DT)
stg
 on ( 
coalesce(stg.TR_ID,0) = coalesce(dv.TR_ID,0)
 ) 
WHEN MATCHED and dv.LOAD_DT <> stg.LOAD_DT and dv.LOAD_END_DT = TO_TIMESTAMP('9999-12-31','YYYY-MM-DD') and stg.HASH_DIF <> dv.HASH_DIF THEN UPDATE 
set LOAD_END_DT = stg.LOAD_DT,
DATA_EFF_END_DT = stg.DATA_EFF_STRT_DT
;