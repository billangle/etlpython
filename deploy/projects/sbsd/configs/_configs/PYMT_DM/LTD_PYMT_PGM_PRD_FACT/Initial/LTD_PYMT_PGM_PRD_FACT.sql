INSERT INTO pymt_dm_stg.ltd_pymt_pgm_prd_fact (
--ltd_pymt_pgm_prd_fact_id, --not in select 
cre_dt,
last_chg_dt,
data_stat_cd,
ltd_pymt_pgm_srgt_id,
ltd_pymt_pgm_durb_id,
sbsd_prd_strt_yr,
sbsd_prd_end_yr,
pgm_msize_nm,
pymt_lmt_ind,
pymt_lmt_amt,
eff_dt,
pymt_lmt_prrt_pct,
cnsv_pgm_ind,
dir_pymt_pgm_ind,
agi_thld_cd,
pay_st_lcl_gvt_ind,
pay_fed_enty_ind,
alow_pos_adj_ind,
alow_neg_adj_ind,
alow_pgm_adj_ind,
app_cmb_pty_rules_ind,
cmb_pty_rules_src_cd,
alow_pmit_enty_adj_ind,
alow_inhrt_adj_ind,
app_mbr_ctrb_rqmt_ind,
app_sbst_chg_rqmt_ind,
app_cash_rent_tnt_rule_ind,
app_3_ownshp_lvl_lmt_ind,
pub_schl_pymt_lmt_rule_cd,
alow_fgn_prsn_ptcp_ind,
alow_pub_schl_ptcp_ind,
rvw_cplt_dt,
rvw_nm,
agi_900k_pymt_pgm_ind,
src_cre_dt,
src_last_chg_dt,
dart_etl_last_chg_dt
)
Select /*+ parallel(24) */
DV.CRE_DT,
DV.LAST_CHG_DT,
'A' DATA_STAT_CD,
DV.LTD_PYMT_PGM_SRGT_ID,
DV.LTD_PYMT_PGM_DURB_ID,
DV.SBSD_PRD_END_YR,
DV.SBSD_PRD_STRT_YR,
DV.PGM_MSIZE_NM,
DV.PYMT_LMT_IND,
DV.PYMT_LMT_AMT,
DV.EFF_DT,
DV.PYMT_LMT_PRRT_PCT,
DV.CNSV_PGM_IND,
DV.DIR_PYMT_PGM_IND, 
DV.AGI_THLD_CD,
DV.PAY_ST_LCL_GVT_IND,
DV.PAY_FED_ENTY_IND,
DV.ALOW_POS_ADJ_IND,
DV.ALOW_NEG_ADJ_IND,
DV.ALOW_PGM_ADJ_IND,
DV.APP_CMB_PTY_RULES_IND,
DV.CMB_PTY_RULES_SRC_CD, 
DV.ALOW_PMIT_ENTY_ADJ_IND,
DV.ALOW_INHRT_ADJ_IND,
DV.APP_MBR_CTRB_RQMT_IND,
DV.APP_SBST_CHG_RQMT_IND,
DV.APP_CASH_RENT_TNT_RULE_IND,
DV.APP_3_OWNSHP_LVL_LMT_IND,
DV.PUB_SCHL_PYMT_LMT_RULE_CD,
DV.ALOW_PUB_SCHL_PTCP_IND,
DV.ALOW_FGN_PRSN_PTCP_IND,
DV.RVW_CPLT_DT,
DV.RVW_NM,
DV.AGI_900K_PYMT_PGM_IND,
DV.SRC_CRE_DT,
DV.SRC_LAST_CHG_DT,
TO_TIMESTAMP('{ETL_START_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF') DART_ETL_LAST_CHG_DT
-- MD5(DV.SBSD_PRD_STRT_YR||'~~'||DV.LTD_PYMT_PGM_DURB_ID) BUS_KEY,
--  /*used by DS job to detect multiple BK records (for single AK field, no hashing required) */
--  ('SBSD_PRD_STRT_YR:'||DV.SBSD_PRD_STRT_YR||'~~'||'LTD_PYMT_PGM_DURB_ID:'||DV.LTD_PYMT_PGM_DURB_ID) BUS_KEY_ERR
 From (

SELECT DV_ALL.* FROM (
Select 
DV_DR.PYMT_PGM_PRD_L_ID,
DV_DR.SBSD_PRD_END_YR,
DV_DR.PGM_MSIZE_NM,
DV_DR.PYMT_LMT_AMT,
DV_DR.EFF_DT,
DV_DR.PYMT_LMT_PRRT_PCT,
DV_DR.AGI_THLD_CD,
DV_DR.PUB_SCHL_PYMT_LMT_RULE_CD,
DV_DR.RVW_CPLT_DT,
DV_DR.RVW_NM,
DV_DR.CMB_PTY_RULES_SRC_CD AS CMB_PTY_RULES_SRC_CD, 
Case UPPER(DV_DR.AGI_900K_PYMT_PGM_IND) when 'Y' Then 1  when 'N' Then 0 else cast(DV_DR.AGI_900K_PYMT_PGM_IND as numeric) END AS AGI_900K_PYMT_PGM_IND,
Case UPPER(DV_DR.ALOW_FGN_PRSN_PTCP_IND) when 'Y' Then 1  when 'N' Then 0 else cast(DV_DR.ALOW_FGN_PRSN_PTCP_IND as numeric) END AS  ALOW_FGN_PRSN_PTCP_IND,
Case UPPER(DV_DR.ALOW_INHRT_ADJ_IND) when 'Y' Then 1  when 'N' Then 0 else cast(DV_DR.ALOW_INHRT_ADJ_IND as numeric) END AS  ALOW_INHRT_ADJ_IND,
Case UPPER(DV_DR.ALOW_NEG_ADJ_IND) when 'Y' Then 1  when 'N' Then 0 else CAST(DV_DR.ALOW_NEG_ADJ_IND as numeric) END AS ALOW_NEG_ADJ_IND,
Case UPPER(DV_DR.ALOW_PGM_ADJ_IND) when 'Y' Then 1  when 'N' Then 0 else CAST(DV_DR.ALOW_PGM_ADJ_IND as numeric) END AS ALOW_PGM_ADJ_IND,
Case UPPER(DV_DR.ALOW_PMIT_ENTY_ADJ_IND) when 'Y' Then 1  when 'N' Then 0 else CAST(DV_DR.ALOW_PMIT_ENTY_ADJ_IND as numeric) END AS ALOW_PMIT_ENTY_ADJ_IND,
Case UPPER(DV_DR.ALOW_POS_ADJ_IND) when 'Y' Then 1  when 'N' Then 0 else CAST(DV_DR.ALOW_POS_ADJ_IND as numeric) END AS ALOW_POS_ADJ_IND,
Case UPPER(DV_DR.ALOW_PUB_SCHL_PTCP_IND) when 'Y' Then 1  when 'N' Then 0 else CAST(DV_DR.ALOW_PUB_SCHL_PTCP_IND as numeric) END AS  ALOW_PUB_SCHL_PTCP_IND,
Case UPPER(DV_DR.APP_3_OWNSHP_LVL_LMT_IND) when 'Y' Then 1  when 'N' Then 0 else CAST(DV_DR.APP_3_OWNSHP_LVL_LMT_IND as numeric) END AS  APP_3_OWNSHP_LVL_LMT_IND,
Case UPPER(DV_DR.APP_CASH_RENT_TNT_RULE_IND) when 'Y' Then 1  when 'N' Then 0 else CAST(DV_DR.APP_CASH_RENT_TNT_RULE_IND as numeric) END AS  APP_CASH_RENT_TNT_RULE_IND,
Case UPPER(DV_DR.APP_CMB_PTY_RULES_IND) when 'Y' Then 1  when 'N' Then 0 else CAST(DV_DR.APP_CMB_PTY_RULES_IND as numeric) END AS APP_CMB_PTY_RULES_IND,
Case UPPER(DV_DR.APP_MBR_CTRB_RQMT_IND) when 'Y' Then 1  when 'N' Then 0 else CAST(DV_DR.APP_MBR_CTRB_RQMT_IND as numeric) END AS  APP_MBR_CTRB_RQMT_IND,
Case UPPER(DV_DR.APP_SBST_CHG_RQMT_IND) when 'Y' Then 1  when 'N' Then 0 else CAST(DV_DR.APP_SBST_CHG_RQMT_IND as numeric) END AS  APP_SBST_CHG_RQMT_IND,
Case UPPER(DV_DR.CNSV_PGM_IND) when 'Y' Then 1  when 'N' Then 0 else CAST(DV_DR.CNSV_PGM_IND as numeric) END AS CNSV_PGM_IND,
Case UPPER(DV_DR.DIR_PYMT_PGM_IND) when 'Y' Then 1  when 'N' Then 0 else CAST(DV_DR.DIR_PYMT_PGM_IND as numeric) END AS DIR_PYMT_PGM_IND, 
Case UPPER(DV_DR.PYMT_LMT_IND) when 'Y' Then 1  when 'N' Then 0 else CAST(DV_DR.PYMT_LMT_IND as numeric) END AS PYMT_LMT_IND,
Case UPPER(DV_DR.PAY_FED_ENTY_IND) when 'Y' Then 1  when 'N' Then 0 else CAST(DV_DR.PAY_FED_ENTY_IND as numeric) END AS PAY_FED_ENTY_IND,
Case UPPER(DV_DR.PAY_ST_LCL_GVT_IND) when 'Y' Then 1  when 'N' Then 0 else CAST(DV_DR.PAY_ST_LCL_GVT_IND as numeric) END AS PAY_ST_LCL_GVT_IND,
DV_DR.SRC_CRE_DT,
DV_DR.SRC_LAST_CHG_DT,
COALESCE(LTD_PYMT_PGM_DIM.LTD_PYMT_PGM_SRGT_ID,-3) LTD_PYMT_PGM_SRGT_ID,
COALESCE(LTD_PYMT_PGM_DIM.LTD_PYMT_PGM_DURB_ID,-3) LTD_PYMT_PGM_DURB_ID,
PYMT_PGM_PRD_L.SBSD_PRD_STRT_YR,
TO_TIMESTAMP ('{ETL_START_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF') CRE_DT ,
TO_TIMESTAMP ('{ETL_START_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF') LAST_CHG_DT,
  RANK() over ( partition by   (LTD_PYMT_PGM_DIM.LTD_PYMT_PGM_DURB_ID,-3),   PYMT_PGM_PRD_L.SBSD_PRD_STRT_YR    order by  PYMT_PGM_PRD_L.SBSD_PRD_STRT_YR asc,
               DV_DR.SRC_CRE_DT DESC NULLS LAST
       )  as Rank_Part      
  
 From  (  
 select * from EDV.PYMT_PGM_PRD_PARM_LS 
where  (DATA_EFF_END_DT) = TO_TIMESTAMP ('9999-12-31', 'YYYY-MM-DD')
AND (DATA_EFF_STRT_DT) <=  TO_TIMESTAMP('{ETL_START_DATE}','YYYY-MM-DD')
--And  PYMT_PGM_PRD_L_ID ='e65fe5d7da53f41f78cb91edf69a0216'
) DV_DR
Join EDV.PYMT_PGM_PRD_L
on (DV_DR.PYMT_PGM_PRD_L_ID = PYMT_PGM_PRD_L.PYMT_PGM_PRD_L_ID)
Left Join pymt_dm_stg.LTD_PYMT_PGM_DIM
   ON ( Trim(PYMT_PGM_PRD_L.PYMT_PGM_NM) = Trim(LTD_PYMT_PGM_DIM.LTD_PYMT_PGM_NM) And  (LTD_PYMT_PGM_DIM.DATA_EFF_END_DT) = TO_TIMESTAMP('9999-12-31','YYYY-MM-DD') )

   ) DV_ALL
 WHERE DV_ALL.Rank_Part = 1 
 ) dv 
 
/* LEFT JOIN  pymt_dm_stg.LTD_PYMT_PGM_PRD_FACT dm
 ON (  (dv.LTD_PYMT_PGM_DURB_ID,0) = (dm.LTD_PYMT_PGM_DURB_ID,0)
       And  (dv.SBSD_PRD_STRT_YR,0) = (dm.SBSD_PRD_STRT_YR,0)
       And dm.DATA_STAT_CD = 'A' 
 ) 
 where
  (
   dm.LTD_PYMT_PGM_DURB_ID IS NULL
   And dm.SBSD_PRD_STRT_YR IS NULL 
    
 )  */
 ORDER BY LAST_CHG_DT asc