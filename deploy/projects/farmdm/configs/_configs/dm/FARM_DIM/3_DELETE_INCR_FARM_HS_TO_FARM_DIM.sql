with del_data as (
SELECT DISTINCT
FH.DURB_ID FARM_DURB_ID,
FHS.DATA_EFF_END_DT DATA_EFF_END_DT,
0 AS CUR_RCD_IND,
FHS.data_eff_strt_dt,
(current_date) as cre_dt,
FH.st_fsa_cd,
FH.cnty_fsa_cd,
FH.farm_nbr,
LOS.CTRY_DIV_NM AS ST_FSA_NM,
LOS.LOC_AREA_NM CNTY_FSA_NM,
FHS.SRC_CRE_DT AS FARM_CRE_DT,
FHS.SRC_LAST_CHG_DT AS FARM_LAST_CHG_DT,
FHS.SRC_LAST_CHG_USER_NM AS FARM_LAST_CHG_USER_NM,
FHS.DATA_STAT_CD AS SRC_DATA_STAT_CD,
FHS.FARM_CMN_NM AS FARM_CMN_NM,
FHS.RCON_PEND_APVL_CD AS RCON_PEND_APVL_CD
FROM EDV.FARM_HS FHS
inner JOIN EDV.FARM_H FH ON ( FH.FARM_H_ID = COALESCE(FHS.FARM_H_ID,'baf6dd71fe45fe2f5c1c0e6724d514fd'))
LEFT JOIN EDV.LOC_AREA_MRT_SRC_RS  LOS ON (FH.ST_FSA_CD = LOS.CTRY_DIV_MRT_CD  
        AND FH.CNTY_FSA_CD = LOS.LOC_AREA_MRT_CD  
        AND LOS.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'
 and TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') >= CAST(LOS.data_eff_strt_dt AS DATE)
                    and TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') < CAST(LOS.data_eff_end_dt AS DATE) )
where (CAST(FHS.DATA_EFF_END_DT AS DATE) =  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')  
      AND NOT EXISTS ( SELECT '1' FROM EDV.FARM_HS FHS_IN
                       inner join EDV.FARM_HS FHS 
                    ON FHS.FARM_H_ID = FHS_IN.FARM_H_ID   
                    AND CAST(FHS_IN.DATA_EFF_END_DT AS DATE)=  TO_TIMESTAMP('9999-12-31','YYYY-MM-DD')  
                 )) 
 AND FH.DURB_ID IS NOT NULL
 
) 
SELECT 
farm_durb_id, 
cur_rcd_ind, 
data_eff_strt_dt, 
data_eff_end_dt, 
cre_dt, 
st_fsa_cd, 
cnty_fsa_cd, 
farm_nbr, 
st_fsa_nm, 
cnty_fsa_nm, 
farm_cre_dt, 
farm_last_chg_dt, 
farm_last_chg_user_nm, 
src_data_stat_cd, 
farm_cmn_nm,
rcon_pend_apvl_cd
FROM del_data ud;
