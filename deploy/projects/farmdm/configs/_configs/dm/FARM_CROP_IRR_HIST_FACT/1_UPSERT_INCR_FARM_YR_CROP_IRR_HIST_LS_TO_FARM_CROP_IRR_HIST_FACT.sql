with insert_data as (
SELECT PGM_YR
    ,FARM_SRGT_ID
    ,FARM_DURB_ID
    ,ADM_FSA_ST_CNTY_SRGT_ID
    ,ADM_FSA_ST_CNTY_DURB_ID
    ,FSA_CROP_SRGT_ID
    ,FSA_CROP_DURB_ID
    ,PGM_ABR
    ,IRR_HIST_CRE_DT
    ,IRR_HIST_CRE_USER_NM
    ,IRR_HIST_LAST_CHG_DT
    ,IRR_HIST_LAST_CHG_USER_NM
    ,SRC_DATA_STAT_CD
    ,HIST_IRR_PCT    
    ,DATA_EFF_STRT_DT
FROM            
(
SELECT DISTINCT dv_dr.PGM_YR AS PGM_YR
    ,COALESCE(fr_dim.FARM_SRGT_ID, -3) AS FARM_SRGT_ID
    ,fr_dim.FARM_DURB_ID AS FARM_DURB_ID
    ,COALESCE(fs_dim.FSA_ST_CNTY_SRGT_ID, -3) ADM_FSA_ST_CNTY_SRGT_ID
    ,COALESCE(fs_dim.FSA_ST_CNTY_DURB_ID, -3) AS ADM_FSA_ST_CNTY_DURB_ID
    ,COALESCE(fsa_dim.FSA_CROP_SRGT_ID, -3) AS FSA_CROP_SRGT_ID
    ,fsa_dim.FSA_CROP_DURB_ID AS FSA_CROP_DURB_ID
    ,fcihl.PGM_ABR AS PGM_ABR
    ,dv_dr.SRC_CRE_DT AS IRR_HIST_CRE_DT
    ,dv_dr.SRC_CRE_USER_NM AS IRR_HIST_CRE_USER_NM
    ,dv_dr.SRC_LAST_CHG_DT AS IRR_HIST_LAST_CHG_DT
    ,dv_dr.SRC_LAST_CHG_USER_NM AS IRR_HIST_LAST_CHG_USER_NM
    ,dv_dr.DATA_STAT_CD AS SRC_DATA_STAT_CD
    ,dv_dr.HIST_IRR_PCT AS HIST_IRR_PCT    
    ,GREATEST(COALESCE(dv_dr.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')),COALESCE(lams.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD'))) AS DATA_EFF_STRT_DT
    ,ROW_NUMBER() OVER (PARTITION BY dv_dr.PGM_YR,fr_dim.FARM_DURB_ID,fsa_dim.FSA_CROP_DURB_ID ORDER BY dv_dr.DATA_EFF_STRT_DT DESC,lams.DATA_EFF_STRT_DT DESC) AS  Row_Num_Part
 FROM EDV.FARM_YR_CROP_IRR_HIST_LS dv_dr
 LEFT JOIN EDV.FARM_CROP_IRR_HIST_L fcihl ON (COALESCE(dv_dr.FARM_CROP_IRR_HIST_L_ID,'6bb61e3b7bce0931da574d19d1d82c88') =  fcihl.FARM_CROP_IRR_HIST_L_ID)
 LEFT JOIN EDV.LOC_AREA_MRT_SRC_RS lams ON (
        fcihl.ST_FSA_CD = COALESCE(lams.CTRY_DIV_MRT_CD,'--')
        AND fcihl.CNTY_FSA_CD = COALESCE(lams.LOC_AREA_MRT_CD,'--')
        AND lams.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA' )
 LEFT JOIN EDV.LOC_AREA_RH lah ON (
        COALESCE(lams.LOC_AREA_CAT_NM, '[NULL IN SOURCE]') = lah.LOC_AREA_CAT_NM
        AND COALESCE(lams.LOC_AREA_NM, '[NULL IN SOURCE]') = lah.LOC_AREA_NM
        AND COALESCE(lams.CTRY_DIV_NM, '[NULL IN SOURCE]') = lah.CTRY_DIV_NM
        AND lams.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA' )
 LEFT JOIN CMN_DIM_DM_STG.FSA_ST_CNTY_DIM fs_dim ON (
        lah.DURB_ID = COALESCE(fs_dim.FSA_ST_CNTY_DURB_ID,-1)
        AND fs_dim.CUR_RCD_IND = 1   )
 LEFT JOIN EDV.FARM_H fh ON (COALESCE(fcihl.FARM_H_ID,'baf6dd71fe45fe2f5c1c0e6724d514fd') = fh.FARM_H_ID)
 LEFT JOIN CMN_DIM_DM_STG.FARM_DIM fr_dim ON (
        fh.DURB_ID = COALESCE(fr_dim.FARM_DURB_ID,-1)
        AND fr_dim.CUR_RCD_IND = 1 )
 LEFT JOIN EBV.FSA_CROP_TYPE_RH fctrh ON (
        COALESCE(fcihl.FSA_CROP_CD,'--') = fctrh.FSA_CROP_CD
        AND COALESCE(fcihl.FSA_CROP_TYPE_CD,'--') = fctrh.FSA_CROP_TYPE_CD
        AND dv_dr.PGM_YR = fctrh.PGM_YR
        )
 LEFT JOIN CMN_DIM_DM_STG.FSA_CROP_TYPE_DIM fsa_dim ON (
        fctrh.DURB_ID = COALESCE(fsa_dim.FSA_CROP_DURB_ID,-1)
        AND fsa_dim.CUR_RCD_IND = 1
        )
 WHERE 
 (
 (
 CAST(dv_dr.DATA_EFF_STRT_DT AS DATE) =  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 AND CAST(dv_dr.DATA_EFF_END_DT AS DATE) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 )
 OR
 (
 CAST(lams.DATA_EFF_STRT_DT AS DATE) =  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 OR CAST(lams.DATA_EFF_END_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 )
 )
 AND CAST(dv_dr.DATA_EFF_STRT_DT AS DATE) <=  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 AND CAST(lams.DATA_EFF_STRT_DT AS DATE)  <=  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 AND dv_dr.PGM_YR IS NOT NULL
 AND fr_dim.FARM_DURB_ID IS NOT NULL
 AND fsa_dim.FSA_CROP_DURB_ID IS NOT NULL
 )dm
 WHERE dm.Row_Num_Part = 1
) 
select  (current_date) AS cre_dt,
	    (current_date) AS last_chg_dt, 
		'A' AS data_stat_cd,
		pgm_yr, 
		farm_srgt_id, 
		farm_durb_id, 
		fsa_crop_srgt_id, 
		fsa_crop_durb_id, 
		pgm_abr, 
		irr_hist_cre_dt, 
		irr_hist_cre_user_nm, 
		irr_hist_last_chg_dt, 
		irr_hist_last_chg_user_nm, 
		src_data_stat_cd, 
		hist_irr_pct
from insert_data
where farm_durb_id > 0
order by farm_durb_id asc