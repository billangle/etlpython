WITH TAB_INCR_DR_ID
AS ( 
    
    SELECT  DISTINCT FARM_TR_L_ID INCR_DR_ID
        ,CLU_YR_ID
        ,PGM_YR
    FROM EDV.CLU_YR_LS
    WHERE CAST(DATA_EFF_END_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
    
    UNION 
	
    SELECT DISTINCT FARM_TR_L.FARM_TR_L_ID INCR_DR_ID
        ,CLU_YR_ID
        ,PGM_YR
    FROM EDV.LOC_AREA_MRT_SRC_RS
        ,EDV.FARM_TR_L
        ,EDV.CLU_YR_LS
        ,EDV.TR_H
    WHERE (
            CAST(LOC_AREA_MRT_SRC_RS.DATA_EFF_END_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
            )
        AND COALESCE(FARM_TR_L.TR_H_ID, '6de912369edfb89b50859d8f305e4f72') = TR_H.TR_H_ID
        AND LOC_AREA_MRT_SRC_RS.CTRY_DIV_MRT_CD = TR_H.ST_FSA_CD
        AND LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD = TR_H.CNTY_FSA_CD
        AND TRIM(LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD_SRC_ACRO) = 'FSA'
        AND CLU_YR_LS.FARM_TR_L_ID = FARM_TR_L.FARM_TR_L_ID
    
    UNION 
	
    SELECT DISTINCT CLU_YR_LS.FARM_TR_L_ID INCR_DR_ID
        ,CLU_YR_ID
        ,PGM_YR
    FROM EDV.LOC_AREA_MRT_SRC_RS
        ,EDV.CLU_YR_LS
    WHERE (
            CAST(LOC_AREA_MRT_SRC_RS.DATA_EFF_END_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
            )
        AND LOC_AREA_MRT_SRC_RS.CTRY_DIV_MRT_CD = COALESCE(CLU_YR_LS.LOC_ST_FSA_CD, '--')
        AND LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD = COALESCE(CLU_YR_LS.LOC_CNTY_FSA_CD, '--')
        AND TRIM(LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD_SRC_ACRO) = 'FSA'
    
    UNION 
	
    SELECT DISTINCT CLU_YR_LS.FARM_TR_L_ID INCR_DR_ID
        ,CLU_YR_ID
        ,PGM_YR
    FROM EDV.CNSV_PRAC_RS
        ,EDV.CLU_YR_LS
    WHERE (
            CAST(CNSV_PRAC_RS.DATA_EFF_END_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
            )
        AND COALESCE(CLU_YR_LS.SRC_CNSV_PRAC_ID, - 1) = COALESCE(CNSV_PRAC_RS.DMN_VAL_ID, - 1)
    
    UNION 
	
    SELECT DISTINCT CLU_YR_LS.FARM_TR_L_ID INCR_DR_ID
        ,CLU_YR_ID
        ,PGM_YR
    FROM EDV.FLD_HEL_STAT_RS
        ,EDV.CLU_YR_LS
    WHERE (
            CAST(FLD_HEL_STAT_RS.DATA_EFF_END_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
            )
        AND COALESCE(FLD_HEL_STAT_RS.DMN_VAL_ID, - 1) = COALESCE(CLU_YR_LS.SRC_HEL_STAT_ID, - 1)
    
    UNION 
	
    SELECT DISTINCT CLU_YR_LS.FARM_TR_L_ID INCR_DR_ID
        ,CLU_YR_ID
        ,PGM_YR
    FROM EDV.LAND_CLS_RS
        ,EDV.CLU_YR_LS
    WHERE (
            CAST(LAND_CLS_RS.DATA_EFF_END_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
            )
        AND COALESCE(LAND_CLS_RS.DMN_VAL_ID, - 1) = COALESCE(CLU_YR_LS.SRC_LAND_CLS_ID, - 1)
    
    UNION 
	
    SELECT DISTINCT FARM_TR_L.FARM_TR_L_ID INCR_DR_ID
        ,CLU_YR_ID
        ,PGM_YR
    FROM EDV.LOC_AREA_MRT_SRC_RS
        ,EDV.TR_HS
        ,EDV.FARM_TR_L
        ,EDV.CLU_YR_LS
        ,EDV.TR_H
    WHERE (
            CAST(LOC_AREA_MRT_SRC_RS.DATA_EFF_END_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
            )
        AND LOC_AREA_MRT_SRC_RS.CTRY_DIV_MRT_CD = COALESCE(TR_HS.LOC_ST_FSA_CD, '--')
        AND LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD = COALESCE(TR_HS.LOC_CNTY_FSA_CD, '--')
        AND LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'
        AND COALESCE(FARM_TR_L.TR_H_ID, '6de912369edfb89b50859d8f305e4f72') = TR_H.TR_H_ID
        AND COALESCE(TR_HS.TR_H_ID, '6de912369edfb89b50859d8f305e4f72') = TR_H.TR_H_ID
        AND CLU_YR_LS.FARM_TR_L_ID = FARM_TR_L.FARM_TR_L_ID
), 
del_data as (
SELECT 
	(current_date) AS CRE_DT
	,(current_date) AS LAST_CHG_DT
	,'I' AS DATA_STAT_CD
    ,DM.PGM_YR
    ,DM.ADM_FSA_ST_CNTY_SRGT_ID
    ,DM.ADM_FSA_ST_CNTY_DURB_ID
    ,DM.FARM_SRGT_ID
    ,DM.FARM_DURB_ID
    ,DM.TR_SRGT_ID
    ,DM.TR_DURB_ID
    ,DM.TR_LOC_FSA_ST_CNTY_SRGT_ID
    ,DM.TR_LOC_FSA_ST_CNTY_DURB_ID
    ,DM.CONG_DIST_SRGT_ID
    ,DM.CONG_DIST_DURB_ID
    ,DM.HEL_STAT_SRGT_ID
    ,DM.HEL_STAT_DURB_ID
    ,DM.CNSV_PRAC_SRGT_ID
    ,DM.CNSV_PRAC_DURB_ID
    ,DM.LAND_CLS_SRGT_ID
    ,DM.LAND_CLS_DURB_ID
    ,DM.CLU_LOC_FSA_ST_CNTY_SRGT_ID
    ,DM.CLU_LOC_FSA_ST_CNTY_DURB_ID
    ,DM.ANSI_ST_CNTY_SRGT_ID
    ,DM.ANSI_ST_CNTY_DURB_ID
    ,COALESCE(DM.FLD_NBR, 'X') FLD_NBR
    ,DM.CLU_YR_CRE_DT
    ,DM.CLU_YR_LAST_CHG_DT
    ,DM.CLU_YR_LAST_CHG_USER_NM
    ,DM.SRC_DATA_STAT_CD
    ,DM.CLU_ALT_ID
    ,DM.CPLD_IND_3CM
    ,DM.CLU_DESC
    ,DM.CRP_CTR_NBR
    ,DM.CRP_CTR_EXPR_DT
    ,DM.NTV_SOD_CVSN_DT
    ,DM.SOD_CVSN_CROP_YR_1
    ,DM.SOD_CVSN_CROP_YR_2
    ,DM.SOD_CVSN_CROP_YR_3
    ,DM.SOD_CVSN_CROP_YR_4
    ,DM.CLU_ACRG
    ,DM.DATA_EFF_STRT_DT
FROM (
    SELECT /* SELECT REQUIRED FIELDS FROM DM_1 */ DM_1.PGM_YR
        ,DM_1.ADM_FSA_ST_CNTY_SRGT_ID
        ,DM_1.ADM_FSA_ST_CNTY_DURB_ID
        ,DM_1.FARM_SRGT_ID
        ,DM_1.FARM_DURB_ID
        ,DM_1.TR_SRGT_ID
        ,DM_1.TR_DURB_ID
        ,DM_1.TR_LOC_FSA_ST_CNTY_SRGT_ID
        ,DM_1.TR_LOC_FSA_ST_CNTY_DURB_ID
        ,DM_1.CONG_DIST_SRGT_ID
        ,DM_1.CONG_DIST_DURB_ID
        ,DM_1.HEL_STAT_SRGT_ID
        ,DM_1.HEL_STAT_DURB_ID
        ,DM_1.CNSV_PRAC_SRGT_ID
        ,DM_1.CNSV_PRAC_DURB_ID
        ,DM_1.LAND_CLS_SRGT_ID
        ,DM_1.LAND_CLS_DURB_ID
        ,DM_1.CLU_LOC_FSA_ST_CNTY_SRGT_ID
        ,DM_1.CLU_LOC_FSA_ST_CNTY_DURB_ID
        ,DM_1.ANSI_ST_CNTY_SRGT_ID
        ,DM_1.ANSI_ST_CNTY_DURB_ID
        ,DM_1.FLD_NBR
        ,DM_1.CLU_YR_CRE_DT
        ,DM_1.CLU_YR_LAST_CHG_DT
        ,DM_1.CLU_YR_LAST_CHG_USER_NM
        ,DM_1.SRC_DATA_STAT_CD
        ,DM_1.CLU_ALT_ID
        ,DM_1.CPLD_IND_3CM
        ,DM_1.CLU_DESC
        ,DM_1.CRP_CTR_NBR
        ,DM_1.CRP_CTR_EXPR_DT
        ,DM_1.NTV_SOD_CVSN_DT
        ,DM_1.SOD_CVSN_CROP_YR_1
        ,DM_1.SOD_CVSN_CROP_YR_2
        ,DM_1.SOD_CVSN_CROP_YR_3
        ,DM_1.SOD_CVSN_CROP_YR_4
        ,DM_1.CLU_ACRG
        ,DM_1.DATA_EFF_STRT_DT 
        ,DM_1.DV_DR_DATA_EFF_STRT_DT
        ,DM_1.LAMS_DATA_EFF_STRT_DT
        ,DM_1.THS_DATA_EFF_STRT_DT
        ,DM_1.LAMSR_DATA_EFF_STRT_DT
        ,DM_1.CPRS_DATA_EFF_STRT_DT
        ,DM_1.FHSRS_DATA_EFF_STRT_DT
        ,DM_1.LCRS_DATA_EFF_STRT_DT
        ,DM_1.LAMSRS_DATA_EFF_STRT_DT 
        ,ROW_NUMBER() OVER (
            PARTITION BY PGM_YR
            ,FARM_DURB_ID
            ,TR_DURB_ID
            ,FLD_NBR ORDER BY DV_DR_DATA_EFF_STRT_DT DESC
                ,SRC_DATA_STAT_CD ASC 
                ,LAMS_DATA_EFF_STRT_DT DESC
                ,THS_DATA_EFF_STRT_DT DESC
                ,LAMSR_DATA_EFF_STRT_DT DESC
                ,CPRS_DATA_EFF_STRT_DT DESC
                ,FHSRS_DATA_EFF_STRT_DT DESC
                ,LCRS_DATA_EFF_STRT_DT DESC
                ,LAMSRS_DATA_EFF_STRT_DT DESC
            ) AS ROW_NUM_PART
    FROM (
        SELECT PGM_YR
            ,ADM_FSA_ST_CNTY_SRGT_ID
            ,ADM_FSA_ST_CNTY_DURB_ID
            ,FARM_SRGT_ID
            ,FARM_DURB_ID
            ,TR_SRGT_ID
            ,TR_DURB_ID
            ,TR_LOC_FSA_ST_CNTY_SRGT_ID
            ,TR_LOC_FSA_ST_CNTY_DURB_ID
            ,CONG_DIST_SRGT_ID
            ,CONG_DIST_DURB_ID
            ,HEL_STAT_SRGT_ID
            ,HEL_STAT_DURB_ID
            ,CNSV_PRAC_SRGT_ID
            ,CNSV_PRAC_DURB_ID
            ,LAND_CLS_SRGT_ID
            ,LAND_CLS_DURB_ID
            ,CLU_LOC_FSA_ST_CNTY_SRGT_ID
            ,CLU_LOC_FSA_ST_CNTY_DURB_ID
            ,ANSI_ST_CNTY_SRGT_ID
            ,ANSI_ST_CNTY_DURB_ID
            ,FLD_NBR
            ,CLU_YR_CRE_DT
            ,CLU_YR_LAST_CHG_DT
            ,CLU_YR_LAST_CHG_USER_NM
            ,SRC_DATA_STAT_CD
            ,CLU_ALT_ID
            ,CPLD_IND_3CM
            ,CLU_DESC
            ,CRP_CTR_NBR
            ,CRP_CTR_EXPR_DT
            ,NTV_SOD_CVSN_DT
            ,SOD_CVSN_CROP_YR_1
            ,SOD_CVSN_CROP_YR_2
            ,SOD_CVSN_CROP_YR_3
            ,SOD_CVSN_CROP_YR_4
            ,CLU_ACRG
            ,DATA_EFF_STRT_DT 
            ,DV_DR_DATA_EFF_STRT_DT
            ,LAMS_DATA_EFF_STRT_DT
            ,THS_DATA_EFF_STRT_DT
            ,LAMSR_DATA_EFF_STRT_DT
            ,CPRS_DATA_EFF_STRT_DT
            ,FHSRS_DATA_EFF_STRT_DT
            ,LCRS_DATA_EFF_STRT_DT
            ,LAMSRS_DATA_EFF_STRT_DT
        FROM (
            SELECT *
            FROM (
                SELECT DISTINCT ROW_NUMBER() OVER (
                        PARTITION BY DV_DR.FARM_TR_L_ID
                        ,DV_DR.PGM_YR
                        ,DV_DR.CLU_YR_ID ORDER BY THS.DATA_EFF_STRT_DT DESC
                            ,DV_DR.SRC_LAST_CHG_DT DESC
                            ,DV_DR.CLU_YR_ID DESC
                            ,LAMS.DATA_EFF_STRT_DT DESC
                            ,LAMSR.DATA_EFF_STRT_DT DESC
                            ,CPRS.DATA_EFF_STRT_DT DESC
                            ,FHSRS.DATA_EFF_STRT_DT DESC
                            ,LCRS.DATA_EFF_STRT_DT DESC
                            ,LAMSRS.DATA_EFF_STRT_DT DESC
                        ) AS SRC_RANK
                    ,DV_DR.PGM_YR AS PGM_YR
                    ,COALESCE(FS_DIM.FSA_ST_CNTY_SRGT_ID, - 3) AS ADM_FSA_ST_CNTY_SRGT_ID
                    ,COALESCE(FS_DIM.FSA_ST_CNTY_DURB_ID, - 3) AS ADM_FSA_ST_CNTY_DURB_ID
                    ,COALESCE(FR_DIM.FARM_SRGT_ID, - 3) AS FARM_SRGT_ID
                    ,FR_DIM.FARM_DURB_ID AS FARM_DURB_ID
                    ,COALESCE(TR_DIM.TR_SRGT_ID, - 3) AS TR_SRGT_ID
                    ,TR_DIM.TR_DURB_ID AS TR_DURB_ID
                    ,COALESCE(FSCD_DIM.FSA_ST_CNTY_SRGT_ID, - 3) AS TR_LOC_FSA_ST_CNTY_SRGT_ID
                    ,COALESCE(FSCD_DIM.FSA_ST_CNTY_DURB_ID, - 3) AS TR_LOC_FSA_ST_CNTY_DURB_ID
                    ,COALESCE(CONG_DIM.CONG_DIST_SRGT_ID, - 3) AS CONG_DIST_SRGT_ID
                    ,COALESCE(CONG_DIM.CONG_DIST_DURB_ID, - 3) AS CONG_DIST_DURB_ID
                    ,COALESCE(HEL_DIM.HEL_STAT_SRGT_ID, - 3) AS HEL_STAT_SRGT_ID
                    ,COALESCE(HEL_DIM.HEL_STAT_DURB_ID, - 3) AS HEL_STAT_DURB_ID
                    ,COALESCE(CP_DIM.CNSV_PRAC_SRGT_ID, - 3) AS CNSV_PRAC_SRGT_ID
                    ,COALESCE(CP_DIM.CNSV_PRAC_DURB_ID, - 3) AS CNSV_PRAC_DURB_ID
                    ,COALESCE(LC_DIM.LAND_CLS_SRGT_ID, - 3) AS LAND_CLS_SRGT_ID
                    ,COALESCE(LC_DIM.LAND_CLS_DURB_ID, - 3) AS LAND_CLS_DURB_ID
                    ,COALESCE(FSC_DIM.FSA_ST_CNTY_SRGT_ID, - 3) AS CLU_LOC_FSA_ST_CNTY_SRGT_ID
                    ,COALESCE(FSC_DIM.FSA_ST_CNTY_DURB_ID, - 3) AS CLU_LOC_FSA_ST_CNTY_DURB_ID
                    ,COALESCE(ASC_DIM.ANSI_ST_CNTY_SRGT_ID, - 3) AS ANSI_ST_CNTY_SRGT_ID
                    ,COALESCE(ASC_DIM.ANSI_ST_CNTY_DURB_ID, - 3) AS ANSI_ST_CNTY_DURB_ID
                    ,DV_DR.FLD_NBR AS FLD_NBR
                    ,DV_DR.SRC_CRE_DT AS CLU_YR_CRE_DT
                    ,DV_DR.SRC_LAST_CHG_DT AS CLU_YR_LAST_CHG_DT
                    ,DV_DR.SRC_LAST_CHG_USER_NM AS CLU_YR_LAST_CHG_USER_NM
                    ,DV_DR.DATA_STAT_CD AS SRC_DATA_STAT_CD
                    ,DV_DR.CLU_ALT_ID AS CLU_ALT_ID
                    ,DV_DR.CPLD_IND_3CM AS CPLD_IND_3CM
                    ,DV_DR.CLU_DESC AS CLU_DESC
                    ,DV_DR.CRP_CTR_NBR AS CRP_CTR_NBR
                    ,DV_DR.CRP_CTR_EXPR_DT AS CRP_CTR_EXPR_DT
                    ,DV_DR.NTV_SOD_CVSN_DT AS NTV_SOD_CVSN_DT
                    ,DV_DR.SOD_CVSN_CROP_YR_1 AS SOD_CVSN_CROP_YR_1
                    ,DV_DR.SOD_CVSN_CROP_YR_2 AS SOD_CVSN_CROP_YR_2
                    ,DV_DR.SOD_CVSN_CROP_YR_3 AS SOD_CVSN_CROP_YR_3
                    ,DV_DR.SOD_CVSN_CROP_YR_4 AS SOD_CVSN_CROP_YR_4
                    ,DV_DR.CLU_ACRG AS CLU_ACRG
                    ,GREATEST(COALESCE(DV_DR.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')), COALESCE(LAMS.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')), COALESCE(THS.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')), COALESCE(LAMSR.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')), COALESCE(CPRS.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')), COALESCE(FHSRS.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')), COALESCE(LCRS.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')), COALESCE(LAMSRS.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD'))) DATA_EFF_STRT_DT
                    ,DV_DR.DATA_EFF_STRT_DT DV_DR_DATA_EFF_STRT_DT
                    ,LAMS.DATA_EFF_STRT_DT LAMS_DATA_EFF_STRT_DT
                    ,THS.DATA_EFF_STRT_DT THS_DATA_EFF_STRT_DT
                    ,LAMSR.DATA_EFF_STRT_DT LAMSR_DATA_EFF_STRT_DT
                    ,CPRS.DATA_EFF_STRT_DT CPRS_DATA_EFF_STRT_DT
                    ,FHSRS.DATA_EFF_STRT_DT FHSRS_DATA_EFF_STRT_DT
                    ,LCRS.DATA_EFF_STRT_DT LCRS_DATA_EFF_STRT_DT
                    ,LAMSRS.DATA_EFF_STRT_DT LAMSRS_DATA_EFF_STRT_DT
                FROM 
                ( SELECT * FROM EDV.CLU_YR_LS 
                  WHERE PGM_YR IS NOT NULL AND FLD_NBR IS NOT NULL  AND    DATA_STAT_CD <> 'D'
                    AND CAST(DATA_EFF_END_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
                ) DV_DR
                    INNER JOIN TAB_INCR_DR_ID TMP ON (
                        DV_DR.FARM_TR_L_ID = TMP.INCR_DR_ID
                        AND DV_DR.CLU_YR_ID = TMP.CLU_YR_ID
                        AND DV_DR.PGM_YR = TMP.PGM_YR
                    )
                LEFT JOIN EDV.FARM_TR_L FTL ON (DV_DR.FARM_TR_L_ID = FTL.FARM_TR_L_ID)
                LEFT JOIN EDV.TR_H TH ON (COALESCE(FTL.TR_H_ID, '6de912369edfb89b50859d8f305e4f72') = TH.TR_H_ID)
                LEFT JOIN EDV.TR_HS THS ON (COALESCE(THS.TR_H_ID, '6de912369edfb89b50859d8f305e4f72') = TH.TR_H_ID)
                LEFT JOIN EDV.LOC_AREA_MRT_SRC_RS LAMS ON (
                        COALESCE(LAMS.CTRY_DIV_MRT_CD, '--') = TH.ST_FSA_CD
                        AND COALESCE(LAMS.LOC_AREA_MRT_CD, '--') = TH.CNTY_FSA_CD
                        AND TRIM(LAMS.LOC_AREA_MRT_CD_SRC_ACRO) = 'FSA'
                        )
                LEFT JOIN EDV.LOC_AREA_RH LAH ON (
                        COALESCE(LAMS.LOC_AREA_CAT_NM, '[NULL IN SOURCE]') = LAH.LOC_AREA_CAT_NM
                        AND COALESCE(LAMS.LOC_AREA_NM, '[NULL IN SOURCE]') = LAH.LOC_AREA_NM
                        AND COALESCE(LAMS.CTRY_DIV_NM, '[NULL IN SOURCE]') = LAH.CTRY_DIV_NM
                        AND TRIM(LAMS.LOC_AREA_MRT_CD_SRC_ACRO) = 'FSA'
                        )
                LEFT JOIN CMN_DIM_DM_STG.FSA_ST_CNTY_DIM FS_DIM ON (
                        LAH.DURB_ID = FS_DIM.FSA_ST_CNTY_DURB_ID
                        AND FS_DIM.CUR_RCD_IND = 1
                        )
                LEFT JOIN EBV.ANSI_ST_CNTY_RH ASCH ON (
                        COALESCE(DV_DR.ST_ANSI_CD, '--') = ASCH.ST_ANSI_CD
                        AND COALESCE(DV_DR.CNTY_ANSI_CD, '--') = ASCH.CNTY_ANSI_CD
                        )
                LEFT JOIN CMN_DIM_DM_STG.ANSI_ST_CNTY_DIM ASC_DIM ON (
                        ASCH.DURB_ID = ASC_DIM.ANSI_ST_CNTY_DURB_ID
                        AND ASC_DIM.CUR_RCD_IND = 1
                        )
                LEFT JOIN EDV.LOC_AREA_MRT_SRC_RS LAMSR ON (
                        COALESCE(DV_DR.LOC_ST_FSA_CD, '--') = LAMSR.CTRY_DIV_MRT_CD
                        AND COALESCE(DV_DR.LOC_CNTY_FSA_CD, '--') = LAMSR.LOC_AREA_MRT_CD
                        AND TRIM(LAMSR.LOC_AREA_MRT_CD_SRC_ACRO) = 'FSA'
                        )
                LEFT JOIN EDV.LOC_AREA_RH LARH ON (
                        COALESCE(LAMSR.LOC_AREA_CAT_NM, '[NULL IN SOURCE]') = LARH.LOC_AREA_CAT_NM
                        AND COALESCE(LAMSR.LOC_AREA_NM, '[NULL IN SOURCE]') = LARH.LOC_AREA_NM
                        AND COALESCE(LAMSR.CTRY_DIV_NM, '[NULL IN SOURCE]') = LARH.CTRY_DIV_NM
                        AND TRIM(LAMSR.LOC_AREA_MRT_CD_SRC_ACRO) = 'FSA'
                        )
                LEFT JOIN CMN_DIM_DM_STG.FSA_ST_CNTY_DIM FSC_DIM ON (
                        LARH.DURB_ID = FSC_DIM.FSA_ST_CNTY_DURB_ID
                        AND FSC_DIM.CUR_RCD_IND = 1
                        )
                LEFT JOIN EDV.CNSV_PRAC_RS CPRS ON (COALESCE(DV_DR.SRC_CNSV_PRAC_ID, - 1) = COALESCE(CPRS.DMN_VAL_ID, - 1))
                LEFT JOIN EDV.CNSV_PRAC_RH CPRH ON (COALESCE(CPRS.CNSV_PRAC_ID, '[NULL IN SOURCE]') = CPRH.CNSV_PRAC_ID)
                LEFT JOIN CMN_DIM_DM_STG.CNSV_PRAC_DIM CP_DIM ON (
                        CPRH.DURB_ID = CP_DIM.CNSV_PRAC_DURB_ID
                        AND CP_DIM.CUR_RCD_IND = 1
                        )
                LEFT JOIN EDV.FARM_H FH ON (COALESCE(FTL.FARM_H_ID, 'baf6dd71fe45fe2f5c1c0e6724d514fd') = FH.FARM_H_ID)
                LEFT JOIN CMN_DIM_DM_STG.FARM_DIM FR_DIM ON (
                        FH.DURB_ID = FR_DIM.FARM_DURB_ID
                        AND FR_DIM.CUR_RCD_IND = 1
                        )
                LEFT JOIN EDV.FLD_HEL_STAT_RS FHSRS ON (COALESCE(DV_DR.SRC_HEL_STAT_ID, - 1) = COALESCE(FHSRS.DMN_VAL_ID, - 1))
                LEFT JOIN EDV.FLD_HEL_STAT_RH FHSH ON (COALESCE(FHSRS.FLD_HEL_STAT_CD, '[NULL IN SOURCE]') = FHSH.FLD_HEL_STAT_CD)
                LEFT JOIN FARM_DM_STG.HEL_STAT_DIM HEL_DIM ON (
                        FHSH.DURB_ID = HEL_DIM.HEL_STAT_DURB_ID
                        AND HEL_DIM.CUR_RCD_IND = 1
                        )
                LEFT JOIN EDV.LAND_CLS_RS LCRS ON (COALESCE(DV_DR.SRC_LAND_CLS_ID, - 1) = COALESCE(LCRS.DMN_VAL_ID, - 1))
                LEFT JOIN EDV.LAND_CLS_RH LCRH ON (COALESCE(LCRS.LAND_CLS_ID, '[NULL IN SOURCE]') = LCRH.LAND_CLS_ID)
                LEFT JOIN FARM_DM_STG.LAND_CLS_DIM LC_DIM ON (
                        LCRH.DURB_ID = LC_DIM.LAND_CLS_DURB_ID
                        AND LC_DIM.CUR_RCD_IND = 1
                        )
                LEFT JOIN CMN_DIM_DM_STG.TR_DIM TR_DIM ON (
                        TH.DURB_ID = TR_DIM.TR_DURB_ID
                        AND TR_DIM.CUR_RCD_IND = 1
                        )
                LEFT JOIN EDV.LOC_AREA_MRT_SRC_RS LAMSRS ON (
                        COALESCE(THS.LOC_ST_FSA_CD, '--') = LAMSRS.CTRY_DIV_MRT_CD
                        AND COALESCE(THS.LOC_CNTY_FSA_CD, '--') = LAMSRS.LOC_AREA_MRT_CD
                        AND TRIM(LAMSRS.LOC_AREA_MRT_CD_SRC_ACRO) = 'FSA'
                        )
                LEFT JOIN EDV.LOC_AREA_RH LARAH ON (
                        COALESCE(LAMSRS.LOC_AREA_CAT_NM, '[NULL IN SOURCE]') = LARAH.LOC_AREA_CAT_NM
                        AND COALESCE(LAMSRS.LOC_AREA_NM, '[NULL IN SOURCE]') = LARAH.LOC_AREA_NM
                        AND COALESCE(LAMSRS.CTRY_DIV_NM, '[NULL IN SOURCE]') = LARAH.CTRY_DIV_NM
                        AND TRIM(LAMSRS.LOC_AREA_MRT_CD_SRC_ACRO) = 'FSA'
                        )
                LEFT JOIN CMN_DIM_DM_STG.FSA_ST_CNTY_DIM FSCD_DIM ON (
                        LARAH.DURB_ID = FSCD_DIM.FSA_ST_CNTY_DURB_ID
                        AND FSCD_DIM.CUR_RCD_IND = 1
                        )
                LEFT JOIN CMN_DIM_DM_STG.CONG_DIST_DIM CONG_DIM ON (
                        CONG_DIM.ST_FSA_CD = THS.LOC_ST_FSA_CD
                        AND CONG_DIM.CONG_DIST_CD = COALESCE(THS.CONG_DIST_CD, '--')
                        AND CONG_DIM.CUR_RCD_IND = 1
                        )
                WHERE (
                CAST(DV_DR.DATA_EFF_END_DT AS DATE) =  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
                )
				AND     FR_DIM.FARM_DURB_ID IS NOT NULL
				AND     DV_DR.PGM_YR IS NOT NULL
				AND     TR_DIM.TR_DURB_ID IS NOT NULL
				AND     DV_DR.FLD_NBR IS NOT null
	
				) sub_1
            WHERE SRC_RANK = 1
            ) sub
        ) DM_1
    ) DM
WHERE DM.ROW_NUM_PART = 1
ORDER BY DM.PGM_YR, DM.FARM_DURB_ID, DM.TR_DURB_ID, DM.FLD_NBR
)
SELECT  clu_yr_cre_dt as cre_dt, 
        last_chg_dt, 
        data_stat_cd, 
        pgm_yr, 
        adm_fsa_st_cnty_srgt_id, 
        adm_fsa_st_cnty_durb_id, 
        farm_srgt_id, 
        farm_durb_id, 
        tr_srgt_id, 
        tr_durb_id, 
        tr_loc_fsa_st_cnty_srgt_id, 
        tr_loc_fsa_st_cnty_durb_id, 
        cong_dist_srgt_id, 
        cong_dist_durb_id, 
        hel_stat_srgt_id, 
        hel_stat_durb_id, 
        cnsv_prac_srgt_id, 
        cnsv_prac_durb_id, land_cls_srgt_id, 
        land_cls_durb_id, 
        clu_loc_fsa_st_cnty_srgt_id, 
        clu_loc_fsa_st_cnty_durb_id, 
        ansi_st_cnty_srgt_id, 
        ansi_st_cnty_durb_id, 
        fld_nbr, 
        clu_yr_cre_dt, 
        clu_yr_last_chg_dt, 
        clu_yr_last_chg_user_nm, 
        src_data_stat_cd, 
        clu_alt_id, 
        cpld_ind_3cm, 
        clu_desc, 
        crp_ctr_nbr, 
        crp_ctr_expr_dt, 
        ntv_sod_cvsn_dt, 
        sod_cvsn_crop_yr_1, 
        sod_cvsn_crop_yr_2, 
        sod_cvsn_crop_yr_3, 
        sod_cvsn_crop_yr_4, 
        clu_acrg
FROM del_data TAB_INCR