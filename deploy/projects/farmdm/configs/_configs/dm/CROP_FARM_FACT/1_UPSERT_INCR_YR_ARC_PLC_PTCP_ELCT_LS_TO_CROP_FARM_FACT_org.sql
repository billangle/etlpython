with APP_DS_RW_CF_INCR_DR_ID as (
            SELECT 
            DISTINCT CROP_FARM_L_ID INCR_DR_ID,
                             YR_ARC_PLC_PTCP_ELCT_ID
            FROM EDV.YR_ARC_PLC_PTCP_ELCT_LS
            WHERE CAST(DATA_EFF_STRT_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
            AND CAST(DATA_EFF_END_DT AS DATE) > TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD') 
            
            UNION
            
            SELECT  
                   DISTINCT CROP_FARM_L.CROP_FARM_L_ID INCR_DR_ID, 
                   YR_ARC_PLC_PTCP_ELCT_ID
            FROM EDV.LOC_AREA_MRT_SRC_RS, EDV.FARM_H, EDV.CROP_FARM_L, EDV.YR_ARC_PLC_PTCP_ELCT_LS 
            WHERE CAST(LOC_AREA_MRT_SRC_RS.DATA_EFF_STRT_DT AS DATE) =  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')  
                  AND CAST(LOC_AREA_MRT_SRC_RS.DATA_EFF_END_DT AS DATE) >  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
                  AND EDV.FARM_H.ST_FSA_CD = COALESCE(LOC_AREA_MRT_SRC_RS.CTRY_DIV_MRT_CD,'X') 
                  AND FARM_H.CNTY_FSA_CD = COALESCE(LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD,'X') 
                  AND (COALESCE(CROP_FARM_L.FARM_H_ID,'baf6dd71fe45fe2f5c1c0e6724d514fd') = FARM_H.FARM_H_ID) 
                  AND (COALESCE(YR_ARC_PLC_PTCP_ELCT_LS.CROP_FARM_L_ID,'6bb61e3b7bce0931da574d19d1d82c88') = CROP_FARM_L.CROP_FARM_L_ID) 
                  AND LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'
         
            UNION
            
            SELECT 
                DISTINCT YR_ARC_PLC_PTCP_ELCT_LS.CROP_FARM_L_ID INCR_DR_ID,
                         YR_ARC_PLC_PTCP_ELCT_ID
            FROM EDV.ARC_PLC_ELCT_RS, EDV.YR_ARC_PLC_PTCP_ELCT_LS
            WHERE (CAST(ARC_PLC_ELCT_RS.DATA_EFF_STRT_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
                   OR CAST(ARC_PLC_ELCT_RS.DATA_EFF_END_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
                  )
            AND YR_ARC_PLC_PTCP_ELCT_LS.SRC_ARC_PLC_ELCT_CHC_ID = ARC_PLC_ELCT_RS.DMN_VAL_ID
            
            UNION
            
            SELECT 
                DISTINCT YR_ARC_PLC_PTCP_ELCT_LS.CROP_FARM_L_ID INCR_DR_ID,
                         YR_ARC_PLC_PTCP_ELCT_ID
            FROM EDV.ARC_PLC_ELG_DTER_RS, EDV.YR_ARC_PLC_PTCP_ELCT_LS, EDV.FARM_YR_HS 
            WHERE (CAST(ARC_PLC_ELG_DTER_RS.DATA_EFF_STRT_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
                   OR CAST(ARC_PLC_ELG_DTER_RS.DATA_EFF_END_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
                  )
            AND ARC_PLC_ELG_DTER_RS.ARC_PLC_ELG_DTER_CD = COALESCE(FARM_YR_HS.ARC_PLC_ELG_DTER_CD, '[NULL IN SOURCE]')
            AND FARM_YR_HS.FARM_YR_ID = COALESCE(YR_ARC_PLC_PTCP_ELCT_LS.FARM_YR_ID, -1)
            
            UNION
            
            SELECT 
                DISTINCT YR_ARC_PLC_PTCP_ELCT_LS.CROP_FARM_L_ID INCR_DR_ID,
                         YR_ARC_PLC_PTCP_ELCT_ID
            FROM EDV.YR_ARC_PLC_PTCP_ELCT_LS, EDV.FARM_YR_HS 
            WHERE (CAST(FARM_YR_HS.DATA_EFF_STRT_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
                   OR CAST(FARM_YR_HS.DATA_EFF_END_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
                  )
            AND FARM_YR_HS.FARM_YR_ID = COALESCE(YR_ARC_PLC_PTCP_ELCT_LS.FARM_YR_ID, -1)
), 
APP_DS_RW_TGT_INCR_DR_ID as ( 
SELECT DM.FARM_SRGT_ID
    ,DM.FARM_DURB_ID
    ,DM.ADM_FSA_ST_CNTY_SRGT_ID
    ,DM.ADM_FSA_ST_CNTY_DURB_ID
    ,DM.PGM_YR
    ,DM.FSA_CROP_SRGT_ID
    ,DM.FSA_CROP_DURB_ID
    ,DM.ARC_PLC_ELCT_SRGT_ID
    ,DM.ARC_PLC_ELCT_DURB_ID
    ,DM.ARC_PLC_PTCP_ELCT_CRE_DT
    ,DM.ARC_PLC_PTCP_ELCT_CRE_USER_NM
    ,DM.ARC_PLC_PTCP_ELCT_LAST_CHG_DT
    ,DM.ARC_PLC_ELCT_LAST_CHG_USER_NM
    ,DM.SRC_DATA_STAT_CD
    ,DM.ARC_PLC_ELG_DTER_DURB_ID
    ,DM.DATA_EFF_STRT_DT
FROM (
    SELECT DM_1.FARM_SRGT_ID
        ,DM_1.FARM_DURB_ID
        ,DM_1.ADM_FSA_ST_CNTY_SRGT_ID
        ,DM_1.ADM_FSA_ST_CNTY_DURB_ID
        ,DM_1.PGM_YR
        ,DM_1.FSA_CROP_SRGT_ID
        ,DM_1.FSA_CROP_DURB_ID
        ,DM_1.ARC_PLC_ELCT_SRGT_ID
        ,DM_1.ARC_PLC_ELCT_DURB_ID
        ,DM_1.ARC_PLC_PTCP_ELCT_CRE_DT
        ,DM_1.ARC_PLC_PTCP_ELCT_CRE_USER_NM
        ,DM_1.ARC_PLC_PTCP_ELCT_LAST_CHG_DT
        ,DM_1.ARC_PLC_ELCT_LAST_CHG_USER_NM
        ,DM_1.SRC_DATA_STAT_CD
        
        ,DM_1.ARC_PLC_ELG_DTER_DURB_ID
        ,DM_1.DATA_EFF_STRT_DT
        ,DM_1.DV_DR_DATA_EFF_STRT_DT
        ,DM_1.LAMS_DATA_EFF_STRT_DT
        ,DM_1.APERS_DATA_EFF_STRT_DT
        ,DM_1.APEDRS_DATA_EFF_STRT_DT
        ,DM_1.FYHS_DATA_EFF_STRT_DT
        ,ROW_NUMBER() OVER (PARTITION BY PGM_YR
                                        ,FSA_CROP_DURB_ID
                                        ,FARM_DURB_ID
                            ORDER BY DV_DR_DATA_EFF_STRT_DT DESC
                                    ,LAMS_DATA_EFF_STRT_DT DESC
                                    ,APERS_DATA_EFF_STRT_DT DESC
                                    ,FYHS_DATA_EFF_STRT_DT DESC
                                    ,APEDRS_DATA_EFF_STRT_DT DESC
            ) AS ROW_NUM_PART
    FROM (
        SELECT FARM_SRGT_ID
            ,FARM_DURB_ID
            ,ADM_FSA_ST_CNTY_SRGT_ID
            ,ADM_FSA_ST_CNTY_DURB_ID
            ,PGM_YR
            ,FSA_CROP_SRGT_ID
            ,FSA_CROP_DURB_ID
            ,ARC_PLC_ELCT_SRGT_ID
            ,ARC_PLC_ELCT_DURB_ID
            ,ARC_PLC_PTCP_ELCT_CRE_DT
            ,ARC_PLC_PTCP_ELCT_CRE_USER_NM
            ,ARC_PLC_PTCP_ELCT_LAST_CHG_DT
            ,ARC_PLC_ELCT_LAST_CHG_USER_NM
            ,SRC_DATA_STAT_CD
            ,ARC_PLC_ELG_DTER_DURB_ID
            ,DATA_EFF_STRT_DT
            ,DV_DR_DATA_EFF_STRT_DT
            ,LAMS_DATA_EFF_STRT_DT
            ,APERS_DATA_EFF_STRT_DT
            ,APEDRS_DATA_EFF_STRT_DT
            ,FYHS_DATA_EFF_STRT_DT
        FROM (
                SELECT *
                FROM (
                        SELECT DISTINCT COALESCE(FR_DIM.FARM_SRGT_ID, -3) AS FARM_SRGT_ID
                        ,FR_DIM.FARM_DURB_ID AS FARM_DURB_ID
                        ,COALESCE(FS_DIM.FSA_ST_CNTY_SRGT_ID, -3) AS ADM_FSA_ST_CNTY_SRGT_ID
                        ,COALESCE(FS_DIM.FSA_ST_CNTY_DURB_ID, -3) AS ADM_FSA_ST_CNTY_DURB_ID
                        ,DV_DR.PGM_YR AS PGM_YR
                        ,COALESCE(FSA_DIM.FSA_CROP_SRGT_ID, -3) AS FSA_CROP_SRGT_ID
                        ,FSA_DIM.FSA_CROP_DURB_ID AS FSA_CROP_DURB_ID
                        ,COALESCE(APE_DIM.ARC_PLC_ELCT_SRGT_ID, -3) AS ARC_PLC_ELCT_SRGT_ID
                        ,APE_DIM.ARC_PLC_ELCT_DURB_ID AS ARC_PLC_ELCT_DURB_ID
                        ,DV_DR.SRC_CRE_DT AS ARC_PLC_PTCP_ELCT_CRE_DT
                        ,DV_DR.SRC_CRE_USER_NM AS ARC_PLC_PTCP_ELCT_CRE_USER_NM
                        ,DV_DR.SRC_LAST_CHG_DT AS ARC_PLC_PTCP_ELCT_LAST_CHG_DT
                        ,DV_DR.SRC_LAST_CHG_USER_NM AS ARC_PLC_ELCT_LAST_CHG_USER_NM
                        ,DV_DR.DATA_STAT_CD AS SRC_DATA_STAT_CD
                        ,COALESCE(aped_dim.ARC_PLC_ELG_DTER_DURB_ID, -3) AS ARC_PLC_ELG_DTER_DURB_ID
                        ,GREATEST(COALESCE(DV_DR.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')), 
                                  COALESCE(LAMS.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')),
                                  COALESCE(APERS.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')),
                                  COALESCE(APEDRS.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')),
                                  COALESCE(FYHS.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD'))
                                 ) DATA_EFF_STRT_DT
                        ,DV_DR.DATA_EFF_STRT_DT DV_DR_DATA_EFF_STRT_DT
                        ,LAMS.DATA_EFF_STRT_DT LAMS_DATA_EFF_STRT_DT 
                        ,APERS.DATA_EFF_STRT_DT APERS_DATA_EFF_STRT_DT
                        ,APEDRS.DATA_EFF_STRT_DT APEDRS_DATA_EFF_STRT_DT
                        ,FYHS.DATA_EFF_STRT_DT FYHS_DATA_EFF_STRT_DT
                FROM EDV.YR_ARC_PLC_PTCP_ELCT_LS DV_DR
                LEFT JOIN EDV.CROP_FARM_L CFL 
                ON (COALESCE(DV_DR.CROP_FARM_L_ID, '6bb61e3b7bce0931da574d19d1d82c88') = CFL.CROP_FARM_L_ID)
                LEFT JOIN EDV.FARM_H FH 
                ON (COALESCE(CFL.FARM_H_ID, 'baf6dd71fe45fe2f5c1c0e6724d514fd') = FH.FARM_H_ID)
                LEFT JOIN EDV.LOC_AREA_MRT_SRC_RS LAMS 
                ON (FH.ST_FSA_CD = COALESCE(LAMS.CTRY_DIV_MRT_CD, '--')
                    AND FH.CNTY_FSA_CD = COALESCE(LAMS.LOC_AREA_MRT_CD, '--')
                    AND LAMS.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA')
                LEFT JOIN EDV.LOC_AREA_RH LAH 
                ON (COALESCE(LAMS.LOC_AREA_CAT_NM, '[NULL IN SOURCE]') = LAH.LOC_AREA_CAT_NM
                    AND COALESCE(LAMS.LOC_AREA_NM, '[NULL IN SOURCE]') = LAH.LOC_AREA_NM
                    AND COALESCE(LAMS.CTRY_DIV_NM, '[NULL IN SOURCE]') = LAH.CTRY_DIV_NM
                    AND LAMS.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA')
                LEFT JOIN CMN_DIM_DM_STG.FSA_ST_CNTY_DIM FS_DIM 
                ON (LAH.DURB_ID = COALESCE(FS_DIM.FSA_ST_CNTY_DURB_ID, -1)
                    AND FS_DIM.CUR_RCD_IND = 1)
                LEFT JOIN EDV.ARC_PLC_ELCT_RS APERS 
                ON (DV_DR.SRC_ARC_PLC_ELCT_CHC_ID = APERS.DMN_VAL_ID)
                LEFT JOIN EDV.ARC_PLC_ELCT_RH APERH 
                ON (COALESCE(APERS.ARC_PLC_ELCT_CD, '[NULL IN SOURCE]') = APERH.ARC_PLC_ELCT_CD)
                LEFT JOIN FARM_DM_STG.ARC_PLC_ELCT_DIM APE_DIM 
                ON (APERH.DURB_ID = COALESCE(APE_DIM.ARC_PLC_ELCT_DURB_ID, -1)
                    AND APE_DIM.CUR_RCD_IND = 1)
                LEFT JOIN CMN_DIM_DM_STG.FARM_DIM FR_DIM 
                ON (FH.DURB_ID = COALESCE(FR_DIM.FARM_DURB_ID, -1)
                    AND FR_DIM.CUR_RCD_IND = 1)
                LEFT JOIN EBV.FSA_CROP_TYPE_RH FCTRH 
                ON (COALESCE(CFL.FSA_CROP_CD, '--') = FCTRH.FSA_CROP_CD
                    AND COALESCE(CFL.FSA_CROP_TYPE_CD, '--') = FCTRH.FSA_CROP_TYPE_CD
                    AND DV_DR.PGM_YR = FCTRH.PGM_YR)
                LEFT JOIN CMN_DIM_DM_STG.FSA_CROP_TYPE_DIM FSA_DIM 
                ON (FCTRH.DURB_ID = COALESCE(FSA_DIM.FSA_CROP_DURB_ID, -1)
                    AND FSA_DIM.CUR_RCD_IND = 1)
                LEFT JOIN EDV.FARM_YR_HS FYHS
                ON (COALESCE(DV_DR.FARM_YR_ID, -1) = FYHS.FARM_YR_ID)
                LEFT JOIN EDV.ARC_PLC_ELG_DTER_RH APEDRH 
                ON (COALESCE(FYHS.ARC_PLC_ELG_DTER_CD, '[NULL IN SOURCE]') = APEDRH.ARC_PLC_ELG_DTER_CD)
                LEFT JOIN EDV.ARC_PLC_ELG_DTER_RS APEDRS 
                ON (APEDRH.ARC_PLC_ELG_DTER_CD = COALESCE(APEDRS.ARC_PLC_ELG_DTER_CD, '[NULL IN SOURCE]'))
                LEFT JOIN FARM_DM_STG.ARC_PLC_ELG_DTER_DIM APED_DIM 
                ON (APEDRH.DURB_ID = COALESCE(APED_DIM.ARC_PLC_ELG_DTER_DURB_ID, -1)
                    AND APED_DIM.DATA_STAT_CD = 'A')
                WHERE (
                        DV_DR.CROP_FARM_L_ID,
                        DV_DR.YR_ARC_PLC_PTCP_ELCT_ID
                      ) 
                   IN (
                        SELECT INCR_DR_ID,
                               YR_ARC_PLC_PTCP_ELCT_ID
                        FROM APP_DS_RW_CF_INCR_DR_ID
                      )
                    AND DV_DR.PGM_YR IS NOT NULL
                    AND FSA_DIM.FSA_CROP_DURB_ID IS NOT NULL
                    AND FR_DIM.FARM_DURB_ID IS NOT NULL
                ) sub_1
            ) sub
        ) DM_1
    WHERE CAST(COALESCE(DV_DR_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) AS DATE) <= TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
        AND CAST(COALESCE(LAMS_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) AS DATE) <= TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
        AND CAST(COALESCE(APERS_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) AS DATE) <= TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
        AND CAST(COALESCE(APEDRS_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) AS DATE) <= TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
        AND CAST(COALESCE(FYHS_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) AS DATE) <= TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
    ) DM
WHERE DM.ROW_NUM_PART = 1
),
UPSERT_DATA
AS ( 

SELECT DM.FARM_SRGT_ID
    ,DM.FARM_DURB_ID
    ,DM.ADM_FSA_ST_CNTY_SRGT_ID
    ,DM.ADM_FSA_ST_CNTY_DURB_ID
    ,DM.PGM_YR
    ,DM.FSA_CROP_SRGT_ID
    ,DM.FSA_CROP_DURB_ID
    ,DM.ARC_PLC_ELCT_SRGT_ID
    ,DM.ARC_PLC_ELCT_DURB_ID
    ,DM.ARC_PLC_PTCP_ELCT_CRE_DT
    ,DM.ARC_PLC_PTCP_ELCT_CRE_USER_NM
    ,DM.ARC_PLC_PTCP_ELCT_LAST_CHG_DT
    ,DM.ARC_PLC_ELCT_LAST_CHG_USER_NM
    ,DM.SRC_DATA_STAT_CD
    ,DM.ARC_PLC_ELG_DTER_DURB_ID
    ,DM.DATA_EFF_STRT_DT
FROM (
    SELECT DM_1.FARM_SRGT_ID
        ,DM_1.FARM_DURB_ID
        ,DM_1.ADM_FSA_ST_CNTY_SRGT_ID
        ,DM_1.ADM_FSA_ST_CNTY_DURB_ID
        ,DM_1.PGM_YR
        ,DM_1.FSA_CROP_SRGT_ID
        ,DM_1.FSA_CROP_DURB_ID
        ,DM_1.ARC_PLC_ELCT_SRGT_ID
        ,DM_1.ARC_PLC_ELCT_DURB_ID
        ,DM_1.ARC_PLC_PTCP_ELCT_CRE_DT
        ,DM_1.ARC_PLC_PTCP_ELCT_CRE_USER_NM
        ,DM_1.ARC_PLC_PTCP_ELCT_LAST_CHG_DT
        ,DM_1.ARC_PLC_ELCT_LAST_CHG_USER_NM
        ,DM_1.SRC_DATA_STAT_CD
        ,DM_1.ARC_PLC_ELG_DTER_DURB_ID
        ,DM_1.DATA_EFF_STRT_DT
        ,DM_1.DV_DR_DATA_EFF_STRT_DT
        ,DM_1.LAMS_DATA_EFF_STRT_DT
        ,DM_1.APERS_DATA_EFF_STRT_DT
        ,DM_1.APEDRS_DATA_EFF_STRT_DT
        ,DM_1.FYHS_DATA_EFF_STRT_DT
        ,ROW_NUMBER() OVER (PARTITION BY PGM_YR
                                        ,FSA_CROP_DURB_ID
                                        ,FARM_DURB_ID
                            ORDER BY DV_DR_DATA_EFF_STRT_DT DESC
                                    ,LAMS_DATA_EFF_STRT_DT DESC
                                    ,APERS_DATA_EFF_STRT_DT DESC
                                    ,FYHS_DATA_EFF_STRT_DT DESC
                                    ,APEDRS_DATA_EFF_STRT_DT DESC
            ) AS ROW_NUM_PART
    FROM (
            SELECT FARM_SRGT_ID
                ,FARM_DURB_ID
                ,ADM_FSA_ST_CNTY_SRGT_ID
                ,ADM_FSA_ST_CNTY_DURB_ID
                ,PGM_YR
                ,FSA_CROP_SRGT_ID
                ,FSA_CROP_DURB_ID
                ,ARC_PLC_ELCT_SRGT_ID
                ,ARC_PLC_ELCT_DURB_ID
                ,ARC_PLC_PTCP_ELCT_CRE_DT
                ,ARC_PLC_PTCP_ELCT_CRE_USER_NM
                ,ARC_PLC_PTCP_ELCT_LAST_CHG_DT
                ,ARC_PLC_ELCT_LAST_CHG_USER_NM
                ,SRC_DATA_STAT_CD
                ,ARC_PLC_ELG_DTER_DURB_ID
                ,DATA_EFF_STRT_DT
                ,DV_DR_DATA_EFF_STRT_DT
                ,LAMS_DATA_EFF_STRT_DT
                ,APERS_DATA_EFF_STRT_DT
                ,APEDRS_DATA_EFF_STRT_DT
                ,FYHS_DATA_EFF_STRT_DT
        FROM (
                SELECT *
                FROM (
                        SELECT DISTINCT COALESCE(FR_DIM.FARM_SRGT_ID, -3) AS FARM_SRGT_ID
                                        ,FR_DIM.FARM_DURB_ID AS FARM_DURB_ID
                                        ,COALESCE(FS_DIM.FSA_ST_CNTY_SRGT_ID, -3) AS ADM_FSA_ST_CNTY_SRGT_ID
                                        ,COALESCE(FS_DIM.FSA_ST_CNTY_DURB_ID, -3) AS ADM_FSA_ST_CNTY_DURB_ID
                                        ,DV_DR.PGM_YR AS PGM_YR
                                        ,COALESCE(FSA_DIM.FSA_CROP_SRGT_ID, -3) AS FSA_CROP_SRGT_ID
                                        ,FSA_DIM.FSA_CROP_DURB_ID AS FSA_CROP_DURB_ID
                                        ,COALESCE(APE_DIM.ARC_PLC_ELCT_SRGT_ID, -3) AS ARC_PLC_ELCT_SRGT_ID
                                        ,APE_DIM.ARC_PLC_ELCT_DURB_ID AS ARC_PLC_ELCT_DURB_ID
                                        ,DV_DR.SRC_CRE_DT AS ARC_PLC_PTCP_ELCT_CRE_DT
                                        ,DV_DR.SRC_CRE_USER_NM AS ARC_PLC_PTCP_ELCT_CRE_USER_NM
                                        ,DV_DR.SRC_LAST_CHG_DT AS ARC_PLC_PTCP_ELCT_LAST_CHG_DT
                                        ,DV_DR.SRC_LAST_CHG_USER_NM AS ARC_PLC_ELCT_LAST_CHG_USER_NM
                                        ,DV_DR.DATA_STAT_CD AS SRC_DATA_STAT_CD
                                        ,COALESCE(APED_DIM.ARC_PLC_ELG_DTER_DURB_ID, -3) AS ARC_PLC_ELG_DTER_DURB_ID
                                        ,GREATEST(COALESCE(DV_DR.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')), 
                                                  COALESCE(LAMS.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')),
                                                  COALESCE(APERS.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')),
                                                  COALESCE(APEDRS.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')),
                                                  COALESCE(FYHS.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD'))
                                                 ) DATA_EFF_STRT_DT
                    ,DV_DR.DATA_EFF_STRT_DT DV_DR_DATA_EFF_STRT_DT
                    ,LAMS.DATA_EFF_STRT_DT LAMS_DATA_EFF_STRT_DT 
                    ,APERS.DATA_EFF_STRT_DT APERS_DATA_EFF_STRT_DT
                    ,APEDRS.DATA_EFF_STRT_DT APEDRS_DATA_EFF_STRT_DT
                    ,FYHS.DATA_EFF_STRT_DT FYHS_DATA_EFF_STRT_DT
                    
                FROM EDV.YR_ARC_PLC_PTCP_ELCT_LS DV_DR
                LEFT JOIN EDV.CROP_FARM_L CFL 
                ON (COALESCE(DV_DR.CROP_FARM_L_ID, '6bb61e3b7bce0931da574d19d1d82c88') = CFL.CROP_FARM_L_ID)
                
                LEFT JOIN EDV.FARM_H FH 
                ON (COALESCE(CFL.FARM_H_ID, 'baf6dd71fe45fe2f5c1c0e6724d514fd') = FH.FARM_H_ID)
                
                LEFT JOIN EDV.LOC_AREA_MRT_SRC_RS LAMS 
                ON (FH.ST_FSA_CD = COALESCE(LAMS.CTRY_DIV_MRT_CD, '--')
                    AND FH.CNTY_FSA_CD = COALESCE(LAMS.LOC_AREA_MRT_CD, '--')
                    AND LAMS.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA')
                    
                LEFT JOIN EDV.LOC_AREA_RH LAH 
                ON (COALESCE(LAMS.LOC_AREA_CAT_NM, '[NULL IN SOURCE]') = LAH.LOC_AREA_CAT_NM
                    AND COALESCE(LAMS.LOC_AREA_NM, '[NULL IN SOURCE]') = LAH.LOC_AREA_NM
                    AND COALESCE(LAMS.CTRY_DIV_NM, '[NULL IN SOURCE]') = LAH.CTRY_DIV_NM
                    AND LAMS.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA')
                    
                LEFT JOIN CMN_DIM_DM_STG.FSA_ST_CNTY_DIM FS_DIM 
                ON (LAH.DURB_ID = COALESCE(FS_DIM.FSA_ST_CNTY_DURB_ID, -1)
                    AND FS_DIM.CUR_RCD_IND = 1)
                    
                LEFT JOIN EDV.ARC_PLC_ELCT_RS APERS 
                ON (DV_DR.SRC_ARC_PLC_ELCT_CHC_ID = APERS.DMN_VAL_ID)
                
                LEFT JOIN EDV.ARC_PLC_ELCT_RH APERH 
                ON (COALESCE(APERS.ARC_PLC_ELCT_CD, '[NULL IN SOURCE]') = APERH.ARC_PLC_ELCT_CD)
                
                LEFT JOIN FARM_DM_STG.ARC_PLC_ELCT_DIM APE_DIM 
                ON (APERH.DURB_ID = COALESCE(APE_DIM.ARC_PLC_ELCT_DURB_ID, -1)
                    AND APE_DIM.CUR_RCD_IND = 1)
                    
                LEFT JOIN CMN_DIM_DM_STG.FARM_DIM FR_DIM 
                ON (FH.DURB_ID = COALESCE(FR_DIM.FARM_DURB_ID, -1)
                    AND FR_DIM.CUR_RCD_IND = 1)
                    
                LEFT JOIN EBV.FSA_CROP_TYPE_RH FCTRH 
                ON (COALESCE(CFL.FSA_CROP_CD, '--') = FCTRH.FSA_CROP_CD
                    AND COALESCE(CFL.FSA_CROP_TYPE_CD, '--') = FCTRH.FSA_CROP_TYPE_CD
                    AND DV_DR.PGM_YR = FCTRH.PGM_YR)
                    
                LEFT JOIN CMN_DIM_DM_STG.FSA_CROP_TYPE_DIM FSA_DIM 
                ON (FCTRH.DURB_ID = COALESCE(FSA_DIM.FSA_CROP_DURB_ID, -1)
                    AND FSA_DIM.CUR_RCD_IND = 1)
                       
                LEFT JOIN EDV.FARM_YR_HS FYHS
                ON (COALESCE(DV_DR.FARM_YR_ID, -1) = FYHS.FARM_YR_ID)
                
                LEFT JOIN EDV.ARC_PLC_ELG_DTER_RH APEDRH 
                ON (COALESCE(FYHS.ARC_PLC_ELG_DTER_CD, '[NULL IN SOURCE]') = APEDRH.ARC_PLC_ELG_DTER_CD)
                
                LEFT JOIN EDV.ARC_PLC_ELG_DTER_RS APEDRS 
                ON (APEDRH.ARC_PLC_ELG_DTER_CD = COALESCE(APEDRS.ARC_PLC_ELG_DTER_CD, '[NULL IN SOURCE]'))
                
                LEFT JOIN FARM_DM_STG.ARC_PLC_ELG_DTER_DIM APED_DIM 
                ON (APEDRH.DURB_ID = COALESCE(APED_DIM.ARC_PLC_ELG_DTER_DURB_ID, -1)
                    AND APED_DIM.DATA_STAT_CD = 'A')
                WHERE (
                        DV_DR.CROP_FARM_L_ID
                        ,DV_DR.YR_ARC_PLC_PTCP_ELCT_ID
                      ) 
                   IN (
                        SELECT INCR_DR_ID
                              ,YR_ARC_PLC_PTCP_ELCT_ID
                        FROM APP_DS_RW_CF_INCR_DR_ID
                      )
                    AND DV_DR.PGM_YR IS NOT NULL
                    AND FSA_DIM.FSA_CROP_DURB_ID IS NOT NULL
                    AND FR_DIM.FARM_DURB_ID IS NOT NULL
                ) sub_1
            ) sub
        ) DM_1
    WHERE CAST(COALESCE(DV_DR_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) AS DATE) <= TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
          AND CAST(COALESCE(LAMS_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) AS DATE) <= TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
          AND CAST(COALESCE(APERS_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) AS DATE) <= TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
          AND CAST(COALESCE(APEDRS_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) AS DATE) <= TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
          AND CAST(COALESCE(FYHS_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) AS DATE) <= TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
    ) DM
WHERE DM.ROW_NUM_PART = 1
)
SELECT 
	   (current_date) as cre_dt,
	   (current_date) as last_chg_dt, 
       'A' as data_stat_cd,
	   farm_srgt_id, 
       farm_durb_id, 
       adm_fsa_st_cnty_srgt_id, 
       adm_fsa_st_cnty_durb_id, 
       pgm_yr, 
       fsa_crop_srgt_id, 
       fsa_crop_durb_id, 
       arc_plc_elct_srgt_id, 
       arc_plc_elct_durb_id, 
       arc_plc_ptcp_elct_cre_dt, 
       arc_plc_ptcp_elct_cre_user_nm, 
       arc_plc_ptcp_elct_last_chg_dt, 
       arc_plc_elct_last_chg_user_nm, 
       src_data_stat_cd, 
       arc_plc_elg_dter_durb_id
FROM UPSERT_DATA TAB_INCR
 


