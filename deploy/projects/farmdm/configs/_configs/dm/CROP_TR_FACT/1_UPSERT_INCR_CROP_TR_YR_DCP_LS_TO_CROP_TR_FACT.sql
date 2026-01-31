WITH TAB_INCR_DR_ID AS
(
        SELECT    DISTINCT CROP_TR_DCP_L_ID INCR_DR_ID, CROP_TR_YR_DCP_ID
        FROM        EDV.CROP_TR_YR_DCP_LS CROP_TR_YR_DCP_LS
        WHERE       CAST(DATA_EFF_STRT_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
            AND     CAST(DATA_EFF_END_DT AS DATE) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')       
        UNION  
        SELECT      DISTINCT CROP_TR_DCP_L.CROP_TR_DCP_L_ID INCR_DR_ID, CROP_TR_YR_DCP_ID
        FROM        EDV.TR_HS TR_HS, EDV.TR_H TR_H, EDV.CROP_TR_DCP_L CROP_TR_DCP_L,EDV.CROP_TR_YR_DCP_LS CROP_TR_YR_DCP_LS
        WHERE       CAST(TR_HS.DATA_EFF_STRT_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
            AND     CAST(TR_HS.DATA_EFF_END_DT AS DATE) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
            AND     COALESCE(TR_HS.TR_H_ID,'6de912369edfb89b50859d8f305e4f72') = TR_H.TR_H_ID 
            AND     CAST(CROP_TR_YR_DCP_LS.DATA_EFF_END_DT AS DATE) = TO_TIMESTAMP('9999-12-31','YYYY-MM-DD')
            AND     COALESCE(CROP_TR_DCP_L.TR_H_ID,'6de912369edfb89b50859d8f305e4f72') = TR_H.TR_H_ID
            AND     (CROP_TR_YR_DCP_LS.CROP_TR_DCP_L_ID = CROP_TR_DCP_L.CROP_TR_DCP_L_ID)      
        UNION        
        SELECT      DISTINCT CROP_TR_DCP_L.CROP_TR_DCP_L_ID INCR_DR_ID, CROP_TR_YR_DCP_ID
        FROM        EDV.LOC_AREA_MRT_SRC_RS LOC_AREA_MRT_SRC_RS, EDV.TR_H TR_H, EDV.CROP_TR_DCP_L CROP_TR_DCP_L, EDV.CROP_TR_YR_DCP_LS CROP_TR_YR_DCP_LS
        WHERE       (
                        CAST(LOC_AREA_MRT_SRC_RS.DATA_EFF_STRT_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD') 
                        OR
                        CAST(LOC_AREA_MRT_SRC_RS.DATA_EFF_END_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
                    )
            AND     TR_H.ST_FSA_CD = COALESCE(LOC_AREA_MRT_SRC_RS.CTRY_DIV_MRT_CD, '--')
            AND     TR_H.CNTY_FSA_CD = COALESCE(LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD, '--')
            AND     TRIM(LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD_SRC_ACRO) = 'FSA'
            AND     COALESCE(CROP_TR_DCP_L.TR_H_ID,'6de912369edfb89b50859d8f305e4f72') = TR_H.TR_H_ID
            AND     CROP_TR_YR_DCP_LS.CROP_TR_DCP_L_ID = CROP_TR_DCP_L.CROP_TR_DCP_L_ID      
        UNION        
        SELECT       DISTINCT CROP_TR_DCP_L.CROP_TR_DCP_L_ID INCR_DR_ID, CROP_TR_YR_DCP_ID
        FROM         EDV.LOC_AREA_MRT_SRC_RS LOC_AREA_MRT_SRC_RS, EDV.TR_HS TR_HS, EDV.TR_H TR_H, EDV.CROP_TR_DCP_L CROP_TR_DCP_L, EDV.CROP_TR_YR_DCP_LS CROP_TR_YR_DCP_LS
        WHERE       (
                        CAST(LOC_AREA_MRT_SRC_RS.DATA_EFF_STRT_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD') 
                        OR
                        CAST(LOC_AREA_MRT_SRC_RS.DATA_EFF_END_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}', 'YYYY-MM-DD')
                     )
            AND     TR_HS.LOC_ST_FSA_CD = LOC_AREA_MRT_SRC_RS.CTRY_DIV_MRT_CD
            AND     TR_HS.LOC_CNTY_FSA_CD = LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD
            AND     LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'
            AND     COALESCE(TR_HS.TR_H_ID,'6de912369edfb89b50859d8f305e4f72') = TR_H.TR_H_ID 
            AND     COALESCE(CROP_TR_DCP_L.TR_H_ID,'6de912369edfb89b50859d8f305e4f72') = TR_H.TR_H_ID
            AND     CROP_TR_YR_DCP_LS.CROP_TR_DCP_L_ID = CROP_TR_DCP_L.CROP_TR_DCP_L_ID           
        UNION        
        SELECT      DISTINCT CROP_TR_YR_DCP_LS.CROP_TR_DCP_L_ID INCR_DR_ID, CROP_TR_YR_DCP_LS.CROP_TR_YR_DCP_ID
        FROM        EDV.CROP_TR_YR_DCP_LS CROP_TR_YR_DCP_LS
                    LEFT JOIN EDV.CROP_TR_DCP_L CROP_TR_DCP_L ON CROP_TR_YR_DCP_LS.CROP_TR_DCP_L_ID = CROP_TR_DCP_L.CROP_TR_DCP_L_ID
                    LEFT JOIN EDV.TR_H TR_H ON CROP_TR_DCP_L.TR_H_ID = TR_H.TR_H_ID
                    LEFT JOIN EDV.CROP_TR_CTR_L CROP_TR_CTR_L ON TR_H.TR_H_ID = CROP_TR_CTR_L.TR_H_ID
                    LEFT JOIN EDV.CROP_TR_CTR_LS CROP_TR_CTR_LS ON CROP_TR_CTR_L.CROP_TR_CTR_L_ID = CROP_TR_CTR_LS.CROP_TR_CTR_L_ID
        WHERE       CAST(CROP_TR_CTR_LS.DATA_EFF_STRT_DT AS DATE) = TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
            AND     CAST(CROP_TR_CTR_LS.DATA_EFF_END_DT AS DATE) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')            
            
),
INSERT_DATA
AS (
SELECT      DM.FARM_SRGT_ID,
            DM.FARM_DURB_ID,
            DM.ADM_FSA_ST_CNTY_SRGT_ID,
            DM.ADM_FSA_ST_CNTY_DURB_ID,
            DM.PGM_YR,
            DM.TR_SRGT_ID,
            DM.TR_DURB_ID,
            DM.FSA_CROP_SRGT_ID,
            DM.FSA_CROP_DURB_ID,
            DM.LOC_FSA_ST_CNTY_SRGT_ID,
            DM.LOC_FSA_ST_CNTY_DURB_ID,
            DM.CONG_DIST_SRGT_ID,
            DM.CONG_DIST_DURB_ID,
            DM.CROP_TR_YR_DCP_CRE_DT,
            DM.CROP_TR_YR_DCP_LAST_CHG_DT,
            DM.CROP_TR_YR_LAST_CHG_USER_NM,
            DM.SRC_DATA_STAT_CD,
            DM.CRP_RDN_ACRG,
            DM.CRP_REL_ACRG,
            DM.DCP_CROP_BASE_ACRG,
            DM.FAV_RDN_ACRG,
            DM.CCP_PYMT_YLD,
            DM.CRP_PYMT_YLD,
            DM.DIR_PYMT_YLD,
            DM.FAV_DIR_PYMT_YLD,
            DM.FAV_CCP_PYMT_YLD,
            DM.DATA_EFF_STRT_DT,
            CTR_RDN_ACRG
FROM        (
                SELECT      DM_1.FARM_SRGT_ID,
                            DM_1.FARM_DURB_ID,
                            DM_1.ADM_FSA_ST_CNTY_SRGT_ID,
                            DM_1.ADM_FSA_ST_CNTY_DURB_ID,
                            DM_1.PGM_YR,
                            DM_1.TR_SRGT_ID,
                            DM_1.TR_DURB_ID,
                            DM_1.FSA_CROP_SRGT_ID,
                            DM_1.FSA_CROP_DURB_ID,
                            DM_1.LOC_FSA_ST_CNTY_SRGT_ID,
                            DM_1.LOC_FSA_ST_CNTY_DURB_ID,
                            DM_1.CONG_DIST_SRGT_ID,
                            DM_1.CONG_DIST_DURB_ID,
                            DM_1.CROP_TR_YR_DCP_CRE_DT,
                            DM_1.CROP_TR_YR_DCP_LAST_CHG_DT,
                            DM_1.CROP_TR_YR_LAST_CHG_USER_NM,
                            DM_1.SRC_DATA_STAT_CD,
                            DM_1.CRP_RDN_ACRG,
                            DM_1.CRP_REL_ACRG,
                            DM_1.DCP_CROP_BASE_ACRG,
                            DM_1.FAV_RDN_ACRG,
                            DM_1.CCP_PYMT_YLD,
                            DM_1.CRP_PYMT_YLD,
                            DM_1.DIR_PYMT_YLD,
                            DM_1.FAV_DIR_PYMT_YLD,
                            DM_1.FAV_CCP_PYMT_YLD,
                            DM_1.DATA_EFF_STRT_DT,
                            DM_1.DV_DR_DATA_EFF_STRT_DT,
                            DM_1.THS_DATA_EFF_STRT_DT, 
                            DM_1.LAMS_DATA_EFF_STRT_DT,
                            DM_1.LAMSR_DATA_EFF_STRT_DT,
                            CTR_RDN_ACRG,
                            ROW_NUMBER() OVER
                            (
                                PARTITION BY 
                                                FARM_DURB_ID,
                                                PGM_YR,
                                                TR_DURB_ID,
                                                FSA_CROP_DURB_ID
                                ORDER BY 
                                                DV_DR_DATA_EFF_STRT_DT DESC,
                                                THS_DATA_EFF_STRT_DT DESC, 
                                                LAMS_DATA_EFF_STRT_DT DESC,
                                                LAMSR_DATA_EFF_STRT_DT DESC
                            ) AS ROW_NUM_PART
                FROM
                            (
                                SELECT      FARM_SRGT_ID,
                                            FARM_DURB_ID,
                                            ADM_FSA_ST_CNTY_SRGT_ID,
                                            ADM_FSA_ST_CNTY_DURB_ID,
                                            PGM_YR,
                                            TR_SRGT_ID,
                                            TR_DURB_ID,
                                            FSA_CROP_SRGT_ID,
                                            FSA_CROP_DURB_ID,
                                            LOC_FSA_ST_CNTY_SRGT_ID,
                                            LOC_FSA_ST_CNTY_DURB_ID,
                                            CONG_DIST_SRGT_ID,
                                            CONG_DIST_DURB_ID,
                                            CROP_TR_YR_DCP_CRE_DT,
                                            CROP_TR_YR_DCP_LAST_CHG_DT,
                                            CROP_TR_YR_LAST_CHG_USER_NM,
                                            SRC_DATA_STAT_CD,
                                            CRP_RDN_ACRG,
                                            CRP_REL_ACRG,
                                            DCP_CROP_BASE_ACRG,
                                            FAV_RDN_ACRG,
                                            CCP_PYMT_YLD,
                                            CRP_PYMT_YLD,
                                            DIR_PYMT_YLD,
                                            FAV_DIR_PYMT_YLD,
                                            FAV_CCP_PYMT_YLD,
                                            DATA_EFF_STRT_DT,
                                            DV_DR_DATA_EFF_STRT_DT,
                                            THS_DATA_EFF_STRT_DT, 
                                            LAMS_DATA_EFF_STRT_DT,
                                            LAMSR_DATA_EFF_STRT_DT,
                                            CTR_RDN_ACRG
                                FROM        (
                                                SELECT      DISTINCT COALESCE(FR_DIM.FARM_SRGT_ID,-3) AS FARM_SRGT_ID
                                                            ,FR_DIM.FARM_DURB_ID AS FARM_DURB_ID
                                                            ,COALESCE(FS_DIM.FSA_ST_CNTY_SRGT_ID, -3) AS ADM_FSA_ST_CNTY_SRGT_ID
                                                            ,COALESCE(FS_DIM.FSA_ST_CNTY_DURB_ID, -3) AS ADM_FSA_ST_CNTY_DURB_ID
                                                            ,DV_DR.PGM_YR AS PGM_YR
                                                            ,COALESCE(T_DIM.TR_SRGT_ID, -3) AS TR_SRGT_ID
                                                            ,T_DIM.TR_DURB_ID AS TR_DURB_ID
                                                            ,COALESCE(FCT_DIM.FSA_CROP_SRGT_ID, -3) AS FSA_CROP_SRGT_ID
                                                            ,FCT_DIM.FSA_CROP_DURB_ID  AS FSA_CROP_DURB_ID
                                                            ,COALESCE(FS_DIM1.FSA_ST_CNTY_SRGT_ID, -3) AS LOC_FSA_ST_CNTY_SRGT_ID
                                                            ,COALESCE(FS_DIM1.FSA_ST_CNTY_DURB_ID, -3) AS LOC_FSA_ST_CNTY_DURB_ID
                                                            ,COALESCE(CD_DIM.CONG_DIST_SRGT_ID, -3) AS CONG_DIST_SRGT_ID
                                                            ,COALESCE(CD_DIM.CONG_DIST_DURB_ID, -3) AS CONG_DIST_DURB_ID
                                                            ,DV_DR.SRC_CRE_DT AS CROP_TR_YR_DCP_CRE_DT
                                                            ,DV_DR.SRC_LAST_CHG_DT AS CROP_TR_YR_DCP_LAST_CHG_DT
                                                            ,DV_DR.SRC_LAST_CHG_USER_NM AS CROP_TR_YR_LAST_CHG_USER_NM
                                                            ,DV_DR.DATA_STAT_CD AS SRC_DATA_STAT_CD
                                                            ,DV_DR.CRP_RDN_ACRG AS CRP_RDN_ACRG
                                                            ,DV_DR.CRP_REL_ACRG AS CRP_REL_ACRG
                                                            ,DV_DR.DCP_CROP_BASE_ACRG AS DCP_CROP_BASE_ACRG
                                                            ,DV_DR.FAV_RDN_ACRG AS FAV_RDN_ACRG
                                                            ,DV_DR.CCP_PYMT_YLD AS CCP_PYMT_YLD
                                                            ,DV_DR.CRP_PYMT_YLD AS CRP_PYMT_YLD
                                                            ,DV_DR.DIR_PYMT_YLD AS DIR_PYMT_YLD
                                                            ,DV_DR.FAV_DIR_PYMT_YLD AS FAV_DIR_PYMT_YLD
                                                            ,DV_DR.FAV_CCP_PYMT_YLD AS FAV_CCP_PYMT_YLD
                                                            ,GREATEST
                                                            (
                                                                COALESCE(DV_DR.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) ,
                                                                COALESCE(THS.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) ,
                                                                COALESCE(LAMS.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) ,
                                                                COALESCE(LAMSR.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) 
                                                            ) DATA_EFF_STRT_DT,
                                                            DV_DR.DATA_EFF_STRT_DT    DV_DR_DATA_EFF_STRT_DT,
                                                            THS.DATA_EFF_STRT_DT      THS_DATA_EFF_STRT_DT, 
                                                            LAMS.DATA_EFF_STRT_DT     LAMS_DATA_EFF_STRT_DT,
                                                            LAMSR.DATA_EFF_STRT_DT    LAMSR_DATA_EFF_STRT_DT,
                                                            DM_0.CTR_RDN_ACRG
                                                FROM       
                                             ( select * from EDV.CROP_TR_YR_DCP_LS  WHERE  CAST(COALESCE(DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) AS DATE) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
                                            And  (CROP_TR_DCP_L_ID, CROP_TR_YR_DCP_ID) IN ( SELECT      INCR_DR_ID, CROP_TR_YR_DCP_ID  FROM        TAB_INCR_DR_ID) 
                                            AND     PGM_YR IS NOT NULL  
                                          ) DV_DR  
                                                            LEFT JOIN EDV.CROP_TR_DCP_L CTDL ON (DV_DR.CROP_TR_DCP_L_ID = CTDL.CROP_TR_DCP_L_ID)
                                                            LEFT JOIN EDV.TR_H TH ON (COALESCE(CTDL.TR_H_ID,'6de912369edfb89b50859d8f305e4f72') = TH.TR_H_ID)
                                                            LEFT JOIN EDV.TR_HS THS ON (TH.TR_H_ID = COALESCE(THS.TR_H_ID,'6de912369edfb89b50859d8f305e4f72'))
                                                            LEFT JOIN EDV.LOC_AREA_MRT_SRC_RS LAMS ON
                                                            (
                                                                TH.ST_FSA_CD = COALESCE(LAMS.CTRY_DIV_MRT_CD, '--')
                                                                AND
                                                                TH.CNTY_FSA_CD = COALESCE(LAMS.LOC_AREA_MRT_CD, '--')
                                                                AND
                                                                TRIM(LAMS.LOC_AREA_MRT_CD_SRC_ACRO) = 'FSA'
                                                            )
                                                            LEFT JOIN EDV.LOC_AREA_RH LAH ON 
                                                            (
                                                                COALESCE(LAMS.LOC_AREA_CAT_NM, 'NULL IN SOURCE') = LAH.LOC_AREA_CAT_NM
                                                                AND
                                                                COALESCE(LAMS.LOC_AREA_NM, 'NULL IN SOURCE') = LAH.LOC_AREA_NM
                                                                AND
                                                                COALESCE(LAMS.CTRY_DIV_NM, 'NULL IN SOURCE') = LAH.CTRY_DIV_NM
                                                                AND
                                                                LAMS.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'
                                                            )
                                                            LEFT JOIN CMN_DIM_DM_STG.FSA_ST_CNTY_DIM FS_DIM ON
                                                            (
                                                                LAH.DURB_ID = COALESCE(FS_DIM.FSA_ST_CNTY_DURB_ID,-1)
                                                                AND
                                                                FS_DIM.CUR_RCD_IND = 1
                                                            )
                                                            LEFT JOIN CMN_DIM_DM_STG.CONG_DIST_DIM CD_DIM ON
                                                            (
                                                                THS.LOC_ST_FSA_CD = COALESCE(CD_DIM.ST_FSA_CD, '--')
                                                                AND
                                                                THS.CONG_DIST_CD = COALESCE(CD_DIM.CONG_DIST_CD, '--')
                                                                AND
                                                                CD_DIM.CUR_RCD_IND = 1
                                                            )
                                                            LEFT JOIN EDV.FARM_H FH ON ( COALESCE(CTDL.FARM_H_ID,'baf6dd71fe45fe2f5c1c0e6724d514fd') = FH.FARM_H_ID )
                                                            LEFT JOIN CMN_DIM_DM_STG.FARM_DIM FR_DIM ON 
                                                            (
                                                                FH.DURB_ID = COALESCE(FR_DIM.FARM_DURB_ID,-1)
                                                                AND
                                                                FR_DIM.CUR_RCD_IND = 1
                                                            )
                                                            LEFT JOIN EBV.FSA_CROP_TYPE_RH FCTH ON 
                                                            (
                                                                COALESCE(CTDL.FSA_CROP_CD,'--') = FCTH.FSA_CROP_CD
                                                                AND 
                                                                COALESCE(CTDL.FSA_CROP_TYPE_CD,'--') = FCTH.FSA_CROP_TYPE_CD
                                                                AND 
                                                                DV_DR.PGM_YR = FCTH.PGM_YR
                                                            )
                                                            LEFT JOIN CMN_DIM_DM_STG.FSA_CROP_TYPE_DIM FCT_DIM ON 
                                                            (
                                                                FCTH.DURB_ID = COALESCE(FCT_DIM.FSA_CROP_DURB_ID,-1)
                                                                AND 
                                                                FCT_DIM.CUR_RCD_IND = 1
                                                            )
                                                            LEFT JOIN EDV.LOC_AREA_MRT_SRC_RS LAMSR ON 
                                                            (
                                                                THS.LOC_ST_FSA_CD = LAMSR.CTRY_DIV_MRT_CD
                                                                AND 
                                                                THS.LOC_CNTY_FSA_CD = LAMSR.LOC_AREA_MRT_CD
                                                                AND 
                                                                LAMSR.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'
                                                            )
                                                            LEFT JOIN EDV.LOC_AREA_RH LARH ON 
                                                            (
                                                                COALESCE(LAMSR.LOC_AREA_CAT_NM, 'NULL IN SOURCE') = LARH.LOC_AREA_CAT_NM
                                                                AND 
                                                                COALESCE(LAMSR.LOC_AREA_NM, 'NULL IN SOURCE') = LARH.LOC_AREA_NM
                                                                AND 
                                                                COALESCE(LAMSR.CTRY_DIV_NM, 'NULL IN SOURCE') = LARH.CTRY_DIV_NM
                                                                AND 
                                                                LAMSR.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'
                                                            )
                                                            LEFT JOIN CMN_DIM_DM_STG.FSA_ST_CNTY_DIM FS_DIM1 ON 
                                                            (
                                                                LARH.DURB_ID = COALESCE(FS_DIM1.FSA_ST_CNTY_DURB_ID,-1)
                                                                AND 
                                                                FS_DIM1.CUR_RCD_IND = 1
                                                            )
                                                            LEFT JOIN CMN_DIM_DM_STG.TR_DIM T_DIM ON 
                                                            (
                                                                TH.DURB_ID = COALESCE(T_DIM.TR_DURB_ID,-1)
                                                                AND 
                                                                T_DIM.CUR_RCD_IND = 1
                                                            )
                                                            LEFT JOIN
                                                            (
                                                                SELECT      DISTINCT CRP_505.FARM_DURB_ID, 
                                                                            CRP_505.PGM_YR,
                                                                            CRP_505.TR_DURB_ID,
                                                                            CRP_505.FSA_CROP_DURB_ID,
                                                                            SUM( CRP_505.RDN_ACRG ) AS CTR_RDN_ACRG
                                                                FROM        (            
                                                                                SELECT      DISTINCT FARM_H.DURB_ID AS FARM_DURB_ID, 
                                                                                            CROP_TR_YR_DCP_LS.PGM_YR,
                                                                                            CROP_TR_CTR_LS.RDN_ACRG_STRT_YR, 
                                                                                            TR_H.DURB_ID AS TR_DURB_ID,
                                                                                            FSA_CROP_TYPE_RH.DURB_ID AS FSA_CROP_DURB_ID,
                                                                                            COALESCE( CROP_TR_CTR_LS.RDN_ACRG, 0 ) AS RDN_ACRG,
                                                                                            -- To accommodate same acreage value from multiple contracts for a given crop-tract combination within a program year, REL-2986
                                                                                            CROP_TR_CTR_LS.CROP_TR_CTR_L_ID,
                                                                                            CROP_TR_CTR_LS.CROP_TR_CTR_ID
                                                                                FROM        EDV.CROP_TR_YR_DCP_LS
                                                                                            LEFT JOIN EDV.CROP_TR_DCP_L ON CROP_TR_YR_DCP_LS.CROP_TR_DCP_L_ID = CROP_TR_DCP_L.CROP_TR_DCP_L_ID
                                                                                            LEFT JOIN EDV.TR_H ON CROP_TR_DCP_L.TR_H_ID = TR_H.TR_H_ID
                                                                                            LEFT JOIN EDV.CROP_TR_CTR_L ON TR_H.TR_H_ID = CROP_TR_CTR_L.TR_H_ID
                                                                                    LEFT JOIN ( 
                                                                                    Select 
                CROP_TR_CTR_L_ID,CROP_TR_CTR_ID,PGM_YR,DATA_EFF_END_DT,RDN_ACRG_STRT_YR,RDN_ACRG,DATA_STAT_CD,DATA_EFF_STRT_DT from (
               select CROP_TR_CTR_L_ID,CROP_TR_CTR_ID,PGM_YR,DATA_EFF_END_DT,RDN_ACRG_STRT_YR,RDN_ACRG,DATA_STAT_CD,DATA_EFF_STRT_DT
                    ,RANK() over ( PARTITION BY  CROP_TR_CTR_L_ID , PGM_YR    ORDER BY       DATA_EFF_STRT_DT DESC,        
                    CASE DATA_STAT_CD
                     WHEN 'A' THEN 1
                     WHEN 'I' THEN 2
                     WHEN 'D' THEN 3
                    ELSE 4
                     END ASC,
                    SRC_CRE_DT DESC NULLS LAST
                )  as Rank_Part
                  from EDV.CROP_TR_CTR_LS where  CAST(DATA_EFF_END_DT AS DATE) = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD')          
                ) sub_1 where  RANK_PART =1 
              ) CROP_TR_CTR_LS ON (CROP_TR_CTR_L.CROP_TR_CTR_L_ID = CROP_TR_CTR_LS.CROP_TR_CTR_L_ID
                                                                                             AND CAST(CROP_TR_CTR_LS.DATA_EFF_END_DT AS DATE) = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD'))
                                                                                            LEFT JOIN EDV.FARM_H ON CROP_TR_DCP_L.FARM_H_ID = FARM_H.FARM_H_ID
                                                                                            LEFT JOIN EBV.FSA_CROP_TYPE_RH ON CROP_TR_DCP_L.FSA_CROP_CD = FSA_CROP_TYPE_RH.FSA_CROP_CD AND CROP_TR_DCP_L.FSA_CROP_TYPE_CD = FSA_CROP_TYPE_RH.FSA_CROP_TYPE_CD
                                                                                WHERE       CROP_TR_CTR_LS.RDN_ACRG_STRT_YR <= CROP_TR_YR_DCP_LS.PGM_YR
                                                                                    AND     COALESCE(CROP_TR_CTR_L.FSA_CROP_CD, '--') = COALESCE(CROP_TR_DCP_L.FSA_CROP_CD, '--' )
                                                                                    AND     COALESCE(CROP_TR_CTR_L.FSA_CROP_TYPE_CD, '--') = COALESCE(CROP_TR_DCP_L.FSA_CROP_TYPE_CD, '--')
                                                                                    AND     CROP_TR_YR_DCP_LS.PGM_YR = FSA_CROP_TYPE_RH.PGM_YR
                                                                               
                                                                                     AND     CROP_TR_YR_DCP_LS.PGM_YR = CROP_TR_CTR_LS.PGM_YR
                                                                                    AND     CROP_TR_CTR_LS.DATA_STAT_CD = 'A'
                                                                            ) CRP_505
                                                                GROUP BY    CRP_505.FARM_DURB_ID, 
                                                                            CRP_505.PGM_YR,
                                                                            CRP_505.TR_DURB_ID,
                                                                            CRP_505.FSA_CROP_DURB_ID
                                                            ) DM_0 ON 
                                                            (
                                                                DM_0.FARM_DURB_ID = FR_DIM.FARM_DURB_ID
                                                                AND DM_0.PGM_YR = DV_DR.PGM_YR
                                                                AND DM_0.TR_DURB_ID = TH.DURB_ID
                                                                AND DM_0.FSA_CROP_DURB_ID = FCTH.DURB_ID
                                                            )
                                                WHERE       (DV_DR.CROP_TR_DCP_L_ID, DV_DR.CROP_TR_YR_DCP_ID) IN 
                                                            (
                                                                SELECT      INCR_DR_ID, CROP_TR_YR_DCP_ID 
                                                                FROM        TAB_INCR_DR_ID
                                                            ) 
                                                    AND     FR_DIM.FARM_DURB_ID IS NOT NULL
                                                    AND     T_DIM.TR_DURB_ID IS NOT NULL
                                                    AND     FCT_DIM.FSA_CROP_DURB_ID IS NOT NULL
                                            ) sub_1
                            ) DM_1
                WHERE         CAST(COALESCE(THS_DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) AS DATE) <=  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
                    AND     CAST(COALESCE(LAMS_DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) AS DATE) <=  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
                    AND     CAST(COALESCE(LAMSR_DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) AS DATE ) <=  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
            ) DM
WHERE       DM.ROW_NUM_PART = 1
)
SELECT (current_date) AS cre_dt,
	   (current_date) AS last_chg_dt, 
       'A' as data_stat_cd, 
	   farm_srgt_id, 
	   farm_durb_id, 
	   adm_fsa_st_cnty_srgt_id, 
	   adm_fsa_st_cnty_durb_id, 
	   pgm_yr, 
	   tr_srgt_id, 
	   tr_durb_id, 
	   fsa_crop_srgt_id, 
	   fsa_crop_durb_id, 
	   loc_fsa_st_cnty_srgt_id, 
	   loc_fsa_st_cnty_durb_id, 
	   cong_dist_srgt_id, 
	   cong_dist_durb_id, 
	   crop_tr_yr_dcp_cre_dt, 
	   crop_tr_yr_dcp_last_chg_dt, 
	   crop_tr_yr_last_chg_user_nm, 
	   src_data_stat_cd, 
	   crp_rdn_acrg, 
	   crp_rel_acrg, 
	   dcp_crop_base_acrg, 
	   fav_rdn_acrg, 
	   ccp_pymt_yld, 
	   crp_pymt_yld, 
	   dir_pymt_yld, 
	   fav_dir_pymt_yld, 
	   fav_ccp_pymt_yld, 
	   ctr_rdn_acrg
FROM INSERT_DATA TAB_INCR