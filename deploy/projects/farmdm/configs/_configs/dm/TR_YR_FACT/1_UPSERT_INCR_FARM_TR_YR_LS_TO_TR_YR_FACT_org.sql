with upsert_data as(
SELECT      DM.PGM_YR 
            ,DM.FARM_SRGT_ID 
            ,DM.FARM_DURB_ID 
            ,DM.ADM_FSA_ST_CNTY_SRGT_ID 
            ,DM.ADM_FSA_ST_CNTY_DURB_ID 
            ,DM.TR_SRGT_ID 
            ,DM.TR_DURB_ID 
            ,DM.LOC_FSA_ST_CNTY_SRGT_ID  
            ,DM.LOC_FSA_ST_CNTY_DURB_ID 
            ,DM.CONG_DIST_SRGT_ID 
            ,DM.CONG_DIST_DURB_ID 
            ,DM.HEL_TR_SRGT_ID 
            ,DM.HEL_TR_DURB_ID 
            ,DM.WL_PRES_SRGT_ID 
            ,DM.WL_PRES_DURB_ID 
            ,DM.TR_YR_CRE_DT 
            ,DM.TR_YR_LAST_CHG_DT 
            ,DM.TR_YR_LAST_CHG_USER_NM 
            ,DM.TR_YR_DCP_CRE_DT 
            ,DM.TR_YR_DCP_LAST_CHG_DT 
            ,DM.TR_YR_DCP_LAST_CHG_USER_NM 
            ,DM.SRC_DATA_STAT_CD 
            ,DM.FAV_WR_HIST_IND 
            ,DM.WL_CVRT_BEF_11281990_IND 
            ,DM.WL_CVRT_AFT_11281990_IND 
            ,DM.PLNT_ON_CVRT_WL_IND                      
            ,DM.FMLD_ACRG 
            ,DM.CPLD_ACRG 
            ,DM.CRP_ACRG 
            ,DM.MPL_ACRG 
            ,DM.WBP_ACRG 
            ,DM.WRP_TR_ACRG 
            ,DM.GRP_CPLD_ACRG 
            ,DM.ST_CNSV_ACRG 
            ,DM.OT_CNSV_ACRG 
            ,DM.SGRCN_ACRG 
            ,DM.NAP_CROP_ACRG 
            ,DM.NTV_SOD_BRK_OUT_ACRG 
            ,DM.DCP_DBL_CROP_ACRG 
            ,DM.DCP_CPLD_ACRG 
            ,DM.DCP_AFT_RDN_ACRG 
            ,DM.DCP_AG_RLT_ACTV_ACRES 
            ,DM.EFF_DCP_CPLD_ACRES 
            ,DM.ARC_PLC_ELG_DTER_DURB_ID
            ,DM.DATA_EFF_STRT_DT
FROM        ( 
                SELECT  DM_1.PGM_YR 
                            ,DM_1.FARM_SRGT_ID 
                            ,DM_1.FARM_DURB_ID 
                            ,DM_1.ADM_FSA_ST_CNTY_SRGT_ID 
                            ,DM_1.ADM_FSA_ST_CNTY_DURB_ID 
                            ,DM_1.TR_SRGT_ID 
                            ,DM_1.TR_DURB_ID 
                            ,DM_1.LOC_FSA_ST_CNTY_SRGT_ID  
                            ,DM_1.LOC_FSA_ST_CNTY_DURB_ID 
                            ,DM_1.CONG_DIST_SRGT_ID 
                            ,DM_1.CONG_DIST_DURB_ID 
                            ,DM_1.HEL_TR_SRGT_ID 
                            ,DM_1.HEL_TR_DURB_ID 
                            ,DM_1.WL_PRES_SRGT_ID 
                            ,DM_1.WL_PRES_DURB_ID 
                            ,DM_1.TR_YR_CRE_DT 
                            ,DM_1.TR_YR_LAST_CHG_DT 
                            ,DM_1.TR_YR_LAST_CHG_USER_NM 
                            ,DM_1.TR_YR_DCP_CRE_DT 
                            ,DM_1.TR_YR_DCP_LAST_CHG_DT 
                            ,DM_1.TR_YR_DCP_LAST_CHG_USER_NM 
                            ,DM_1.SRC_DATA_STAT_CD 
                            ,DM_1.FAV_WR_HIST_IND 
                            ,DM_1.WL_CVRT_BEF_11281990_IND 
                            ,DM_1.WL_CVRT_AFT_11281990_IND 
                            ,DM_1.PLNT_ON_CVRT_WL_IND                      
                            ,DM_1.FMLD_ACRG 
                            ,DM_1.CPLD_ACRG 
                            ,DM_1.CRP_ACRG 
                            ,DM_1.MPL_ACRG 
                            ,DM_1.WBP_ACRG 
                            ,DM_1.WRP_TR_ACRG 
                            ,DM_1.GRP_CPLD_ACRG 
                            ,DM_1.ST_CNSV_ACRG 
                            ,DM_1.OT_CNSV_ACRG 
                            ,DM_1.SGRCN_ACRG 
                            ,DM_1.NAP_CROP_ACRG 
                            ,DM_1.NTV_SOD_BRK_OUT_ACRG 
                            ,DM_1.DCP_DBL_CROP_ACRG 
                            ,DM_1.DCP_CPLD_ACRG 
                            ,DM_1.DCP_AFT_RDN_ACRG 
                            ,DM_1.DCP_AG_RLT_ACTV_ACRES 
                            ,DM_1.EFF_DCP_CPLD_ACRES 
                            
                            ,DM_1.ARC_PLC_ELG_DTER_DURB_ID 
                            ,DM_1.DATA_EFF_STRT_DT 
                            ,DM_1.FS_DATA_EFF_STRT_DT 
                            ,DM_1.LAS_ADM_DATA_EFF_STRT_DT 
                            ,DM_1.TRS_DATA_EFF_STRT_DT 
                            ,DM_1.LAS_LOC_DATA_EFF_STRT_DT 
                            ,DM_1.TR_DATA_EFF_STRT_DT 
                            ,DM_1.HS_DATA_EFF_STRT_DT 
                            ,DM_1.WLVS_DATA_EFF_STRT_DT 
                            ,DM_1.WLS_DATA_EFF_STRT_DT 
                            
                            ,DM_1.APEDRS_DATA_EFF_STRT_DT
                            ,DM_1.FYHS_DATA_EFF_STRT_DT 
                           
                            ,ROW_NUMBER() OVER  
                            ( 
                                PARTITION BY    PGM_YR, FARM_DURB_ID, TR_DURB_ID 
                                ORDER BY        FS_DATA_EFF_STRT_DT DESC, LAS_ADM_DATA_EFF_STRT_DT DESC, TRS_DATA_EFF_STRT_DT DESC, LAS_LOC_DATA_EFF_STRT_DT DESC, 
                                                TR_DATA_EFF_STRT_DT DESC, HS_DATA_EFF_STRT_DT DESC, WLVS_DATA_EFF_STRT_DT DESC, WLS_DATA_EFF_STRT_DT DESC, APEDRS_DATA_EFF_STRT_DT DESC, FYHS_DATA_EFF_STRT_DT DESC
                            )  AS ROW_NUM_PART 
                FROM        ( 
                                SELECT      PGM_YR 
                                            ,FARM_SRGT_ID 
                                            ,FARM_DURB_ID 
                                            ,ADM_FSA_ST_CNTY_SRGT_ID 
                                            ,ADM_FSA_ST_CNTY_DURB_ID 
                                            ,TR_SRGT_ID 
                                            ,TR_DURB_ID 
                                            ,LOC_FSA_ST_CNTY_SRGT_ID  
                                            ,LOC_FSA_ST_CNTY_DURB_ID 
                                            ,CONG_DIST_SRGT_ID 
                                            ,CONG_DIST_DURB_ID 
                                            ,HEL_TR_SRGT_ID 
                                            ,HEL_TR_DURB_ID 
                                            ,WL_PRES_SRGT_ID 
                                            ,WL_PRES_DURB_ID 
                                            ,TR_YR_CRE_DT 
                                            ,TR_YR_LAST_CHG_DT 
                                            ,TR_YR_LAST_CHG_USER_NM 
                                            ,TR_YR_DCP_CRE_DT 
                                            ,TR_YR_DCP_LAST_CHG_DT 
                                            ,TR_YR_DCP_LAST_CHG_USER_NM 
                                            ,SRC_DATA_STAT_CD 
                                            ,FAV_WR_HIST_IND 
                                            ,WL_CVRT_BEF_11281990_IND 
                                            ,WL_CVRT_AFT_11281990_IND 
                                            ,PLNT_ON_CVRT_WL_IND                      
                                            ,FMLD_ACRG 
                                            ,CPLD_ACRG 
                                            ,CRP_ACRG 
                                            ,MPL_ACRG 
                                            ,WBP_ACRG 
                                            ,WRP_TR_ACRG 
                                            ,GRP_CPLD_ACRG 
                                            ,ST_CNSV_ACRG 
                                            ,OT_CNSV_ACRG 
                                            ,SGRCN_ACRG 
                                            ,NAP_CROP_ACRG 
                                            ,NTV_SOD_BRK_OUT_ACRG 
                                            ,DCP_DBL_CROP_ACRG 
                                            ,DCP_CPLD_ACRG 
                                            ,DCP_AFT_RDN_ACRG 
                                            ,DCP_AG_RLT_ACTV_ACRES 
                                            ,EFF_DCP_CPLD_ACRES 
                                            
                                            ,ARC_PLC_ELG_DTER_DURB_ID 
                                            ,DATA_EFF_STRT_DT  
                                            ,FS_DATA_EFF_STRT_DT 
                                            ,LAS_ADM_DATA_EFF_STRT_DT 
                                            ,TRS_DATA_EFF_STRT_DT 
                                            ,LAS_LOC_DATA_EFF_STRT_DT 
                                            ,TR_DATA_EFF_STRT_DT 
                                            ,HS_DATA_EFF_STRT_DT 
                                            ,WLVS_DATA_EFF_STRT_DT 
                                            ,WLS_DATA_EFF_STRT_DT 
                                            ,FYHS_DATA_EFF_STRT_DT 
                                            
                                            ,APEDRS_DATA_EFF_STRT_DT 
                                FROM        ( 
                                                SELECT      * 
                                                FROM        ( 
                                                                SELECT       DISTINCT  
                                                                            ROW_NUMBER () OVER  
                                                                            ( 
                                                                                PARTITION BY    FS.FARM_TR_L_ID, FS.PGM_YR 
                                                                                ORDER BY        ( 
                                                                                                    CASE
                                                                                                        WHEN FS.DATA_STAT_CD = 'A' THEN 1 
                                                                                                        ELSE 99 
                                                                                                    END 
                                                                                                ) ASC, FS.DATA_EFF_END_DT DESC NULLS LAST, FS.SRC_LAST_CHG_DT DESC NULLS LAST, FS.TR_YR_ID DESC
                                                                                                , TR.DATA_EFF_END_DT DESC NULLS LAST, TR.SRC_LAST_CHG_DT DESC NULLS LAST
                                                                                                , LAS_ADM.DATA_EFF_END_DT DESC NULLS LAST, LAS_ADM.SRC_LAST_CHG_DT DESC NULLS LAST
                                                                                                , TRS.DATA_EFF_END_DT DESC NULLS LAST, TRS.SRC_LAST_CHG_DT DESC NULLS LAST
                                                                                                , LAS_LOC.DATA_EFF_END_DT DESC NULLS LAST, LAS_LOC.SRC_LAST_CHG_DT DESC NULLS LAST
                                                                                                , HS.DATA_EFF_END_DT DESC NULLS LAST, HS.SRC_LAST_CHG_DT DESC NULLS LAST
                                                                                                , WLVS.DATA_EFF_END_DT DESC NULLS LAST, WLVS.SRC_LAST_CHG_DT DESC NULLS LAST
                                                                                                , WLS.DATA_EFF_END_DT DESC NULLS LAST, WLS.SRC_LAST_CHG_DT DESC NULLS LAST
                                                                                                , FYHS.DATA_EFF_END_DT DESC NULLS LAST, FYHS.SRC_LAST_CHG_DT DESC NULLS LAST
                                                                                                , APEDRS.DATA_EFF_END_DT DESC NULLS LAST, APEDRS.SRC_LAST_CHG_DT DESC NULLS LAST
                                                                            ) AS SRC_RANK,  
                                                                            FS.SRC_HEL_TR_CD, FS.SRC_WL_PRES_CD, FS.SRC_LAST_CHG_DT, FS.TR_YR_ID, FS.PGM_YR, 
                                                                            coalesce(FD.FARM_SRGT_ID,-3) FARM_SRGT_ID, FD.FARM_DURB_ID, coalesce(FSD_ADM.FSA_ST_CNTY_SRGT_ID,-3) ADM_FSA_ST_CNTY_SRGT_ID, coalesce(FSD_ADM.FSA_ST_CNTY_DURB_ID,-3) ADM_FSA_ST_CNTY_DURB_ID, coalesce(TD.TR_SRGT_ID,-3) TR_SRGT_ID, TD.TR_DURB_ID, coalesce(FSD_LOC.FSA_ST_CNTY_SRGT_ID,-3) LOC_FSA_ST_CNTY_SRGT_ID, coalesce(FSD_LOC.FSA_ST_CNTY_DURB_ID,-3) LOC_FSA_ST_CNTY_DURB_ID, 
                                                                            coalesce(CDS.CONG_DIST_SRGT_ID,-3) CONG_DIST_SRGT_ID, coalesce(CDS.CONG_DIST_DURB_ID,-3) CONG_DIST_DURB_ID, coalesce(HD.HEL_TR_SRGT_ID,-3) HEL_TR_SRGT_ID, coalesce(HD.HEL_TR_DURB_ID,-3) HEL_TR_DURB_ID, coalesce(WLD.WL_PRES_SRGT_ID,-3) WL_PRES_SRGT_ID, coalesce(WLD.WL_PRES_DURB_ID,-3) WL_PRES_DURB_ID, FS.SRC_CRE_DT  TR_YR_CRE_DT, 
                                                                            FS.SRC_LAST_CHG_DT  TR_YR_LAST_CHG_DT, FS.SRC_LAST_CHG_USER_NM  TR_YR_LAST_CHG_USER_NM, TR.SRC_CRE_DT  TR_YR_DCP_CRE_DT, TR.SRC_LAST_CHG_DT  TR_YR_DCP_LAST_CHG_DT, TR.SRC_LAST_CHG_USER_NM  TR_YR_DCP_LAST_CHG_USER_NM, FS.DATA_STAT_CD  SRC_DATA_STAT_CD, TR.FAV_WR_HIST_IND, 
                                                                            coalesce 
                                                                            ( 
                                                                                ( 
                                                                                    SELECT      1 
                                                                                    FROM        EDV.TR_YR_WL_VLT_LS WLVS 
                                                                                    WHERE       SRC_WL_VLT_TYPE_CD = '112'  
                                                                                        AND     FS.FARM_TR_L_ID = WLVS.FARM_TR_L_ID 
                                                                                        AND     FS.PGM_YR = WLVS.PGM_YR 
                                                                                        AND     cast(WLVS.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
                                                                                        AND     cast(WLVS.DATA_EFF_STRT_DT as date) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
                                                                                        AND     coalesce(WLVS.DATA_STAT_CD, '') <> 'D' 
                                                                                ) 
                                                                                ,0 
                                                                            )  AS WL_CVRT_BEF_11281990_IND, 
                                                                            coalesce 
                                                                            ( 
                                                                                ( 
                                                                                    SELECT      1 
                                                                                    FROM        EDV.TR_YR_WL_VLT_LS WLVS 
                                                                                    WHERE       SRC_WL_VLT_TYPE_CD = '113' 
                                                                                        AND     FS.FARM_TR_L_ID = WLVS.FARM_TR_L_ID 
                                                                                        AND     FS.PGM_YR = WLVS.PGM_YR  
                                                                                        AND     cast(WLVS.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
                                                                                        AND     cast(WLVS.DATA_EFF_STRT_DT as date) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
                                                                                        AND     coalesce(WLVS.DATA_STAT_CD, '') <> 'D' 
                                                                                ) 
                                                                                ,0 
                                                                            ) AS WL_CVRT_AFT_11281990_IND, 
                                                                            coalesce 
                                                                            ( 
                                                                                ( 
                                                                                    SELECT      1 
                                                                                    FROM        EDV.TR_YR_WL_VLT_LS WLVS 
                                                                                    WHERE       SRC_WL_VLT_TYPE_CD = '116' 
                                                                                        AND     FS.FARM_TR_L_ID = WLVS.FARM_TR_L_ID 
                                                                                        AND     FS.PGM_YR = WLVS.PGM_YR 
                                                                                        AND     cast(WLVS.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
                                                                                        AND     cast(WLVS.DATA_EFF_STRT_DT as date) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
                                                                                        AND     coalesce(WLVS.DATA_STAT_CD, '') <> 'D' 
                                                                                ) 
                                                                                ,0 
                                                                            ) AS PLNT_ON_CVRT_WL_IND,          
                                                                            FS.FMLD_ACRG FMLD_ACRG, FS.CPLD_ACRG  CPLD_ACRG, FS.CRP_ACRG CRP_ACRG, FS.MPL_ACRG MPL_ACRG, FS.WBP_ACRG WBP_ACRG, FS.WRP_TR_ACRG WRP_TR_ACRG, FS.GRP_CPLD_ACRG GRP_CPLD_ACRG, FS.ST_CNSV_ACRG  ST_CNSV_ACRG, 
                                                                            FS.OT_CNSV_ACRG  OT_CNSV_ACRG, FS.SGRCN_ACRG  SGRCN_ACRG, FS.NAP_CROP_ACRG NAP_CROP_ACRG, FS.NTV_SOD_BRK_OUT_ACRG NTV_SOD_BRK_OUT_ACRG, TR.DCP_DBL_CROP_ACRG DCP_DBL_CROP_ACRG, TR.DCP_CPLD_ACRG  DCP_CPLD_ACRG, TR.DCP_AFT_RDN_ACRG DCP_AFT_RDN_ACRG, 
                                                                             
                                                                            (TR.DCP_CPLD_ACRG - FS.CPLD_ACRG) DCP_AG_RLT_ACTV_ACRES, 
                                                                            (TR.DCP_CPLD_ACRG - FS.CRP_ACRG - FS.WBP_ACRG - FS.WRP_TR_ACRG - FS.ST_CNSV_ACRG - FS.OT_CNSV_ACRG - FS.GRP_CPLD_ACRG) EFF_DCP_CPLD_ACRES, 
                                                                             
                                                                            coalesce(APED_DIM.ARC_PLC_ELG_DTER_DURB_ID,-3) ARC_PLC_ELG_DTER_DURB_ID, 
                                                                            GREATEST 
                                                                            ( 
                                                                                coalesce(FS.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) , 
                                                                                coalesce(LAS_ADM.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) , 
                                                                                coalesce(TRS.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) , 
                                                                                coalesce(LAS_LOC.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) , 
                                                                                coalesce(TR.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) , 
                                                                                coalesce(HS.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) , 
                                                                                coalesce(WLVS.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')), 
                                                                                coalesce(WLS.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')),  
                                                                                
                                                                                coalesce(APEDRS.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')),
                                                                                coalesce(FYHS.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD'))
                                                                            ) DATA_EFF_STRT_DT, 
                                                                            FS.DATA_EFF_STRT_DT       FS_DATA_EFF_STRT_DT,  
                                                                            LAS_ADM.DATA_EFF_STRT_DT  LAS_ADM_DATA_EFF_STRT_DT,  
                                                                            TRS.DATA_EFF_STRT_DT      TRS_DATA_EFF_STRT_DT, 
                                                                            LAS_LOC.DATA_EFF_STRT_DT  LAS_LOC_DATA_EFF_STRT_DT, 
                                                                            TR.DATA_EFF_STRT_DT    TR_DATA_EFF_STRT_DT, 
                                                                            HS.DATA_EFF_STRT_DT    HS_DATA_EFF_STRT_DT, 
                                                                            WLVS.DATA_EFF_STRT_DT  WLVS_DATA_EFF_STRT_DT, 
                                                                            WLS.DATA_EFF_STRT_DT   WLS_DATA_EFF_STRT_DT,  
                                                                            APEDRS.DATA_EFF_STRT_DT   APEDRS_DATA_EFF_STRT_DT,  
                                                                            FYHS.DATA_EFF_STRT_DT   FYHS_DATA_EFF_STRT_DT
                                                                FROM      EDV.FARM_TR_YR_LS  FS
                                                                INNER JOIN (SELECT INCR_DR_ID, PGM_YR 
																	FROM (SELECT DISTINCT FARM_TR_L_ID INCR_DR_ID, PGM_YR
																	                   FROM EDV.FARM_TR_YR_LS
																	                  WHERE     cast (DATA_EFF_STRT_DT as date) =
																	                            TO_TIMESTAMP ('{V_CDC_DT}', 'YYYY-MM-DD')
																	                        AND cast (DATA_EFF_END_DT as date) >
																	                            TO_TIMESTAMP ('{V_CDC_DT}', 'YYYY-MM-DD')
																	                 UNION
																	                 SELECT
																	                        DISTINCT
																	                        FARM_TR_L.FARM_TR_L_ID     INCR_DR_ID,
																	                        FARM_TR_YR_LS.PGM_YR
																	                   FROM EDV.TR_HS,
																	                        EDV.TR_H,
																	                        EDV.FARM_TR_L,
																	                        EDV.FARM_TR_YR_LS
																	                  WHERE     FARM_TR_L.FARM_TR_L_ID =
																	                            coalesce (FARM_TR_YR_LS.FARM_TR_L_ID,
																	                                 '6bb61e3b7bce0931da574d19d1d82c88')
																	                        AND coalesce (FARM_TR_L.TR_H_ID,
																	                                 '6de912369edfb89b50859d8f305e4f72') =
																	                            TR_H.TR_H_ID
																	                        AND TR_H.TR_H_ID =
																	                            coalesce (TR_HS.TR_H_ID,
																	                                 '6de912369edfb89b50859d8f305e4f72')
																	                        AND cast (TR_HS.DATA_EFF_STRT_DT as date) =
																	                            TO_TIMESTAMP ('{V_CDC_DT}', 'YYYY-MM-DD')
																	                        AND cast (TR_HS.DATA_EFF_END_DT as date) >
																	                            TO_TIMESTAMP ('{V_CDC_DT}', 'YYYY-MM-DD')
																	                 UNION
																	                                  SELECT
																	                        DISTINCT
																	                        TR_YR_DCP_LS.FARM_TR_L_ID     INCR_DR_ID,
																	                        TR_YR_DCP_LS.PGM_YR
																	                   FROM EDV.TR_YR_DCP_LS, EDV.FARM_TR_YR_LS, EDV.FARM_TR_L
																	                  WHERE     FARM_TR_L.FARM_TR_L_ID =
																	                            coalesce (FARM_TR_YR_LS.FARM_TR_L_ID,
																	                                 '6bb61e3b7bce0931da574d19d1d82c88')
																	                        AND FARM_TR_YR_LS.FARM_TR_L_ID =
																	                            TR_YR_DCP_LS.FARM_TR_L_ID
																	                        AND FARM_TR_YR_LS.PGM_YR = TR_YR_DCP_LS.PGM_YR
																	                        AND (   cast (TR_YR_DCP_LS.DATA_EFF_STRT_DT as date) =
																	                                TO_TIMESTAMP ('{V_CDC_DT}', 'YYYY-MM-DD')
																	                             OR cast (TR_YR_DCP_LS.DATA_EFF_END_DT as date) =
																	                                TO_TIMESTAMP ('{V_CDC_DT}', 'YYYY-MM-DD'))
																	                 UNION
																	                                  SELECT
																	                        DISTINCT
																	                        FARM_TR_YR_LS.FARM_TR_L_ID     INCR_DR_ID,
																	                        FARM_TR_YR_LS.PGM_YR           PGM_YR
																	                   FROM EDV.HEL_TR_RS, EDV.FARM_TR_YR_LS
																	                  WHERE     FARM_TR_YR_LS.SRC_HEL_TR_CD =
																	                            HEL_TR_RS.DMN_VAL_ID
																	                        AND (   cast (HEL_TR_RS.DATA_EFF_STRT_DT as date) =
																	                                TO_TIMESTAMP ('{V_CDC_DT}', 'YYYY-MM-DD')
																	                             OR cast (HEL_TR_RS.DATA_EFF_END_DT as date) =
																	                                TO_TIMESTAMP ('{V_CDC_DT}', 'YYYY-MM-DD'))
																	                 UNION
																	                                 SELECT
																	                        DISTINCT
																	                        TR_YR_WL_VLT_LS.FARM_TR_L_ID     INCR_DR_ID,
																	                        TR_YR_WL_VLT_LS.PGM_YR
																	                   FROM EDV.TR_YR_WL_VLT_LS, EDV.FARM_TR_YR_LS
																	                  WHERE     FARM_TR_YR_LS.FARM_TR_L_ID =
																	                            TR_YR_WL_VLT_LS.FARM_TR_L_ID
																	                        AND FARM_TR_YR_LS.PGM_YR = TR_YR_WL_VLT_LS.PGM_YR
																	                        AND (   cast (TR_YR_WL_VLT_LS.DATA_EFF_STRT_DT as date) =
																	                                TO_TIMESTAMP ('{V_CDC_DT}', 'YYYY-MM-DD')
																	                             OR cast (TR_YR_WL_VLT_LS.DATA_EFF_END_DT as date) =
																	                                TO_TIMESTAMP ('{V_CDC_DT}', 'YYYY-MM-DD'))
																	                 UNION
																	                                  SELECT
																	                        DISTINCT
																	                        FARM_TR_YR_LS.FARM_TR_L_ID     INCR_DR_ID,
																	                        FARM_TR_YR_LS.PGM_YR
																	                   FROM EDV.WL_PRES_RS, EDV.FARM_TR_YR_LS
																	                  WHERE     FARM_TR_YR_LS.SRC_WL_PRES_CD =
																	                            WL_PRES_RS.DMN_VAL_ID
																	                        AND (   cast (WL_PRES_RS.DATA_EFF_STRT_DT as date) =
																	                                TO_TIMESTAMP ('{V_CDC_DT}', 'YYYY-MM-DD')
																	                             OR cast (WL_PRES_RS.DATA_EFF_END_DT as date) =
																	                                TO_TIMESTAMP ('{V_CDC_DT}', 'YYYY-MM-DD'))
																	                 UNION
																	                                 SELECT
																	                        DISTINCT
																	                        FARM_TR_YR_LS.FARM_TR_L_ID     INCR_DR_ID,
																	                        FARM_TR_YR_LS.PGM_YR
																	                   FROM EDV.LOC_AREA_MRT_SRC_RS,
																	                        EDV.FARM_H,
																	                        EDV.FARM_TR_L,
																	                        EDV.FARM_TR_YR_LS
																	                  WHERE     FARM_TR_L.FARM_TR_L_ID =
																	                            coalesce (FARM_TR_YR_LS.FARM_TR_L_ID,
																	                                 '6bb61e3b7bce0931da574d19d1d82c88')
																	                        AND coalesce (FARM_TR_L.FARM_H_ID,
																	                                 'baf6dd71fe45fe2f5c1c0e6724d514fd') =
																	                            FARM_H.FARM_H_ID
																	                        AND coalesce (LOC_AREA_MRT_SRC_RS.CTRY_DIV_MRT_CD, 'EDW_DEFAULT') =  coalesce(FARM_H.ST_FSA_CD,'EDW_DEFAULT')
																	                        AND coalesce (LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD, 'EDW_DEFAULT') =  coalesce(FARM_H.CNTY_FSA_CD,'EDW_DEFAULT')
																	                        AND LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD_SRC_ACRO =
																	                            'FSA'
																	                        AND (   cast (LOC_AREA_MRT_SRC_RS.DATA_EFF_STRT_DT as date) =
																	                                TO_TIMESTAMP ('{V_CDC_DT}', 'YYYY-MM-DD')
																	                             OR cast (LOC_AREA_MRT_SRC_RS.DATA_EFF_END_DT as date) =
																	                                TO_TIMESTAMP ('{V_CDC_DT}', 'YYYY-MM-DD'))
																	                 UNION
																	                                  SELECT
																	                        DISTINCT
																	                        FARM_TR_YR_LS.FARM_TR_L_ID     INCR_DR_ID,
																	                        FARM_TR_YR_LS.PGM_YR           PGM_YR
																	                   FROM EDV.LOC_AREA_MRT_SRC_RS,
																	                        EDV.TR_HS,
																	                        EDV.TR_H,
																	                        EDV.FARM_TR_L,
																	                        EDV.FARM_TR_YR_LS
																	                  WHERE     FARM_TR_L.FARM_TR_L_ID =
																	                            coalesce (FARM_TR_YR_LS.FARM_TR_L_ID,
																	                                 '6bb61e3b7bce0931da574d19d1d82c88')
																	                        AND coalesce (FARM_TR_L.TR_H_ID,
																	                                 '6de912369edfb89b50859d8f305e4f72') =
																	                            TR_H.TR_H_ID
																	                        AND TR_H.TR_H_ID =
																	                            coalesce (TR_HS.TR_H_ID,
																	                                 '6de912369edfb89b50859d8f305e4f72')
																	                        AND TR_HS.LOC_ST_FSA_CD =
																	                            LOC_AREA_MRT_SRC_RS.CTRY_DIV_MRT_CD
																	                        AND TR_HS.LOC_CNTY_FSA_CD =
																	                            LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD
																	                        AND LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD_SRC_ACRO =
																	                            'FSA'
																	                        AND (   cast (LOC_AREA_MRT_SRC_RS.DATA_EFF_STRT_DT as date) =
																	                                TO_TIMESTAMP ('{V_CDC_DT}', 'YYYY-MM-DD')
																	                             OR cast (LOC_AREA_MRT_SRC_RS.DATA_EFF_END_DT as date) =
																	                                TO_TIMESTAMP ('{V_CDC_DT}', 'YYYY-MM-DD'))
																	                 UNION
																	                 SELECT
																	                        DISTINCT
																	                        FARM_TR_YR_LS.FARM_TR_L_ID     INCR_DR_ID,
																	                        FARM_TR_YR_LS.PGM_YR
																	                   FROM EDV.FARM_TR_YR_LS,
																	                        EDV.FARM_YR_HS,
																	                        EDV.ARC_PLC_ELG_DTER_RS
																	                  WHERE     ARC_PLC_ELG_DTER_RS.ARC_PLC_ELG_DTER_CD =
																	                            coalesce (FARM_YR_HS.ARC_PLC_ELG_DTER_CD,
																	                                 '[NULL IN SOURCE]')
																	                        AND FARM_YR_HS.FARM_YR_ID =
																	                            coalesce (FARM_TR_YR_LS.FARM_YR_ID, -1)
																	                        AND (   (   cast (
																	                                        ARC_PLC_ELG_DTER_RS.DATA_EFF_STRT_DT as date) =
																	                                    TO_TIMESTAMP ('{V_CDC_DT}', 'YYYY-MM-DD')
																	                                 OR cast (
																	                                        ARC_PLC_ELG_DTER_RS.DATA_EFF_END_DT as date) =
																	                                    TO_TIMESTAMP ('{V_CDC_DT}', 'YYYY-MM-DD'))
																	                             OR (   cast (FARM_YR_HS.DATA_EFF_STRT_DT as date) =
																	                                    TO_TIMESTAMP ('{V_CDC_DT}', 'YYYY-MM-DD')
																	                                 OR cast (FARM_YR_HS.DATA_EFF_END_DT as date) = TO_TIMESTAMP ('{V_CDC_DT}', 'YYYY-MM-DD')))) sub_1)TMP 
																	                                 ON (FS.FARM_TR_L_ID = TMP.INCR_DR_ID AND FS.PGM_YR = TMP.PGM_YR
								  AND cast(FS.DATA_EFF_STRT_DT as date) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								  AND cast(FS.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
								  )
								  LEFT JOIN EDV.FARM_TR_L FT ON (coalesce(FS.FARM_TR_L_ID,'6bb61e3b7bce0931da574d19d1d82c88') = FT.FARM_TR_L_ID) 
								  LEFT JOIN EDV.FARM_H FH ON (coalesce(FT.FARM_H_ID,'baf6dd71fe45fe2f5c1c0e6724d514fd') = FH.FARM_H_ID) 
								  LEFT JOIN CMN_DIM_DM_STG.FARM_DIM FD ON (
								      FH.DURB_ID = coalesce(FD.FARM_DURB_ID,-1) 
								      AND cast(FD.DATA_EFF_STRT_DT as date) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								      AND cast(FD.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								  ) 
								  LEFT JOIN EDV.LOC_AREA_MRT_SRC_RS LAS_ADM ON (coalesce(LAS_ADM.CTRY_DIV_MRT_CD,'--') = FH.ST_FSA_CD AND  coalesce(LAS_ADM.LOC_AREA_MRT_CD,'--') = FH.CNTY_FSA_CD AND LAS_ADM.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'
								   AND cast(LAS_ADM.DATA_EFF_STRT_DT as date) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								  AND cast(LAS_ADM.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') ) 
								  
								  LEFT JOIN EDV.LOC_AREA_RH LAH_ADM ON (coalesce(LAS_ADM.LOC_AREA_CAT_NM, 'NULL IN SOURCE') = LAH_ADM.LOC_AREA_CAT_NM AND coalesce(LAS_ADM.LOC_AREA_NM, 'NULL IN SOURCE') = LAH_ADM.LOC_AREA_NM AND coalesce(LAS_ADM.CTRY_DIV_NM, 'NULL IN SOURCE') = LAH_ADM.CTRY_DIV_NM AND LAS_ADM.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA') 
								  LEFT JOIN CMN_DIM_DM_STG.FSA_ST_CNTY_DIM FSD_ADM ON (
								      LAH_ADM.DURB_ID = coalesce(FSD_ADM.FSA_ST_CNTY_DURB_ID,-1) 
								      AND cast(FSD_ADM.DATA_EFF_STRT_DT as date) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								      AND cast(FSD_ADM.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								  ) 
								  LEFT JOIN EDV.TR_H TRH ON (coalesce(FT.TR_H_ID,'6de912369edfb89b50859d8f305e4f72') = TRH.TR_H_ID) 
								  LEFT JOIN CMN_DIM_DM_STG.TR_DIM TD ON (
								      TRH.DURB_ID = coalesce(TD.TR_DURB_ID,-1) 
								      AND cast(TD.DATA_EFF_STRT_DT as date) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								      AND cast(TD.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								  ) 
								  LEFT JOIN EDV.TR_HS TRS ON (TRH.TR_H_ID = coalesce(TRS.TR_H_ID,'6de912369edfb89b50859d8f305e4f72')
								   AND cast(TRS.DATA_EFF_STRT_DT as date) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								      AND cast(TRS.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')) 
								  LEFT JOIN CMN_DIM_DM_STG.CONG_DIST_DIM CDS ON (
								      TRS.LOC_ST_FSA_CD = coalesce(CDS.ST_FSA_CD,'--') 
								      AND TRS.CONG_DIST_CD = CDS.CONG_DIST_CD 
								      AND cast(CDS.DATA_EFF_STRT_DT as date) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								      AND cast(CDS.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								  )  
								  LEFT JOIN EDV.LOC_AREA_MRT_SRC_RS LAS_LOC ON ( TRS.LOC_ST_FSA_CD = LAS_LOC.CTRY_DIV_MRT_CD AND TRS.LOC_CNTY_FSA_CD = LAS_LOC.LOC_AREA_MRT_CD AND LAS_LOC.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA' 
								  AND cast(LAS_LOC.DATA_EFF_STRT_DT as date) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								  AND cast(LAS_LOC.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								  ) 
								  
								  
								  LEFT JOIN EDV.LOC_AREA_RH LAH_LOC ON ( coalesce(LAS_LOC.LOC_AREA_CAT_NM, 'NULL IN SOURCE') = LAH_LOC.LOC_AREA_CAT_NM AND coalesce(LAS_LOC.LOC_AREA_NM, 'NULL IN SOURCE') = LAH_LOC.LOC_AREA_NM AND coalesce(LAS_LOC.CTRY_DIV_NM, 'NULL IN SOURCE') = LAH_LOC.CTRY_DIV_NM AND LAS_LOC.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA' ) 
								  LEFT JOIN CMN_DIM_DM_STG.FSA_ST_CNTY_DIM FSD_LOC ON ( 
								      LAH_LOC.DURB_ID = coalesce(FSD_LOC.FSA_ST_CNTY_DURB_ID,-1) 
								      AND cast(FSD_LOC.DATA_EFF_STRT_DT as date) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								      AND cast(FSD_LOC.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								  ) 
								  LEFT JOIN EDV.TR_YR_DCP_LS TR ON ( FS.FARM_TR_L_ID = coalesce(TR.FARM_TR_L_ID,'6bb61e3b7bce0931da574d19d1d82c88') AND FS.PGM_YR = TR.PGM_YR 
								  AND cast(TR.DATA_EFF_STRT_DT as date) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								  AND cast(TR.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								  ) 
								  
								  
								  LEFT JOIN EDV.HEL_TR_RS HS ON (FS.SRC_HEL_TR_CD = HS.DMN_VAL_ID
								    AND cast(HS.DATA_EFF_STRT_DT as date) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								  AND cast(HS.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								  ) 
								  LEFT JOIN EDV.HEL_TR_RH HH ON (coalesce(HS.HEL_TR_CD,'NULL IN SOURCE') = HH.HEL_TR_CD) 
								  LEFT JOIN FARM_DM_STG.HEL_TR_DIM HD ON (
								      HH.DURB_ID = HD.HEL_TR_DURB_ID 
								      AND cast(HD.DATA_EFF_STRT_DT as date) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								      AND cast(HD.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								  ) 
								  
								  LEFT JOIN EDV.TR_YR_WL_VLT_LS WLVS ON ( FS.FARM_TR_L_ID = WLVS.FARM_TR_L_ID AND FS.PGM_YR = WLVS.PGM_YR
								  AND cast(WLVS.DATA_EFF_STRT_DT as date) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								      AND cast(WLVS.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') ) 
								  LEFT JOIN EDV.WL_PRES_RS WLS ON (FS.SRC_WL_PRES_CD = WLS.DMN_VAL_ID 
								    AND cast(WLS.DATA_EFF_STRT_DT as date) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								    AND cast(WLS.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') ) 
								  
								  LEFT JOIN EDV.WL_PRES_RH WLH ON (coalesce(WLS.WL_PRES_CD,'NULL IN SOURCE') = WLH.WL_PRES_CD)  
								  LEFT JOIN FARM_DM_STG.WL_PRES_DIM WLD ON (
								      WLH.DURB_ID = coalesce(WLD.WL_PRES_DURB_ID,-1) 
								      AND cast(WLD.DATA_EFF_STRT_DT as date) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								      AND cast(WLD.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								  )  
								  
								  LEFT JOIN EDV.FARM_YR_HS FYHS ON (coalesce(FS.FARM_YR_ID, -1) = FYHS.FARM_YR_ID
								  AND cast(FYHS.DATA_EFF_STRT_DT as date) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								  AND cast(FYHS.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								  ) 
								  LEFT JOIN EDV.ARC_PLC_ELG_DTER_RH APEDRH ON (coalesce(FYHS.ARC_PLC_ELG_DTER_CD, '[NULL IN SOURCE]') = APEDRH.ARC_PLC_ELG_DTER_CD) 
								  LEFT JOIN EDV.ARC_PLC_ELG_DTER_RS APEDRS ON (APEDRH.ARC_PLC_ELG_DTER_CD = coalesce(APEDRS.ARC_PLC_ELG_DTER_CD, '[NULL IN SOURCE]')
								  AND cast(APEDRS.DATA_EFF_STRT_DT as date) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								  AND cast(APEDRS.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
								  ) 
								  LEFT JOIN FARM_DM_STG.ARC_PLC_ELG_DTER_DIM APED_DIM ON (
								      APEDRH.DURB_ID = APED_DIM.ARC_PLC_ELG_DTER_DURB_ID 
								      AND APED_DIM.DATA_STAT_CD = 'A' )
                                                                WHERE       FS.PGM_YR IS NOT NULL 
                                                                    AND     FD.FARM_DURB_ID IS NOT NULL 
                                                                    AND     TD.TR_DURB_ID IS NOT NULL 
                                                            ) sub_1
                                                WHERE       SRC_RANK = 1 
                                            ) sub_2
                            ) DM_1 
                WHERE       coalesce(FS_DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
                    AND     coalesce(LAS_ADM_DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
                    AND     coalesce(TRS_DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
                    AND     coalesce(LAS_LOC_DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
                    AND     coalesce(TR_DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
                    AND     coalesce(HS_DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
                    AND     coalesce(WLVS_DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
                    AND     coalesce(WLS_DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
                    AND     coalesce(FYHS_DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
                     
                    AND     coalesce(APEDRS_DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
            ) DM 
WHERE       DM.ROW_NUM_PART = 1
)
select  (CURRENT_DATE) as cre_dt,
		(CURRENT_DATE) as last_chg_dt,
		'A' as data_stat_cd,
		pgm_yr,
		farm_srgt_id,
		farm_durb_id,
		adm_fsa_st_cnty_srgt_id,
		adm_fsa_st_cnty_durb_id,
		tr_srgt_id,
		tr_durb_id,
		loc_fsa_st_cnty_srgt_id,
		loc_fsa_st_cnty_durb_id,
		cong_dist_srgt_id,
		cong_dist_durb_id,
		hel_tr_srgt_id,
		hel_tr_durb_id,
		wl_pres_srgt_id,
		wl_pres_durb_id,
		tr_yr_cre_dt,
		tr_yr_last_chg_dt,
		tr_yr_last_chg_user_nm,
		tr_yr_dcp_cre_dt,
		tr_yr_dcp_last_chg_dt,
		tr_yr_dcp_last_chg_user_nm,
		src_data_stat_cd,
		fav_wr_hist_ind,
		wl_cvrt_bef_11281990_ind,
		wl_cvrt_aft_11281990_ind,
		plnt_on_cvrt_wl_ind,
		fmld_acrg,
		cpld_acrg,
		crp_acrg,
		mpl_acrg,
		wbp_acrg,
		wrp_tr_acrg,
		grp_cpld_acrg,
		st_cnsv_acrg,
		ot_cnsv_acrg,
		sgrcn_acrg,
		nap_crop_acrg,
		ntv_sod_brk_out_acrg,
		dcp_dbl_crop_acrg,
		dcp_cpld_acrg,
		dcp_aft_rdn_acrg,
		dcp_ag_rlt_actv_acres,
		eff_dcp_cpld_acres,
		arc_plc_elg_dter_durb_id
from upsert_data
