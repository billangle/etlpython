WITH TAB_INCR_DR_ID AS
(
    SELECT  DISTINCT TR_PRDR_L_ID INCR_DR_ID, TR_PRDR_YR_ID
    FROM        EDV.TR_PRDR_YR_LS
    WHERE       cast(DATA_EFF_STRT_DT as date) = TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')
        AND     cast(DATA_EFF_END_DT as date) > TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')     
   UNION  
    SELECT  DISTINCT TR_PRDR_L.TR_PRDR_L_ID INCR_DR_ID, TR_PRDR_YR_ID
    FROM        EDV.TR_HS TR_HS
                ,EDV.TR_PRDR_L TR_PRDR_L
                ,EDV.TR_PRDR_YR_LS TS
    WHERE       cast(TR_HS.DATA_EFF_STRT_DT as date) = TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')
        AND     cast(TR_HS.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')
        AND     coalesce(TR_HS.TR_H_ID, '6de912369edfb89b50859d8f305e4f72') = TR_PRDR_L.TR_H_ID
        AND     (TR_PRDR_L.TR_PRDR_L_ID = coalesce(TS.TR_PRDR_L_ID, '6bb61e3b7bce0931da574d19d1d82c88'))   
    UNION
    SELECT  DISTINCT TR_PRDR_YR_LS.TR_PRDR_L_ID INCR_DR_ID, TR_PRDR_YR_ID
    FROM        EDV.PRDR_INVL_RS PRDR_INVL_RS
                ,EDV.TR_PRDR_YR_LS TR_PRDR_YR_LS
    WHERE       cast(PRDR_INVL_RS.DATA_EFF_STRT_DT as date) = TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')
        AND     cast(PRDR_INVL_RS.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')
        AND     PRDR_INVL_RS.DMN_VAL_ID = TR_PRDR_YR_LS.SRC_PRDR_INVL_CD   
    UNION    
    SELECT DISTINCT TR_PRDR_YR_LS.TR_PRDR_L_ID INCR_DR_ID, TR_PRDR_YR_ID
    FROM        EDV.FARM_PRDR_CW_EXCP_RS FARM_PRDR_CW_EXCP_RS
                ,EDV.TR_PRDR_YR_LS TR_PRDR_YR_LS
    WHERE       (
                    cast(FARM_PRDR_CW_EXCP_RS.DATA_EFF_STRT_DT as date) = TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')
                    OR cast(FARM_PRDR_CW_EXCP_RS.DATA_EFF_END_DT as date) = TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')
                )
        AND     FARM_PRDR_CW_EXCP_RS.DMN_VAL_ID = TR_PRDR_YR_LS.SRC_TR_PRDR_CW_EXCP_CD    
    UNION   
    SELECT  DISTINCT TR_PRDR_YR_LS.TR_PRDR_L_ID INCR_DR_ID, TR_PRDR_YR_ID
    FROM        EDV.FARM_PRDR_PCW_EXCP_RS FARM_PRDR_PCW_EXCP_RS
                ,EDV.TR_PRDR_YR_LS TR_PRDR_YR_LS
    WHERE       (
                    cast(FARM_PRDR_PCW_EXCP_RS.DATA_EFF_STRT_DT as date) = TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')
                    OR cast(FARM_PRDR_PCW_EXCP_RS.DATA_EFF_END_DT as date) = TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')
                )
        AND     FARM_PRDR_PCW_EXCP_RS.DMN_VAL_ID = TR_PRDR_YR_LS.SRC_TR_PRDR_PCW_EXCP_CD    
    UNION   
	SELECT  DISTINCT TR_PRDR_YR_LS.TR_PRDR_L_ID INCR_DR_ID, TR_PRDR_YR_ID
    FROM        EDV.FARM_PRDR_HEL_EXCP_RS FARM_PRDR_HEL_EXCP_RS
                ,EDV.TR_PRDR_YR_LS TR_PRDR_YR_LS
    WHERE       (
                    cast(FARM_PRDR_HEL_EXCP_RS.DATA_EFF_STRT_DT as date) = TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')
                    OR cast(FARM_PRDR_HEL_EXCP_RS.DATA_EFF_END_DT as date) = TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')
                )
        AND     FARM_PRDR_HEL_EXCP_RS.DMN_VAL_ID = TR_PRDR_YR_LS.SRC_TR_PRDR_HEL_EXCP_CD  
    UNION
    SELECT  DISTINCT TR_PRDR_YR_LS.TR_PRDR_L_ID INCR_DR_ID, TR_PRDR_YR_ID
    FROM        EDV.LOC_AREA_MRT_SRC_RS LOC_AREA_MRT_SRC_RS
                ,EDV.TR_PRDR_YR_LS TR_PRDR_YR_LS
    WHERE       (
                    cast(LOC_AREA_MRT_SRC_RS.DATA_EFF_STRT_DT as date) = TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')
                    OR cast(LOC_AREA_MRT_SRC_RS.DATA_EFF_END_DT as date) = TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')
                )
        AND     LOC_AREA_MRT_SRC_RS.CTRY_DIV_MRT_CD = TR_PRDR_YR_LS.ST_FSA_CD
        AND     LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD = TR_PRDR_YR_LS.CNTY_FSA_CD
        AND     LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'    
    UNION
    SELECT   DISTINCT TR_PRDR_L.TR_PRDR_L_ID INCR_DR_ID, TR_PRDR_YR_ID
    FROM        EDV.LOC_AREA_MRT_SRC_RS LOC_AREA_MRT_SRC_RS
                ,EDV.TR_HS TR_HS
                ,EDV.TR_PRDR_L TR_PRDR_L
                ,EDV.TR_PRDR_YR_LS TS
    WHERE       (
                    cast(LOC_AREA_MRT_SRC_RS.DATA_EFF_STRT_DT as date) = TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')
                    OR cast(LOC_AREA_MRT_SRC_RS.DATA_EFF_END_DT as date) = TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')
                )
        AND     LOC_AREA_MRT_SRC_RS.CTRY_DIV_MRT_CD = TR_HS.LOC_ST_FSA_CD
        AND     LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD = TR_HS.LOC_CNTY_FSA_CD
        AND     LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'
        AND     coalesce(TR_HS.TR_H_ID, '6de912369edfb89b50859d8f305e4f72') = TR_PRDR_L.TR_H_ID
        AND     (TR_PRDR_L.TR_PRDR_L_ID = coalesce(TS.TR_PRDR_L_ID, '6bb61e3b7bce0931da574d19d1d82c88'))
        
), upsert_data as(
SELECT      DM.FARM_SRGT_ID
            ,DM.FARM_DURB_ID
            ,DM.ADM_FSA_ST_CNTY_SRGT_ID
            ,DM.ADM_FSA_ST_CNTY_DURB_ID
            ,DM.PGM_YR
            ,DM.TR_SRGT_ID
            ,DM.TR_DURB_ID
            ,DM.LOC_FSA_ST_CNTY_SRGT_ID
            ,DM.LOC_FSA_ST_CNTY_DURB_ID
            ,DM.CONG_DIST_SRGT_ID
            ,DM.CONG_DIST_DURB_ID
            ,DM.PRDR_CUST_SRGT_ID
            ,DM.PRDR_CUST_DURB_ID
            ,DM.PRDR_INVL_SRGT_ID
            ,DM.PRDR_INVL_DURB_ID     
            ,DM.TR_PRDR_PCW_EXCP_DURB_ID           
            ,DM.TR_PRDR_CW_EXCP_DURB_ID
            ,DM.TR_PRDR_HEL_EXCP_DURB_ID
            ,DM.TR_PRDR_CRE_DT
            ,DM.TR_PRDR_LAST_CHG_DT
            ,DM.TR_PRDR_LAST_CHG_USER_NM
            ,DM.SRC_DATA_STAT_CD
            ,DM.PRDR_INVL_INTRPT_IND
            ,DM.PRDR_INVL_STRT_DT
            ,DM.PRDR_INVL_END_DT
            ,DM.DATA_EFF_STRT_DT,
             RMA_TR_PRDR_PCW_EXCP_DURB_ID, 
            RMA_TR_PRDR_CW_EXCP_DURB_ID,                      
            RMA_TR_PRDR_HEL_EXCP_DURB_ID,
            HEL_APL_EXHST_DT,
            CW_APL_EXHST_DT,
            PCW_APL_EXHST_DT
FROM        (
                SELECT      DM_1.FARM_SRGT_ID
                            ,DM_1.FARM_DURB_ID
                            ,DM_1.ADM_FSA_ST_CNTY_SRGT_ID
                            ,DM_1.ADM_FSA_ST_CNTY_DURB_ID
                            ,DM_1.PGM_YR
                            ,DM_1.TR_SRGT_ID
                            ,DM_1.TR_DURB_ID
                            ,DM_1.LOC_FSA_ST_CNTY_SRGT_ID
                            ,DM_1.LOC_FSA_ST_CNTY_DURB_ID
                            ,DM_1.CONG_DIST_SRGT_ID
                            ,DM_1.CONG_DIST_DURB_ID
                            ,DM_1.PRDR_CUST_SRGT_ID
                            ,DM_1.PRDR_CUST_DURB_ID
                            ,DM_1.PRDR_INVL_SRGT_ID
                            ,DM_1.PRDR_INVL_DURB_ID                          
                            ,DM_1.TR_PRDR_PCW_EXCP_DURB_ID                            
                            ,DM_1.TR_PRDR_CW_EXCP_DURB_ID                            
                            ,DM_1.TR_PRDR_HEL_EXCP_DURB_ID
                            ,DM_1.TR_PRDR_CRE_DT
                            ,DM_1.TR_PRDR_LAST_CHG_DT
                            ,DM_1.TR_PRDR_LAST_CHG_USER_NM
                            ,DM_1.SRC_DATA_STAT_CD
                            ,DM_1.PRDR_INVL_INTRPT_IND
                            ,DM_1.PRDR_INVL_STRT_DT
                            ,DM_1.PRDR_INVL_END_DT
                            ,DM_1.DATA_EFF_STRT_DT
        
                            ,DM_1.TS_DATA_EFF_STRT_DT
                            ,DM_1.LAS_ADM_DATA_EFF_STRT_DT
                            ,DM_1.TRS_DATA_EFF_STRT_DT
                            ,DM_1.CWE_DATA_EFF_STRT_DT
                            ,DM_1.HES_DATA_EFF_STRT_DT
                            ,DM_1.LAS_LOC_DATA_EFF_STRT_DT
                            ,DM_1.PIS_DATA_EFF_STRT_DT,
                             DM_1.RMA_TR_PRDR_PCW_EXCP_DURB_ID, 
                             DM_1.RMA_TR_PRDR_CW_EXCP_DURB_ID,                      
                             DM_1.RMA_TR_PRDR_HEL_EXCP_DURB_ID,
                             DM_1.HEL_APL_EXHST_DT,
                             DM_1.CW_APL_EXHST_DT,
                             DM_1.PCW_APL_EXHST_DT
                            
                            ,ROW_NUMBER() OVER 
                            (
                                PARTITION BY    FARM_DURB_ID, PGM_YR, TR_DURB_ID, PRDR_CUST_DURB_ID, PRDR_INVL_DURB_ID
                                ORDER BY        coalesce(TS_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) DESC
                                                ,coalesce(LAS_ADM_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) DESC
                                                ,coalesce(TRS_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) DESC
                                                ,coalesce(CWE_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) DESC
                                                ,coalesce(HES_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) DESC
                                                ,coalesce(LAS_LOC_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) DESC
                                                ,coalesce(PIS_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) DESC
                            ) AS ROW_NUM_PART
                FROM        (
                                SELECT      FARM_SRGT_ID
                                            ,FARM_DURB_ID
                                            ,ADM_FSA_ST_CNTY_SRGT_ID
                                            ,ADM_FSA_ST_CNTY_DURB_ID
                                            ,PGM_YR
                                            ,TR_SRGT_ID
                                            ,TR_DURB_ID
                                            ,LOC_FSA_ST_CNTY_SRGT_ID
                                            ,LOC_FSA_ST_CNTY_DURB_ID
                                            ,CONG_DIST_SRGT_ID
                                            ,CONG_DIST_DURB_ID
                                            ,PRDR_CUST_SRGT_ID
                                            ,PRDR_CUST_DURB_ID
                                            ,PRDR_INVL_SRGT_ID
                                            ,PRDR_INVL_DURB_ID                                          
                                            ,TR_PRDR_PCW_EXCP_DURB_ID                                      
                                            ,TR_PRDR_CW_EXCP_DURB_ID                                            
                                            ,TR_PRDR_HEL_EXCP_DURB_ID
                                            ,TR_PRDR_CRE_DT
                                            ,TR_PRDR_LAST_CHG_DT
                                            ,TR_PRDR_LAST_CHG_USER_NM
                                            ,SRC_DATA_STAT_CD
                                            ,PRDR_INVL_INTRPT_IND
                                            ,PRDR_INVL_STRT_DT
                                            ,PRDR_INVL_END_DT
                                            ,DATA_EFF_STRT_DT
            
                                            ,TS_DATA_EFF_STRT_DT
                                            ,LAS_ADM_DATA_EFF_STRT_DT
                                            ,TRS_DATA_EFF_STRT_DT
                                            ,CWE_DATA_EFF_STRT_DT
                                            ,HES_DATA_EFF_STRT_DT
                                            ,LAS_LOC_DATA_EFF_STRT_DT
                                            ,PIS_DATA_EFF_STRT_DT ,
                                             RMA_TR_PRDR_PCW_EXCP_DURB_ID, 
                                             RMA_TR_PRDR_CW_EXCP_DURB_ID,                      
                                             RMA_TR_PRDR_HEL_EXCP_DURB_ID,
                                             HEL_APL_EXHST_DT,
                                             CW_APL_EXHST_DT,
                                             PCW_APL_EXHST_DT
                                FROM        (
                                                SELECT      *
                                                FROM        (
                                                                SELECT  DISTINCT
                                                                            ROW_NUMBER() OVER
                                                                            (
                                                                                PARTITION BY    FD.FARM_DURB_ID, TS.PGM_YR, TD.TR_DURB_ID, CCD.CUST_DURB_ID, PID.PRDR_INVL_DURB_ID
                                                                                ORDER BY        coalesce(TRS.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) DESC
                                                                                                , coalesce(CWE.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) DESC, coalesce(LAS_ADM.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) DESC
                                                                                                , coalesce(HES.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) DESC, coalesce(LAS_LOC.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) DESC
                                                                                                , coalesce(PES.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) DESC, coalesce(PIS.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) DESC
                                                                                                , coalesce(TS.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) DESC, TS.TR_PRDR_YR_ID DESC
                                                                            ) AS SRC_RANK
                                                                            ,coalesce(FD.FARM_SRGT_ID, - 3) FARM_SRGT_ID
                                                                            ,FD.FARM_DURB_ID FARM_DURB_ID
                                                                            ,coalesce(FSD_ADM.FSA_ST_CNTY_SRGT_ID, - 3) ADM_FSA_ST_CNTY_SRGT_ID
                                                                            ,coalesce(FSD_ADM.FSA_ST_CNTY_DURB_ID, - 3) ADM_FSA_ST_CNTY_DURB_ID
                                                                            ,TS.PGM_YR PGM_YR
                                                                            ,coalesce(TD.TR_SRGT_ID, - 3) TR_SRGT_ID
                                                                            ,TD.TR_DURB_ID TR_DURB_ID
                                                                            ,coalesce(FSD_LOC.FSA_ST_CNTY_SRGT_ID, - 3) LOC_FSA_ST_CNTY_SRGT_ID
                                                                            ,coalesce(FSD_LOC.FSA_ST_CNTY_DURB_ID, - 3) LOC_FSA_ST_CNTY_DURB_ID
                                                                            ,coalesce(CDS.CONG_DIST_SRGT_ID, - 3) CONG_DIST_SRGT_ID
                                                                            ,coalesce(CDS.CONG_DIST_DURB_ID, - 3) CONG_DIST_DURB_ID
                                                                            ,coalesce(CCD.CUST_SRGT_ID, - 3) PRDR_CUST_SRGT_ID
                                                                            ,CCD.CUST_DURB_ID PRDR_CUST_DURB_ID
                                                                            ,coalesce(PID.PRDR_INVL_SRGT_ID, - 3) PRDR_INVL_SRGT_ID
                                                                            ,PID.PRDR_INVL_DURB_ID                                                                            
                                                                            ,coalesce(PED.TR_PRDR_PCW_EXCP_DURB_ID, - 3) TR_PRDR_PCW_EXCP_DURB_ID 
                                                                            ,coalesce(RPED.RMA_TR_PRDR_PCW_EXCP_DURB_ID,-3)   RMA_TR_PRDR_PCW_EXCP_DURB_ID                                                                           
                                                                            ,coalesce(CWD.TR_PRDR_CW_EXCP_DURB_ID, - 3) TR_PRDR_CW_EXCP_DURB_ID   
                                                                            ,coalesce(RCWD.RMA_TR_PRDR_CW_EXCP_DURB_ID,-3) RMA_TR_PRDR_CW_EXCP_DURB_ID                                                                      
                                                                            ,coalesce(HED.TR_PRDR_HEL_EXCP_DURB_ID, - 3) TR_PRDR_HEL_EXCP_DURB_ID
                                                                            ,coalesce(RHED.RMA_TR_PRDR_HEL_EXCP_DURB_ID,-3)  RMA_TR_PRDR_HEL_EXCP_DURB_ID
                                                                            ,TS.SRC_CRE_DT TR_PRDR_CRE_DT
                                                                            ,TS.SRC_LAST_CHG_DT TR_PRDR_LAST_CHG_DT
                                                                            ,TS.SRC_LAST_CHG_USER_NM TR_PRDR_LAST_CHG_USER_NM
                                                                            ,TS.DATA_STAT_CD SRC_DATA_STAT_CD
                                                                            ,TS.PRDR_INVL_INTRPT_IND PRDR_INVL_INTRPT_IND
                                                                            ,TS.PRDR_INVL_STRT_DT PRDR_INVL_STRT_DT
                                                                            ,TS.PRDR_INVL_END_DT PRDR_INVL_END_DT
                                                                            ,GREATEST
                                                                            (
                                                                                coalesce(TS.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')), coalesce(LAS_ADM.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD'))
                                                                                , coalesce(TRS.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')), coalesce(CWE.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD'))
                                                                                , coalesce(HES.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')), coalesce(LAS_LOC.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD'))
                                                                                , coalesce(PES.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')), coalesce(PIS.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD'))
                                                                            ) DATA_EFF_STRT_DT
                    
                                                                            ,TS.DATA_EFF_STRT_DT TS_DATA_EFF_STRT_DT
                                                                            ,LAS_ADM.DATA_EFF_STRT_DT LAS_ADM_DATA_EFF_STRT_DT
                                                                            ,TRS.DATA_EFF_STRT_DT TRS_DATA_EFF_STRT_DT
                                                                            ,CWE.DATA_EFF_STRT_DT CWE_DATA_EFF_STRT_DT
                                                                            ,HES.DATA_EFF_STRT_DT HES_DATA_EFF_STRT_DT
                                                                            ,LAS_LOC.DATA_EFF_STRT_DT LAS_LOC_DATA_EFF_STRT_DT
                                                                            ,PIS.DATA_EFF_STRT_DT PIS_DATA_EFF_STRT_DT,
                                                                             coalesce(TO_NUMBER(TS.CW_APLS_EXHST_DT::VARCHAR,'YYYYMMDD'),-1) CW_APL_EXHST_DT,
                                                                             coalesce(TO_NUMBER(TS.HEL_APLS_EXHST_DT::VARCHAR,'YYYYMMDD'),-1) HEL_APL_EXHST_DT,
                                                                             coalesce(TO_NUMBER(TS.PCW_APLS_EXHST_DT::VARCHAR,'YYYYMMDD'),-1) PCW_APL_EXHST_DT
                FROM    ( select * from  EDV.TR_PRDR_YR_LS where  (TR_PRDR_L_ID, TR_PRDR_YR_ID) IN
                   (  SELECT      INCR_DR_ID, TR_PRDR_YR_ID   FROM        TAB_INCR_DR_ID   )
                      AND     PGM_YR IS NOT NULL
                    ) TS
                                                                            LEFT JOIN EDV.TR_PRDR_L TL ON (TL.TR_PRDR_L_ID = coalesce(TS.TR_PRDR_L_ID, '6bb61e3b7bce0931da574d19d1d82c88'))
                                                                            LEFT JOIN EDV.LOC_AREA_MRT_SRC_RS LAS_ADM ON (
                                                                                    TS.ST_FSA_CD = LAS_ADM.CTRY_DIV_MRT_CD
                                                                                    AND TS.CNTY_FSA_CD = LAS_ADM.LOC_AREA_MRT_CD
                                                                                    AND LAS_ADM.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'
                                                                                    )
                                                                            LEFT JOIN EDV.LOC_AREA_RH LAH_ADM ON (
                                                                                    coalesce(LAS_ADM.LOC_AREA_CAT_NM, '[NULL IN SOURCE]') = LAH_ADM.LOC_AREA_CAT_NM
                                                                                    AND coalesce(LAS_ADM.LOC_AREA_NM, '[NULL IN SOURCE]') = LAH_ADM.LOC_AREA_NM
                                                                                    AND coalesce(LAS_ADM.CTRY_DIV_NM, '[NULL IN SOURCE]') = LAH_ADM.CTRY_DIV_NM
                                                                                    AND LAS_ADM.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'
                                                                                    )
                                                                            LEFT JOIN CMN_DIM_DM_STG.FSA_ST_CNTY_DIM FSD_ADM ON (
                                                                                    LAH_ADM.DURB_ID = coalesce(FSD_ADM.FSA_ST_CNTY_DURB_ID, - 1)
                                                                                    AND FSD_ADM.CUR_RCD_IND = 1
                                                                                    )
                                                                            LEFT JOIN EDV.TR_HS TRS ON (TL.TR_H_ID = coalesce(TRS.TR_H_ID, '6de912369edfb89b50859d8f305e4f72'))
                                                                            LEFT JOIN EDV.TR_H TRH ON (coalesce(TRS.TR_H_ID, '6de912369edfb89b50859d8f305e4f72') = TRH.TR_H_ID)
                                                                            LEFT JOIN CMN_DIM_DM_STG.CONG_DIST_DIM CDS ON (
                                                                                    coalesce(TRS.LOC_ST_FSA_CD, '--') = CDS.ST_FSA_CD
                                                                                    AND coalesce(TRS.CONG_DIST_CD, '--') = CDS.CONG_DIST_CD
                                                                                    AND CDS.CUR_RCD_IND = 1
                                                                                    )
                                                                            LEFT JOIN CMN_DIM_DM_STG.TR_DIM TD ON (
                                                                                    TRH.DURB_ID = coalesce(TD.TR_DURB_ID, - 1)
                                                                                    AND TD.CUR_RCD_IND = 1
                                                                                    )
                                                                            LEFT JOIN EDV.TR_PRDR_CW_EXCP_RS CWE ON (coalesce(TS.SRC_TR_PRDR_CW_EXCP_CD, -1) = coalesce(CWE.DMN_VAL_ID, -1))
                                                                            LEFT JOIN EDV.TR_PRDR_CW_EXCP_RH CWH ON (coalesce(CWE.TR_PRDR_CW_EXCP_CD, '[NULL IN SOURCE]') = CWH.TR_PRDR_CW_EXCP_CD)
                                                                            LEFT JOIN FARM_DM_STG.TR_PRDR_CW_EXCP_DIM CWD ON (
                                                                                    CWH.DURB_ID = coalesce(CWD.TR_PRDR_CW_EXCP_DURB_ID, - 1)
                                                                                    AND CWD.DATA_STAT_CD='A'
                                                                                    )
                                                                            LEFT JOIN EDV.FARM_H FH ON (coalesce(TL.FARM_H_ID, 'baf6dd71fe45fe2f5c1c0e6724d514fd') = FH.FARM_H_ID)
                                                                            LEFT JOIN CMN_DIM_DM_STG.FARM_DIM FD ON (
                                                                                    coalesce(FD.FARM_DURB_ID, - 1) = FH.DURB_ID
                                                                                    AND FD.CUR_RCD_IND = 1
                                                                                    )
                                                                            LEFT JOIN EDV.TR_PRDR_HEL_EXCP_RS HES ON (coalesce(TS.SRC_TR_PRDR_HEL_EXCP_CD, -1) = coalesce(HES.DMN_VAL_ID, -1))
                                                                            LEFT JOIN EDV.TR_PRDR_HEL_EXCP_RH HEH ON (HEH.TR_PRDR_HEL_EXCP_CD = coalesce(HES.TR_PRDR_HEL_EXCP_CD, '[NULL IN SOURCE]'))
                                                                            LEFT JOIN FARM_DM_STG.TR_PRDR_HEL_EXCP_DIM HED ON (
                                                                                    HED.TR_PRDR_HEL_EXCP_DURB_ID = HEH.DURB_ID
                                                                                    AND HED.DATA_STAT_CD ='A'
                                                                                    )
                                                                            LEFT JOIN EDV.LOC_AREA_MRT_SRC_RS LAS_LOC ON (
                                                                                    TRS.LOC_ST_FSA_CD = LAS_LOC.CTRY_DIV_MRT_CD
                                                                                    AND TRS.LOC_CNTY_FSA_CD = LAS_LOC.LOC_AREA_MRT_CD
                                                                                    AND LAS_LOC.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'
                                                                                    )
                                                                            LEFT JOIN EDV.LOC_AREA_RH LAH_LOC ON (
                                                                                    coalesce(LAS_LOC.LOC_AREA_CAT_NM, '[NULL IN SOURCE]') = LAH_LOC.LOC_AREA_CAT_NM
                                                                                    AND coalesce(LAS_LOC.LOC_AREA_NM, '[NULL IN SOURCE]') = LAH_LOC.LOC_AREA_NM
                                                                                    AND coalesce(LAS_LOC.CTRY_DIV_NM, '[NULL IN SOURCE]') = LAH_LOC.CTRY_DIV_NM
                                                                                    AND LAS_LOC.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'
                                                                                    )
                                                                            LEFT JOIN CMN_DIM_DM_STG.FSA_ST_CNTY_DIM FSD_LOC ON (
                                                                                    LAH_LOC.DURB_ID = coalesce(FSD_LOC.FSA_ST_CNTY_DURB_ID, - 1)
                                                                                    AND FSD_LOC.CUR_RCD_IND = 1
                                                                                    )
                                                                            LEFT JOIN EDV.CORE_CUST_H CCH ON (CCH.CORE_CUST_ID = coalesce(TS.CORE_CUST_ID, - 1))
                                                                            LEFT JOIN CMN_DIM_DM_STG.CUST_DIM CCD ON (
                                                                                    CCH.DURB_ID = coalesce(CCD.CUST_DURB_ID, - 1)
                                                                                    AND CCD.CUR_RCD_IND = 1
                                                                                    )
                                                                            LEFT JOIN EDV.TR_PRDR_PCW_EXCP_RS PES ON (coalesce(TS.SRC_TR_PRDR_PCW_EXCP_CD, -1) = coalesce(PES.DMN_VAL_ID, -1))
                                                                            LEFT JOIN EDV.TR_PRDR_PCW_EXCP_RH PEH ON (coalesce(PES.TR_PRDR_PCW_EXCP_CD, '[NULL IN SOURCE]') = PEH.TR_PRDR_PCW_EXCP_CD)
                                                                            LEFT JOIN FARM_DM_STG.TR_PRDR_PCW_EXCP_DIM PED ON (
                                                                                    PEH.DURB_ID = coalesce(PED.TR_PRDR_PCW_EXCP_DURB_ID, - 1)
                                                                                    AND PED.DATA_STAT_CD='A'
                                                                                    )
                                                                            LEFT JOIN EDV.PRDR_INVL_RS PIS ON (TS.SRC_PRDR_INVL_CD = PIS.DMN_VAL_ID)
                                                                            LEFT JOIN EBV.PRDR_INVL_RH PIR ON (coalesce(PIS.PRDR_INVL_CD, '[NULL IN SOURCE]') = PIR.PRDR_INVL_CD)
                                                                            LEFT JOIN FARM_DM_STG.PRDR_INVL_DIM PID ON (
                                                                                    PIR.DURB_ID = coalesce(PID.PRDR_INVL_DURB_ID, - 1)
                                                                                    AND PID.CUR_RCD_IND = 1
                                                                                    )
                                                                                    
                            LEFT JOIN EDV.RMA_CW_EXCP_RS RCWE ON (TS.TR_PRDR_RMA_CW_EXCP_CD = RCWE.DMN_VAL_ID 
                             AND cast(RCWE.DATA_EFF_END_DT as date) = TO_TIMESTAMP('9999-12-31','YYYY-MM-DD')       )
                             LEFT JOIN EDV.RMA_CW_EXCP_RH  RCWH ON ( coalesce(RCWE.RMA_CW_EXCP_CD,'[NULL IN SOURCE]') = RCWH.RMA_CW_EXCP_CD )
                             LEFT JOIN FARM_DM_STG.RMA_TR_PRDR_CW_EXCP_DIM RCWD ON ( RCWH.DURB_ID = RCWD.RMA_TR_PRDR_CW_EXCP_DURB_ID AND RCWD.DATA_STAT_CD = 'A') 
                              
                            LEFT JOIN EDV.RMA_HEL_EXCP_RS RHES ON (TS.TR_PRDR_RMA_HEL_EXCP_CD = RHES.DMN_VAL_ID
                             AND cast(RHES.DATA_EFF_END_DT as date) = TO_TIMESTAMP('9999-12-31','YYYY-MM-DD') )                                  
                             LEFT JOIN EDV.RMA_HEL_EXCP_RH RHEH ON ( coalesce(RHES.RMA_HEL_EXCP_CD,'[NULL IN SOURCE]') = RHEH.RMA_HEL_EXCP_CD)
                             LEFT JOIN FARM_DM_STG.RMA_TR_PRDR_HEL_EXCP_DIM RHED ON (coalesce(RHED.RMA_TR_PRDR_HEL_EXCP_DURB_ID,-1) = RHEH.DURB_ID AND RHED.DATA_STAT_CD = 'A' )
                             

                             LEFT JOIN EDV.RMA_PCW_EXCP_RS RPES ON (TS.TR_PRDR_RMA_PCW_EXCP_CD = RPES.DMN_VAL_ID
                              AND cast(RPES.DATA_EFF_END_DT as date) = TO_TIMESTAMP('9999-12-31','YYYY-MM-DD') )
                              LEFT JOIN EDV.RMA_PCW_EXCP_RH  RPEH ON ( coalesce(RPES.RMA_PCW_EXCP_CD,'[NULL IN SOURCE]') = RPEH.RMA_PCW_EXCP_CD )
                              LEFT JOIN FARM_DM_STG.RMA_TR_PRDR_PCW_EXCP_DIM RPED ON (RPEH.DURB_ID = RPED.RMA_TR_PRDR_PCW_EXCP_DURB_ID AND RPED.DATA_STAT_CD = 'A' )
                                
                                                                WHERE       FD.FARM_DURB_ID IS NOT NULL                                                                    
                                                                    AND     TD.TR_DURB_ID IS NOT NULL
                                                                    AND     CCD.CUST_DURB_ID IS NOT NULL
                                                                    AND     PID.PRDR_INVL_DURB_ID IS NOT NULL
                                                            ) sub_1
                                                WHERE       SRC_RANK = 1
                                            ) sub
                            ) DM_1
                WHERE       cast(coalesce(TS_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) as date) <= TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')
                    AND     cast(coalesce(LAS_ADM_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) as date) <= TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')
                    AND     cast(coalesce(TRS_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) as date) <= TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')
                    AND     cast(coalesce(CWE_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) as date) <= TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')
                    AND     cast(coalesce(HES_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) as date) <= TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')
                    AND     cast(coalesce(LAS_LOC_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD'))as date) <= TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')
                    AND     cast(coalesce(PIS_DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31', 'YYYY-MM-DD')) as date) <= TO_TIMESTAMP('{ETL_DATE}', 'YYYY-MM-DD')
            ) DM
WHERE       DM.ROW_NUM_PART = 1
)

select  (current_date) as cre_dt,
		(current_date) as last_chg_dt,
		'A' as data_stat_cd,
		farm_srgt_id,
		farm_durb_id,
		adm_fsa_st_cnty_srgt_id,
		adm_fsa_st_cnty_durb_id,
		pgm_yr,
		tr_srgt_id,
		tr_durb_id,
		loc_fsa_st_cnty_srgt_id,
		loc_fsa_st_cnty_durb_id,
		cong_dist_srgt_id,
		cong_dist_durb_id,
		prdr_cust_srgt_id,
		prdr_cust_durb_id,
		prdr_invl_srgt_id,
		prdr_invl_durb_id,
		tr_prdr_pcw_excp_durb_id,
		tr_prdr_cw_excp_durb_id,
		tr_prdr_hel_excp_durb_id,
		tr_prdr_cre_dt,
		tr_prdr_last_chg_dt,
		tr_prdr_last_chg_user_nm,
		src_data_stat_cd,
		prdr_invl_intrpt_ind,
		prdr_invl_strt_dt,
		prdr_invl_end_dt,
		rma_tr_prdr_pcw_excp_durb_id,
		rma_tr_prdr_cw_excp_durb_id,
		rma_tr_prdr_hel_excp_durb_id,
		hel_apl_exhst_dt,
		cw_apl_exhst_dt,
		pcw_apl_exhst_dt
from upsert_data tab_incr 

