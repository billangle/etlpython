with tab_incr_dr_id as
(
    /*Get TR_H_ID starting from TR_HS , the driving table
    as this sat is part of Fact alternate key we need to use AND cdc condition */
    
    SELECT /*+ parallel(4) */ distinct TR_H_ID incr_dr_id
FROM EDV.TR_H
Where trunc((TR_H.LOAD_DT)-1) = TO_TIMESTAMP ('{V_CDC_DT}', 'YYYY-MM-DD')
UNION
    
    SELECT      /*+ parallel(4) */ distinct TR_H_ID incr_dr_id
    FROM        EDV.TR_HS
    WHERE       trunc(TR_HS.DATA_EFF_STRT_DT) =  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
        AND     trunc(TR_HS.DATA_EFF_END_DT) >  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
    
    UNION
    
    /*Get TR_H_ID starting from edv.LOC_AREA_MRT_SRC_RS LAS_ADM
    as this sat is not part of Fact alternate key we need to use OR cdc condition */
    
    SELECT      /*+ parallel(4) */ distinct  TR_H_ID incr_dr_id
    FROM        EDV.LOC_AREA_MRT_SRC_RS, EDV.TR_H
    WHERE       (
                    trunc(LOC_AREA_MRT_SRC_RS.DATA_EFF_STRT_DT) =  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
                    OR trunc(LOC_AREA_MRT_SRC_RS.DATA_EFF_END_DT) =   TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
                )
        AND     LOC_AREA_MRT_SRC_RS.CTRY_DIV_MRT_CD = TR_H.ST_FSA_CD
        AND     LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD = TR_H.CNTY_FSA_CD
        AND     LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'

    UNION
    
    /*Get TR_H_ID starting from edv.LOC_AREA_MRT_SRC_RS LAS_LOC
   as this sat is not part of Fact alternate key we need to use OR cdc condition */

    SELECT      /*+ parallel(4) */ distinct  TR_H_ID incr_dr_id
    FROM        EDV.LOC_AREA_MRT_SRC_RS, EDV.TR_HS
    WHERE       (
                    trunc(LOC_AREA_MRT_SRC_RS.DATA_EFF_STRT_DT) =  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
                    OR trunc(LOC_AREA_MRT_SRC_RS.DATA_EFF_END_DT) =   TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
                )
        AND     LOC_AREA_MRT_SRC_RS.CTRY_DIV_MRT_CD = NVL(TR_HS.LOC_ST_FSA_CD,'--')
        AND     LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD = NVL(TR_HS.LOC_CNTY_FSA_CD,'--')
        AND     TRIM(LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD_SRC_ACRO) = 'FSA'

    UNION

    /* Get TR_H_ID starting from edv.LOC_AREA_MRT_SRC_RS LAS_LOC
    as this sat is not part of Fact alternate key we need to use OR cdc condition */
        
    SELECT  /*+ parallel(4) */ distinct  TR_H_ID incr_dr_id
    FROM    CMN_DIM_DM.CONG_DIST_DIM, EDV.TR_HS
    WHERE   (
                trunc(CMN_DIM_DM.CONG_DIST_DIM.DATA_EFF_STRT_DT) =  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
                OR trunc(CMN_DIM_DM.CONG_DIST_DIM.DATA_EFF_END_DT) =   TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
            )
        AND ( NVL(TR_HS.CONG_DIST_CD,'--') = NVL(CMN_DIM_DM.CONG_DIST_DIM.CONG_DIST_CD,'--') )

UNION

    /*Get TR_H_ID starting from edv.WL_CERT_CPLT_RS
    as this sat is not part of Fact alternate key we need to use OR cdc condition */
    
    SELECT      /*+ parallel(4) */ distinct  TR_H_ID incr_dr_id
    FROM        EDV.WL_CERT_CPLT_RS, EDV.TR_HS
    WHERE       (
                    trunc(WL_CERT_CPLT_RS.DATA_EFF_STRT_DT) =  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD') 
                    OR trunc(WL_CERT_CPLT_RS.DATA_EFF_END_DT) =   TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
                )
        AND     ( NVL(TR_HS.SRC_WL_CERT_CPLT_CD,-1) = NVL(WL_CERT_CPLT_RS.DMN_VAL_ID,-1) )
)

SELECT      DISTINCT 
            dm.TR_DURB_ID,
            dm.DATA_EFF_STRT_DT,
            dm.ST_FSA_CD,
            dm.CNTY_FSA_CD,
            dm.ST_FSA_NM,
            dm.CNTY_FSA_NM,
            dm.TR_NBR,
            dm.TR_CRE_DT,
            dm.TR_LAST_CHG_DT,
            dm.TR_LAST_CHG_USER_NM,
            dm.SRC_DATA_STAT_CD,
            dm.TR_DESC,
            dm.BIA_RNG_UNIT_NBR,
            dm.LOC_ST_FSA_CD,
            dm.LOC_CNTY_FSA_CD,
            dm.LOC_ST_FSA_NM,
            dm.LOC_CNTY_FSA_NM,
            dm.CONG_DIST_CD,
            dm.CONG_DIST_NM,
            dm.WL_CERT_CPLT_CD,
            dm.WL_CERT_CPLT_DESC,
            dm.WL_CERT_CPLT_YR
From        (
                select      /*+ PARALLEL(8) */ 
                            TH.DURB_ID as TR_DURB_ID,
                            greatest
                            (   COALESCE(THS.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')),
                                COALESCE(ADM.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')),
                                COALESCE(LOC.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')),
                                COALESCE(CDS.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')),
                                COALESCE(WLS.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD'))
                            ) as DATA_EFF_STRT_DT,
                            TH.ST_FSA_CD       as ST_FSA_CD,
                            TH.CNTY_FSA_CD     as CNTY_FSA_CD,
                            ADM.CTRY_DIV_NM    as ST_FSA_NM,
                            ADM.LOC_AREA_NM    as CNTY_FSA_NM,
                            TH.TR_NBR          as TR_NBR,
                            THS.SRC_CRE_DT     as TR_CRE_DT,
                            THS.SRC_LAST_CHG_DT       as  TR_LAST_CHG_DT,
                            THS.SRC_LAST_CHG_USER_NM   as TR_LAST_CHG_USER_NM,
                            THS.DATA_STAT_CD          as  SRC_DATA_STAT_CD,
                            THS.TR_DESC               as  TR_DESC,
                            THS.BIA_RNG_UNIT_NBR      as  BIA_RNG_UNIT_NBR,
                            THS.LOC_ST_FSA_CD         as  LOC_ST_FSA_CD,
                            THS.LOC_CNTY_FSA_CD      as   LOC_CNTY_FSA_CD,
                            LOC.CTRY_DIV_NM          as   LOC_ST_FSA_NM,
                            LOC.LOC_AREA_NM          as   LOC_CNTY_FSA_NM,
                            THS.CONG_DIST_CD         as   CONG_DIST_CD,
                            CDS.CONG_DIST_NM         as   CONG_DIST_NM,
                            WLS.WL_CERT_CPLT_CD      as   WL_CERT_CPLT_CD,
                            THS.WL_CERT_CPLT_NM      as   WL_CERT_CPLT_DESC,
                            THS.WL_CERT_CPLT_YR      as   WL_CERT_CPLT_YR,
                            ROW_NUMBER() OVER 
                            (
                                partition by    TH.DURB_ID
                                ORDER BY        THS.DATA_EFF_STRT_DT desc,
                                                ADM.DATA_EFF_STRT_DT desc,
                                                LOC.DATA_EFF_STRT_DT desc,
                                                CDS.DATA_EFF_STRT_DT desc,
                                                WLS.DATA_EFF_STRT_DT desc
                            ) as ROW_NUM_PART
                FROM        EDV.TR_H TH
                            LEFT JOIN EDV.TR_HS THS ON ( TH.TR_H_ID = NVL(THS.TR_H_ID,'6de912369edfb89b50859d8f305e4f72'))
                            LEFT JOIN EDV.LOC_AREA_MRT_SRC_RS ADM ON ( TH.ST_FSA_CD = NVL(ADM.CTRY_DIV_MRT_CD,'--')
                                AND TH.CNTY_FSA_CD = NVL(ADM.LOC_AREA_MRT_CD,'--')
                                AND TRIM(ADM.LOC_AREA_MRT_CD_SRC_ACRO) = 'FSA')
                            LEFT JOIN EDV.LOC_AREA_MRT_SRC_RS LOC ON ( THS.LOC_ST_FSA_CD = NVL(LOC.CTRY_DIV_MRT_CD,'--') 
                                and THS.LOC_CNTY_FSA_CD = NVL(LOC.LOC_AREA_MRT_CD,'--')
                               and TRIM(LOC.LOC_AREA_MRT_CD_SRC_ACRO) = 'FSA' )
                            LEFT JOIN CMN_DIM_DM.CONG_DIST_DIM CDS ON ( THS.LOC_ST_FSA_CD = NVL(CDS.ST_FSA_CD,'--')
                                                    AND NVL(THS.CONG_DIST_CD,'--') = NVL(CDS.CONG_DIST_CD,'--') 
                                                    AND TRUNC(CDS.DATA_EFF_END_DT) = TO_TIMESTAMP('9999-12-31','YYYY-MM-DD')
                                                    AND TRUNC(CDS.DATA_EFF_STRT_DT) =  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD'))
                                                    AND CUR_RCD_IND = 1
                            LEFT JOIN EDV.WL_CERT_CPLT_RS WLS ON ( NVL(THS.SRC_WL_CERT_CPLT_CD,-1) = NVL(WLS.DMN_VAL_ID,-1))
                where       TH.TR_H_ID in (select incr_dr_ID FROM tab_incr_dr_ID)
                    AND     nvl(trunc(THS.DATA_EFF_STRT_DT), TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) <=  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
                    AND     nvl(trunc(ADM.DATA_EFF_STRT_DT), TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
                    AND     nvl(trunc(LOC.DATA_EFF_STRT_DT), TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
                    AND     nvl(trunc(CDS.DATA_EFF_STRT_DT), TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
                    AND     nvl(trunc(WLS.DATA_EFF_STRT_DT), TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) <= TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
                    AND     TH.DURB_ID IS NOT NULL
  
        ) dm
WHERE   dm.ROW_NUM_PART = 1