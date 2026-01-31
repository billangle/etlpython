with upsert_data as(
SELECT
     PGM_YR
    ,ADM_FSA_ST_CNTY_SRGT_ID
    ,ADM_FSA_ST_CNTY_DURB_ID
    ,PRNT_TR_SRGT_ID
    ,PRNT_TR_DURB_ID
    ,RSLT_TR_SRGT_ID
    ,RSLT_TR_DURB_ID
    ,RCON_TYPE_SRGT_ID
    ,RCON_TYPE_DURB_ID
    ,RCON_APVL_DT
    ,RCON_EFF_DT
    ,RCON_INIT_DT
    ,TR_RCON_CRE_DT
    ,TR_RCON_CRE_USER_NM
    ,TR_RCON_LAST_CHG_DT
    ,TR_RCON_LAST_CHG_USER_NM
    ,SRC_DATA_STAT_CD
    ,RCON_SEQ_NBR
    ,DATA_EFF_STRT_DT
FROM (
SELECT DISTINCT TRLS.PGM_YR PGM_YR
    ,coalesce(FSD_ADM.FSA_ST_CNTY_SRGT_ID, -3) ADM_FSA_ST_CNTY_SRGT_ID
    ,coalesce(FSD_ADM.FSA_ST_CNTY_DURB_ID, -3) ADM_FSA_ST_CNTY_DURB_ID
    ,coalesce(TD_PRNT.TR_SRGT_ID, -3) PRNT_TR_SRGT_ID
    ,TD_PRNT.TR_DURB_ID PRNT_TR_DURB_ID
    ,coalesce(TD_RSLT.TR_SRGT_ID, -3) RSLT_TR_SRGT_ID
    ,TD_RSLT.TR_DURB_ID RSLT_TR_DURB_ID
    ,coalesce(RD.RCON_TYPE_SRGT_ID, -3) AS RCON_TYPE_SRGT_ID
    ,coalesce(RD.RCON_TYPE_DURB_ID, -3) AS RCON_TYPE_DURB_ID
    ,coalesce(cast(TO_CHAR(RR.RCON_APVL_DT, 'YYYYMMDD')as integer), -1) AS RCON_APVL_DT
    ,coalesce(cast(TO_CHAR(RR.RCON_EFF_DT, 'YYYYMMDD')as integer), -1) AS RCON_EFF_DT
    ,coalesce(cast(TO_CHAR(RR.RCON_INIT_DT, 'YYYYMMDD') as integer), -1) AS RCON_INIT_DT
    ,TRLS.SRC_CRE_DT TR_RCON_CRE_DT
    ,'-1' TR_RCON_CRE_USER_NM
    ,TRLS.SRC_LAST_CHG_DT TR_RCON_LAST_CHG_DT
    ,TRLS.SRC_LAST_CHG_USER_NM TR_RCON_LAST_CHG_USER_NM
    ,TRLS.DATA_STAT_CD SRC_DATA_STAT_CD
    ,RR.RCON_SEQ_NBR RCON_SEQ_NBR
     ,GREATEST(coalesce(TRLS.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')), 
        coalesce(LAS_ADM.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')),
        coalesce(RR.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')),
        coalesce(FCS.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')) ) DATA_EFF_STRT_DT,
      TRLS.DATA_EFF_STRT_DT TRLS_DATA_EFF_STRT_DT,
      LAS_ADM.DATA_EFF_STRT_DT LAS_ADM_DATA_EFF_STRT_DT,  
       RR.DATA_EFF_STRT_DT RR_DATA_EFF_STRT_DT,
       FCS.DATA_EFF_STRT_DT FCS_DATA_EFF_STRT_DT,
       ROW_NUMBER() OVER (PARTITION BY TRLS.PGM_YR,TRH_PRNT.DURB_ID,TRH_RSLT.DURB_ID ORDER BY TRLS.DATA_EFF_STRT_DT DESC,LAS_ADM.DATA_EFF_STRT_DT DESC, RR.DATA_EFF_STRT_DT DESC ) AS  ROW_NUM_PART       
 FROM EDV.TR_RCON_LS TRLS
 LEFT JOIN EDV.TR_RCON_L TRL ON (coalesce(TRLS.TR_RCON_L_ID,'6bb61e3b7bce0931da574d19d1d82c88') = TRL.TR_RCON_L_ID)
 LEFT JOIN EDV.RCON_RS RR ON (TRLS.RCON_ID = RR.RCON_ID)
 LEFT JOIN EDV.FARM_CHG_TYPE_RS FCS ON (RR.SRC_RCON_TYPE_CD = FCS.FARM_CHG_TYPE_CD)
 LEFT JOIN EDV.FARM_CHG_TYPE_RH FCH ON (coalesce(FCS.FARM_CHG_TYPE_CD,'[NULL IN SOURCE]') = FCH.FARM_CHG_TYPE_CD)
 LEFT JOIN FARM_DM_STG.RCON_TYPE_DIM RD ON (FCH.DURB_ID = coalesce(RD.RCON_TYPE_DURB_ID,-1))
 LEFT JOIN EDV.LOC_AREA_MRT_SRC_RS LAS_ADM ON (
        RR.ST_FSA_CD = LAS_ADM.CTRY_DIV_MRT_CD
        AND RR.CNTY_FSA_CD = LAS_ADM.LOC_AREA_MRT_CD
       AND LAS_ADM.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'   )
 LEFT JOIN EDV.LOC_AREA_RH LAH_ADM ON (
        coalesce(LAS_ADM.LOC_AREA_CAT_NM,'[NULL IN SOURCE]') = LAH_ADM.LOC_AREA_CAT_NM
        AND coalesce(LAS_ADM.LOC_AREA_NM,'[NULL IN SOURCE]') = LAH_ADM.LOC_AREA_NM
        AND coalesce(LAS_ADM.CTRY_DIV_NM,'[NULL IN SOURCE]') = LAH_ADM.CTRY_DIV_NM
        AND LAS_ADM.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'
        )
 LEFT JOIN CMN_DIM_DM_STG.FSA_ST_CNTY_DIM FSD_ADM ON (
        LAH_ADM.DURB_ID = coalesce(FSD_ADM.FSA_ST_CNTY_DURB_ID,-1)
        AND FSD_ADM.CUR_RCD_IND = 1
        )
 LEFT JOIN EDV.TR_H TRH_PRNT ON (coalesce(TRL.PRNT_TR_H_ID,'6de912369edfb89b50859d8f305e4f72') = TRH_PRNT.TR_H_ID)
 LEFT JOIN CMN_DIM_DM_STG.TR_DIM TD_PRNT ON (
        TRH_PRNT.DURB_ID = coalesce(TD_PRNT.TR_DURB_ID,-1)
        AND TD_PRNT.CUR_RCD_IND = 1
        )
 LEFT JOIN EDV.TR_H TRH_RSLT ON (coalesce(TRL.RSLT_TR_H_ID,'6de912369edfb89b50859d8f305e4f72') = TRH_RSLT.TR_H_ID)
 LEFT JOIN CMN_DIM_DM_STG.TR_DIM TD_RSLT ON (
        TRH_RSLT.DURB_ID = coalesce(TD_RSLT.TR_DURB_ID,-1)
        AND TD_RSLT.CUR_RCD_IND = 1
        )
 WHERE 
  (
 (
 cast(TRLS.DATA_EFF_STRT_DT as date) =  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 AND cast(TRLS.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 )
 OR
 (
 cast(RR.DATA_EFF_STRT_DT as date) =  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 OR cast(RR.DATA_EFF_END_DT as date) = TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 )
 OR
 (
 cast(LAS_ADM.DATA_EFF_STRT_DT as date) =  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 OR cast(LAS_ADM.DATA_EFF_END_DT as date) = TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 )
 OR
 (
 cast(FCS.DATA_EFF_STRT_DT as date) =  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 OR cast(FCS.DATA_EFF_END_DT as date) = TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 )
 )
 AND cast(TRLS.DATA_EFF_STRT_DT as date) <=  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 AND cast(RR.DATA_EFF_STRT_DT as date)  <=  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 AND cast(LAS_ADM.DATA_EFF_STRT_DT as date)  <=  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 AND cast(FCS.DATA_EFF_STRT_DT as date)  <=  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 AND TRLS.PGM_YR IS NOT NULL
 AND TD_PRNT.TR_DURB_ID IS NOT NULL
 AND TD_RSLT.TR_DURB_ID IS NOT NULL 
 )DM
 WHERE DM.ROW_NUM_PART = 1
 
UNION

SELECT PGM_YR
    ,ADM_FSA_ST_CNTY_SRGT_ID
    ,ADM_FSA_ST_CNTY_DURB_ID
    ,PRNT_TR_SRGT_ID
    ,PRNT_TR_DURB_ID
    ,RSLT_TR_SRGT_ID
    ,RSLT_TR_DURB_ID
    ,RCON_TYPE_SRGT_ID
    ,RCON_TYPE_DURB_ID
    ,RCON_APVL_DT
    ,RCON_EFF_DT
    ,RCON_INIT_DT
    ,TR_RCON_CRE_DT
    ,TR_RCON_CRE_USER_NM
    ,TR_RCON_LAST_CHG_DT
    ,TR_RCON_LAST_CHG_USER_NM
    ,SRC_DATA_STAT_CD
    ,RCON_SEQ_NBR
    ,DATA_EFF_STRT_DT
FROM (
 SELECT DISTINCT 
    cast(coalesce(TRLS.TM_PRD_NM, (FIRST_VALUE(FTLS.PGM_YR) OVER (PARTITION BY FTLS.TR_ID ORDER BY FTLS.PGM_YR ASC))::VARCHAR) as numeric) PGM_YR
    ,coalesce(FSD_ADM.FSA_ST_CNTY_SRGT_ID, -3) ADM_FSA_ST_CNTY_SRGT_ID
    ,coalesce(FSD_ADM.FSA_ST_CNTY_DURB_ID, -3) ADM_FSA_ST_CNTY_DURB_ID
    ,coalesce(TD_PRNT.TR_SRGT_ID, -3) PRNT_TR_SRGT_ID
    ,TD_PRNT.TR_DURB_ID PRNT_TR_DURB_ID
    ,coalesce(TD_RSLT.TR_SRGT_ID, -3) RSLT_TR_SRGT_ID
    ,TD_RSLT.TR_DURB_ID RSLT_TR_DURB_ID
    ,coalesce(RD.RCON_TYPE_SRGT_ID, -3) AS RCON_TYPE_SRGT_ID
    ,coalesce(RD.RCON_TYPE_DURB_ID, -3) AS RCON_TYPE_DURB_ID
    ,coalesce(cast(TO_CHAR(TRLS.RCON_APVL_DT, 'YYYYMMDD')as integer), -1) AS RCON_APVL_DT
    ,-1 AS RCON_EFF_DT
    ,coalesce(cast(TO_CHAR(TRLS.RCON_INIT_DT, 'YYYYMMDD')as integer), -1) AS RCON_INIT_DT
    ,TRLS.SRC_CRE_DT TR_RCON_CRE_DT
    ,TRLS.SRC_CRE_USER_NM TR_RCON_CRE_USER_NM
    ,TRLS.SRC_LAST_CHG_DT TR_RCON_LAST_CHG_DT
    ,TRLS.SRC_LAST_CHG_USER_NM TR_RCON_LAST_CHG_USER_NM
    ,TRLS.DATA_STAT_CD SRC_DATA_STAT_CD
    ,TRLS.RCON_SEQ_NBR RCON_SEQ_NBR
    ,GREATEST(coalesce(TRLS.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')), 
        coalesce(LAS_ADM.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')),
        coalesce(FTLS.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD'))  ) DATA_EFF_STRT_DT,
        TRLS.DATA_EFF_STRT_DT TRLS_DATA_EFF_STRT_DT  ,
        FTLS.DATA_EFF_STRT_DT  FTLS_DATA_EFF_STRT_DT,
        LAS_ADM.DATA_EFF_STRT_DT  LAS_ADM_DATA_EFF_STRT_DT,
ROW_NUMBER() OVER (PARTITION BY FTLS.PGM_YR,TRH_PRNT.DURB_ID,TRH_RSLT.DURB_ID ORDER BY TRLS.DATA_EFF_STRT_DT DESC,FTLS.DATA_EFF_STRT_DT DESC,
LAS_ADM.DATA_EFF_STRT_DT DESC ) AS  ROW_NUM_PART
 FROM EDV.MIDAS_TR_RCON_LS TRLS
 LEFT JOIN EDV.TR_RCON_L TRL ON (coalesce(TRLS.TR_RCON_L_ID,'6bb61e3b7bce0931da574d19d1d82c88') = TRL.TR_RCON_L_ID)
 LEFT JOIN EDV.FARM_TR_L FTL ON ( TRL.RSLT_TR_H_ID = coalesce(FTL.TR_H_ID,'6de912369edfb89b50859d8f305e4f72'))
 LEFT JOIN EDV.FARM_TR_YR_LS FTLS ON (
        FTL.FARM_TR_L_ID = FTLS.FARM_TR_L_ID
        )
 LEFT JOIN EDV.LOC_AREA_MRT_SRC_RS LAS_ADM ON (
        TRLS.RSLT_ST_FSA_CD = LAS_ADM.CTRY_DIV_MRT_CD
        AND TRLS.RSLT_CNTY_FSA_CD = LAS_ADM.LOC_AREA_MRT_CD
        AND LAS_ADM.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'
        )
 LEFT JOIN EDV.LOC_AREA_RH LAH_ADM ON (
        coalesce(LAS_ADM.LOC_AREA_CAT_NM,'[NULL IN SOURCE]') = LAH_ADM.LOC_AREA_CAT_NM
        AND coalesce(LAS_ADM.LOC_AREA_NM,'[NULL IN SOURCE]') = LAH_ADM.LOC_AREA_NM
        AND coalesce(LAS_ADM.CTRY_DIV_NM,'[NULL IN SOURCE]') = LAH_ADM.CTRY_DIV_NM
        AND LAS_ADM.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'
        )
 LEFT JOIN CMN_DIM_DM_STG.FSA_ST_CNTY_DIM FSD_ADM ON (
        LAH_ADM.DURB_ID = coalesce(FSD_ADM.FSA_ST_CNTY_DURB_ID,-1)
        AND FSD_ADM.CUR_RCD_IND = 1
        )
 LEFT JOIN EDV.TR_H TRH_PRNT ON (
        coalesce(TRLS.PRNT_ST_FSA_CD,'--') = TRH_PRNT.ST_FSA_CD
        AND coalesce(TRLS.PRNT_CNTY_FSA_CD,'--') = TRH_PRNT.CNTY_FSA_CD
        AND coalesce(TRLS.PRNT_TR_NBR::INTEGER,-1) = TRH_PRNT.TR_NBR
        )
 LEFT JOIN CMN_DIM_DM_STG.TR_DIM TD_PRNT ON (
        TRH_PRNT.DURB_ID = coalesce(TD_PRNT.TR_DURB_ID,-1)
        AND TD_PRNT.CUR_RCD_IND = 1
        )
 LEFT JOIN (
    SELECT TR_RCON_L_ID
        ,RCON_TYP_CD
    FROM (
        SELECT CASE 
                WHEN L1.TR_RCON_L_ID IS NOT NULL
                    THEN L1.TR_RCON_L_ID
                WHEN L2.TR_RCON_L_ID IS NOT NULL
                    THEN L2.TR_RCON_L_ID
                END TR_RCON_L_ID
            ,CASE 
                WHEN RCON_TYP_CD_T = 'T'
                    AND RCON_TYP_CD_M = 'M'
                    THEN 'I'
                WHEN RCON_TYP_CD_T = 'T'
                    AND RCON_TYP_CD_M IS NULL
                    THEN 'T'
                WHEN RCON_TYP_CD_T IS NULL
                    AND RCON_TYP_CD_M = 'M'
                    THEN 'M'
                ELSE ''
                END RCON_TYP_CD
        FROM  (
            SELECT LID1.TR_RCON_L_ID
                ,LID2.RCON_TYP_CD_T
            FROM EDV.MIDAS_TR_RCON_LS LID1
            INNER JOIN (
                SELECT DISTINCT PRNT_ST_FSA_CD
                    ,PRNT_CNTY_FSA_CD
                    ,PRNT_TR_NBR
                    ,CASE 
                        WHEN COUNT(CONCAT (
                                    CONCAT (
                                        RSLT_ST_FSA_CD
                                        ,RSLT_CNTY_FSA_CD
                                        )
                                    ,RSLT_TR_NBR
                                    )) > 1
                            THEN 'T'
                        END AS RCON_TYP_CD_T
                FROM EDV.MIDAS_TR_RCON_LS
                WHERE DATA_EFF_END_DT = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD')
                GROUP BY PRNT_ST_FSA_CD
                    ,PRNT_CNTY_FSA_CD
                    ,PRNT_TR_NBR
                ) LID2 ON (
                    LID1.PRNT_ST_FSA_CD = LID2.PRNT_ST_FSA_CD
                    AND LID1.PRNT_CNTY_FSA_CD = LID2.PRNT_CNTY_FSA_CD
                    AND LID1.PRNT_TR_NBR = LID2.PRNT_TR_NBR
                    )
            ) L1
        FULL JOIN (
            SELECT LID1.TR_RCON_L_ID
                ,LID2.RCON_TYP_CD_M
            FROM EDV.MIDAS_TR_RCON_LS LID1
            INNER JOIN (
                SELECT DISTINCT RSLT_ST_FSA_CD
                    ,RSLT_CNTY_FSA_CD
                    ,RSLT_TR_NBR
                    ,CASE 
                        WHEN COUNT(CONCAT (
                                    CONCAT (
                                        PRNT_ST_FSA_CD
                                        ,PRNT_CNTY_FSA_CD
                                        )
                                    ,PRNT_TR_NBR
                                    )) > 1
                            THEN 'M'
                        END AS RCON_TYP_CD_M
                FROM EDV.MIDAS_TR_RCON_LS
                WHERE DATA_EFF_END_DT = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD')
                GROUP BY RSLT_ST_FSA_CD
                    ,RSLT_CNTY_FSA_CD
                    ,RSLT_TR_NBR
                ) LID2 ON (
                    LID1.RSLT_ST_FSA_CD = LID2.RSLT_ST_FSA_CD
                    AND LID1.RSLT_CNTY_FSA_CD = LID2.RSLT_CNTY_FSA_CD
                    AND LID1.RSLT_TR_NBR = LID2.RSLT_TR_NBR
                    )
            ) L2 ON L1.TR_RCON_L_ID = L2.TR_RCON_L_ID
        ) sub
    ) RT ON (RT.TR_RCON_L_ID = TRLS.TR_RCON_L_ID)
 LEFT JOIN FARM_DM_STG.RCON_TYPE_DIM RD ON (RD.RCON_TYPE_CD = RT.RCON_TYP_CD)
 LEFT JOIN EDV.TR_H TRH_RSLT ON (
        coalesce(TRLS.RSLT_ST_FSA_CD,'--') = TRH_RSLT.ST_FSA_CD
        AND coalesce(TRLS.RSLT_CNTY_FSA_CD,'--') = TRH_RSLT.CNTY_FSA_CD
        AND coalesce(TRLS.RSLT_TR_NBR::INTEGER,-1) = TRH_RSLT.TR_NBR
        )
 LEFT JOIN CMN_DIM_DM_STG.TR_DIM TD_RSLT ON (
        TRH_RSLT.DURB_ID = coalesce(TD_RSLT.TR_DURB_ID,-1)
        AND TD_RSLT.CUR_RCD_IND = 1
        )
 WHERE      
 (
 (
 cast(TRLS.DATA_EFF_STRT_DT as date) =  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 AND cast(TRLS.DATA_EFF_END_DT as date) > TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 )
 OR
 (
 cast(FTLS.DATA_EFF_STRT_DT as date) =  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 AND cast(FTLS.DATA_EFF_END_DT as date) = TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 )
 OR
 (
 cast(LAS_ADM.DATA_EFF_STRT_DT as date) =  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 OR cast(LAS_ADM.DATA_EFF_END_DT as date) = TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 )
 )
 AND cast(TRLS.DATA_EFF_STRT_DT as date) <=  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 AND cast(FTLS.DATA_EFF_STRT_DT as date)  <=  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 AND cast(LAS_ADM.DATA_EFF_STRT_DT as date)  <=  TO_TIMESTAMP('{V_CDC_DT}','YYYY-MM-DD')
 AND FTLS.PGM_YR IS NOT NULL
 AND TD_PRNT.TR_DURB_ID IS NOT NULL
 AND TD_RSLT.TR_DURB_ID IS NOT NULL
 )DM
 WHERE DM.ROW_NUM_PART = 1
 )

select  (current_date) as cre_dt,
		(current_date) as last_chg_dt,
		'A' as data_stat_cd,
		pgm_yr,
		adm_fsa_st_cnty_srgt_id,
		adm_fsa_st_cnty_durb_id,
		prnt_tr_srgt_id,
		prnt_tr_durb_id,
		rslt_tr_srgt_id,
		rslt_tr_durb_id,
		rcon_type_srgt_id,
		rcon_type_durb_id,
		rcon_apvl_dt,
		rcon_eff_dt,
		rcon_init_dt,
		tr_rcon_cre_dt,
		tr_rcon_cre_user_nm,
		tr_rcon_last_chg_dt,
		tr_rcon_last_chg_user_nm,
		src_data_stat_cd,
		rcon_seq_nbr
from upsert_data tab_incr 
where adm_fsa_st_cnty_srgt_id > 0
order by adm_fsa_st_cnty_srgt_id asc 

					