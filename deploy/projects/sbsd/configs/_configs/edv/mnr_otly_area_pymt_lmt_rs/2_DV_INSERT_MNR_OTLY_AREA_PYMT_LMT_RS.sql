INSERT INTO EDV.MNR_OTLY_AREA_PYMT_LMT_RS (MNR_OTLY_AREA_NM, SBSD_PRD_NM, LOAD_DT, DATA_EFF_STRT_DT, DATA_SRC_NM, DATA_STAT_CD, SRC_CRE_DT, SRC_CRE_USER_NM, SRC_LAST_CHG_DT, SRC_LAST_CHG_USER_NM, ST_FSA_CD, SCHL_PYMT_LMT_IND, SBSD_PRD_ID, SBSD_PRD_ST_ID, CTRY_DIV_ID, CTRY_DIV_CAT_ID, CTRY_DIV_REF_ID, DATA_EFF_END_DT, LOAD_END_DT)
  (SELECT stg.*
   FROM
     (SELECT DISTINCT COALESCE (MNR_OTLY_AREA_RS.MNR_OTLY_AREA_NM,
                           '[NULL IN SOURCE]') MNR_OTLY_AREA_NM,
                          SBSD_PRD.SBSD_PRD_NM SBSD_PRD_NM,
                              SBSD_PRD_ST.LOAD_DT LOAD_DT,
                              SBSD_PRD_ST.CDC_DT DATA_EFF_STRT_DT,
                              SBSD_PRD_ST.DATA_SRC_NM DATA_SRC_NM,
                              SBSD_PRD_ST.DATA_STAT_CD DATA_STAT_CD,
                              SBSD_PRD_ST.CRE_DT SRC_CRE_DT,
                              SBSD_PRD_ST.CRE_USER_NM SRC_CRE_USER_NM,
                              SBSD_PRD_ST.LAST_CHG_DT SRC_LAST_CHG_DT,
                              SBSD_PRD_ST.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM,
                              SBSD_PRD_ST.ST_FSA_CD ST_FSA_CD,
                              SBSD_PRD_ST.SCHL_PYMT_LMT_IND SCHL_PYMT_LMT_IND,
                              SBSD_PRD_ST.SBSD_PRD_ID SBSD_PRD_ID,
                              SBSD_PRD_ST.SBSD_PRD_ST_ID SBSD_PRD_ST_ID,
                              MNR_OTLY_AREA_RS.CTRY_DIV_ID CTRY_DIV_ID,
                              MNR_OTLY_AREA_RS.CTRY_DIV_CAT_ID CTRY_DIV_CAT_ID,
                              MNR_OTLY_AREA_RS.CTRY_DIV_REF_ID CTRY_DIV_REF_ID,
                              TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD') DATA_EFF_END_DT,
                              TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD') LOAD_END_DT
      FROM SBSD_STG.SBSD_PRD_ST
      JOIN EDV.V_SBSD_PRD SBSD_PRD ON (SBSD_PRD_ST.SBSD_PRD_ID = SBSD_PRD.SBSD_PRD_ID)
      INNER JOIN
        (SELECT MNR_OTLY_MRT_CD,
                MNR_OTLY_AREA_NM,
                CTRY_DIV_ID,
                CTRY_DIV_CAT_ID,
                CTRY_DIV_REF_ID
         FROM
           (SELECT MNR_OTLY_MRT_CD,
                   MNR_OTLY_AREA_NM,
                   CTRY_DIV_ID,
                   CTRY_DIV_CAT_ID,
                   CTRY_DIV_REF_ID,
                   ROW_NUMBER() OVER(PARTITION BY MNR_OTLY_MRT_CD
                                     ORDER BY LOAD_END_DT DESC) AS rnum
            FROM EDV.MNR_OTLY_AREA_RS
            WHERE trim(lower(MNR_OTLY_MRT_CD_SRC_ACRO))= 'fsa' ) q1
         WHERE rnum=1 ) MNR_OTLY_AREA_RS ON (MNR_OTLY_AREA_RS.MNR_OTLY_MRT_CD = SBSD_PRD_ST.ST_FSA_CD)
      WHERE SBSD_PRD_ST.cdc_oper_cd <> 'D'
        AND date(SBSD_PRD_ST.CDC_DT) = date(TO_TIMESTAMP ('{ETL_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF'))
        AND SBSD_PRD_ST.LOAD_DT = (SELECT MAX(LOAD_DT) FROM SBSD_STG.SBSD_PRD_ST)
      ORDER BY SBSD_PRD_ST.CDC_DT) stg
   LEFT JOIN EDV.MNR_OTLY_AREA_PYMT_LMT_RS dv ON (coalesce(stg.MNR_OTLY_AREA_NM, 'X') = coalesce(dv.MNR_OTLY_AREA_NM, 'X')
                                                  AND coalesce(stg.SBSD_PRD_NM, 0) = coalesce(dv.SBSD_PRD_NM, 0)
                                                  AND coalesce(stg.DATA_STAT_CD, 'X') = coalesce(dv.DATA_STAT_CD, 'X')
                                                  AND coalesce(stg.ST_FSA_CD, 'X') = coalesce(dv.ST_FSA_CD, 'X')
                                                  AND coalesce(stg.SCHL_PYMT_LMT_IND, 'X') = coalesce(dv.SCHL_PYMT_LMT_IND, 'X')
                                                  AND coalesce(stg.SBSD_PRD_ID, 0) = coalesce(dv.SBSD_PRD_ID, 0)
                                                  AND coalesce(stg.SBSD_PRD_ST_ID, 0) = coalesce(dv.SBSD_PRD_ST_ID, 0)
                                                  AND coalesce(stg.CTRY_DIV_ID, 0) = coalesce(dv.CTRY_DIV_ID, 0)
                                                  AND coalesce(stg.CTRY_DIV_CAT_ID, 0) = coalesce(dv.CTRY_DIV_CAT_ID, 0)
                                                  AND coalesce(stg.CTRY_DIV_REF_ID, 0) = coalesce(dv.CTRY_DIV_REF_ID, 0)
                                                  AND dv.LOAD_END_DT = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD'))
   WHERE dv.MNR_OTLY_AREA_NM IS NULL
     OR dv.SBSD_PRD_NM IS NULL )