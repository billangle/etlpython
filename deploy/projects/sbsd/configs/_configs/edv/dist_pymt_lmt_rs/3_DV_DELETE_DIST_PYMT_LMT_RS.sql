MERGE INTO EDV.DIST_PYMT_LMT_RS dv USING
  (SELECT DISTINCT COALESCE (DIST_REF_MRT_RS.DIST_NM,
                        '[NULL IN SOURCE]') DIST_NM,
                       SBSD_PRD.SBSD_PRD_NM SBSD_PRD_NM,
                           SBSD_PRD_ST.LOAD_DT LOAD_DT,
                           SBSD_PRD_ST.CDC_DT DATA_EFF_STRT_DT,
                           SBSD_PRD_ST.DATA_SRC_NM DATA_SRC_NM,
                           SBSD_PRD_ST.DATA_STAT_CD DATA_STAT_CD,
                           SBSD_PRD_ST.SBSD_PRD_ID SBSD_PRD_ID,
                           SBSD_PRD_ST.SBSD_PRD_ST_ID SBSD_PRD_ST_ID,
                           SBSD_PRD_ST.SCHL_PYMT_LMT_IND SCHL_PYMT_LMT_IND,
                           SBSD_PRD_ST.CRE_DT SRC_CRE_DT,
                           SBSD_PRD_ST.CRE_USER_NM SRC_CRE_USER_NM,
                           SBSD_PRD_ST.LAST_CHG_DT SRC_LAST_CHG_DT,
                           SBSD_PRD_ST.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM,
                           SBSD_PRD_ST.ST_FSA_CD ST_FSA_CD,
                           DIST_REF_MRT_RS.CTRY_DIV_ID CTRY_DIV_ID,
                           DIST_REF_MRT_RS.CTRY_DIV_CAT_ID CTRY_DIV_CAT_ID,
                           DIST_REF_MRT_RS.CTRY_DIV_REF_ID CTRY_DIV_REF_ID
   FROM SBSD_STG.SBSD_PRD_ST
   JOIN EDV.V_SBSD_PRD SBSD_PRD ON (SBSD_PRD_ST.SBSD_PRD_ID = SBSD_PRD.SBSD_PRD_ID)
   INNER JOIN
     (SELECT DIST_MRT_CD,
             DIST_NM,
             CTRY_DIV_ID,
             CTRY_DIV_CAT_ID,
             CTRY_DIV_REF_ID
      FROM
        (SELECT DIST_MRT_CD,
                DIST_NM,
                CTRY_DIV_ID,
                CTRY_DIV_CAT_ID,
                CTRY_DIV_REF_ID,
                ROW_NUMBER() OVER(PARTITION BY DIST_MRT_CD
                                  ORDER BY LOAD_END_DT DESC) AS rnum
         FROM EDV.DIST_REF_MRT_RS
         WHERE trim(lower(DIST_MRT_CD_SRC_ACRO)) = 'fsa' ) q1
      WHERE rnum=1 ) DIST_REF_MRT_RS ON (DIST_REF_MRT_RS.DIST_MRT_CD = SBSD_PRD_ST.ST_FSA_CD)
   WHERE SBSD_PRD_ST.cdc_oper_cd = 'D'
     AND date(SBSD_PRD_ST.CDC_DT) = date(TO_TIMESTAMP ('{ETL_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF'))
     AND SBSD_PRD_ST.LOAD_DT = (SELECT MAX(LOAD_DT) FROM SBSD_STG.SBSD_PRD_ST)
   ORDER BY SBSD_PRD_ST.CDC_DT) stg ON (coalesce(stg.SBSD_PRD_ID, 0) = coalesce(dv.SBSD_PRD_ID, 0)
                                        AND coalesce(stg.SBSD_PRD_ST_ID, 0) = coalesce(dv.SBSD_PRD_ST_ID, 0)
                                        AND coalesce(stg.CTRY_DIV_ID, 0) = coalesce(dv.CTRY_DIV_ID, 0)
                                        AND coalesce(stg.CTRY_DIV_CAT_ID, 0) = coalesce(dv.CTRY_DIV_CAT_ID, 0)
                                        AND coalesce(stg.CTRY_DIV_REF_ID, 0) = coalesce(dv.CTRY_DIV_REF_ID, 0)) 
WHEN MATCHED 
  AND dv.LOAD_DT <> stg.LOAD_DT
  AND dv.LOAD_END_DT = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD')
THEN
UPDATE
SET LOAD_END_DT = stg.LOAD_DT,
    DATA_EFF_END_DT = stg.DATA_EFF_STRT_DT
WHEN NOT MATCHED THEN
  INSERT (DIST_NM,
          SBSD_PRD_NM,
          LOAD_DT,
          DATA_EFF_STRT_DT,
          DATA_SRC_NM,
          DATA_STAT_CD,
          SBSD_PRD_ID,
          SBSD_PRD_ST_ID,
          SCHL_PYMT_LMT_IND,
          SRC_CRE_DT,
          SRC_CRE_USER_NM,
          SRC_LAST_CHG_DT,
          SRC_LAST_CHG_USER_NM,
          ST_FSA_CD,
          CTRY_DIV_ID,
          CTRY_DIV_CAT_ID,
          CTRY_DIV_REF_ID,
          DATA_EFF_END_DT,
          LOAD_END_DT)
  VALUES (stg.DIST_NM,
          stg.SBSD_PRD_NM,
          stg.LOAD_DT,
          stg.DATA_EFF_STRT_DT,
          stg.DATA_SRC_NM,
          stg.DATA_STAT_CD,
          stg.SBSD_PRD_ID,
          stg.SBSD_PRD_ST_ID,
          stg.SCHL_PYMT_LMT_IND,
          stg.SRC_CRE_DT,
          stg.SRC_CRE_USER_NM,
          stg.SRC_LAST_CHG_DT,
          stg.SRC_LAST_CHG_USER_NM,
          stg.ST_FSA_CD,
          stg.CTRY_DIV_ID,
          stg.CTRY_DIV_CAT_ID,
          stg.CTRY_DIV_REF_ID,
          stg.DATA_EFF_STRT_DT,
          stg.LOAD_DT)