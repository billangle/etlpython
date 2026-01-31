MERGE INTO EDV.ST_PYMT_LMT_RS dv USING
  (SELECT DISTINCT COALESCE (ST_RS.ST_NM,
                        '[NULL IN SOURCE]') ST_NM,
                       SBSD_PRD.SBSD_PRD_NM SBSD_PRD_NM,
                               SBSD_PRD_ST.SBSD_PRD_ST_ID  SBSD_PRD_ST_ID,
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
                               ST_RS.CTRY_DIV_ID CTRY_DIV_ID,
                               ST_RS.CTRY_DIV_CAT_ID CTRY_DIV_CAT_ID,
                               ST_RS.CTRY_DIV_REF_ID CTRY_DIV_REF_ID
   FROM
     (SELECT *
      FROM SBSD_STG.SBSD_PRD_ST
      WHERE SBSD_PRD_ST.ST_FSA_CD NOT IN
          (SELECT ADM_AREA_MRT_CD AS MRT_CD
           FROM
             (SELECT ADM_AREA_REF_MRT_RS.*,
                     ROW_NUMBER() OVER (PARTITION BY ADM_AREA_MRT_CD
                                        ORDER  BY LOAD_END_DT DESC) AS rnum
              FROM EDV.ADM_AREA_REF_MRT_RS
              WHERE trim(lower(ADM_AREA_MRT_CD_SRC_ACRO)) = 'fsa' ) q1
           WHERE rnum=1
           UNION SELECT OTLY_AREA_MRT_CD AS MRT_CD
           FROM
             (SELECT OTLY_AREA_RS.*,
                     ROW_NUMBER() OVER(PARTITION BY OTLY_AREA_MRT_CD
                                       ORDER BY LOAD_END_DT DESC) AS rnum
              FROM EDV.OTLY_AREA_RS
              WHERE trim(lower(OTLY_AREA_MRT_CD_SRC_ACRO))= 'fsa' ) q2
           WHERE rnum=1
           UNION SELECT DIST_MRT_CD AS MRT_CD
           FROM
             (SELECT DIST_REF_MRT_RS.*,
                     ROW_NUMBER() OVER(PARTITION BY DIST_MRT_CD
                                       ORDER BY LOAD_END_DT DESC) AS rnum
              FROM EDV.DIST_REF_MRT_RS
              WHERE trim(lower(DIST_MRT_CD_SRC_ACRO)) = 'fsa' ) q3
           WHERE rnum=1
           UNION SELECT MNR_OTLY_MRT_CD AS MRT_CD
           FROM
             (SELECT MNR_OTLY_AREA_RS.*,
                     ROW_NUMBER() OVER(PARTITION BY MNR_OTLY_MRT_CD
                                       ORDER BY LOAD_END_DT DESC) AS rnum
              FROM EDV.MNR_OTLY_AREA_RS
              WHERE trim(lower(MNR_OTLY_MRT_CD_SRC_ACRO))= 'fsa' ) q4
           WHERE rnum=1 ))SBSD_PRD_ST
   LEFT OUTER JOIN
     (SELECT ST_MRT_CD,
             ST_NM,
             CTRY_DIV_ID,
             CTRY_DIV_CAT_ID,
             CTRY_DIV_REF_ID
      FROM
        (SELECT ST_RS.*,
                ROW_NUMBER() OVER(PARTITION BY ST_MRT_CD
                                  ORDER BY LOAD_END_DT DESC) AS rnum
         FROM EDV.ST_RS
         WHERE trim(lower(ST_MRT_CD_SRC_ACRO)) = 'fsa' ) q5
      WHERE rnum=1 ) ST_RS ON (ST_RS.ST_MRT_CD = SBSD_PRD_ST.ST_FSA_CD)
   JOIN EDV.V_SBSD_PRD SBSD_PRD ON (SBSD_PRD_ST.SBSD_PRD_ID = SBSD_PRD.SBSD_PRD_ID)
   WHERE SBSD_PRD_ST.cdc_oper_cd <> 'D'
     AND date(SBSD_PRD_ST.CDC_DT) = date(TO_TIMESTAMP ('{ETL_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF'))
     AND SBSD_PRD_ST.LOAD_DT = (SELECT MAX(LOAD_DT) FROM SBSD_STG.SBSD_PRD_ST)
   ORDER BY SBSD_PRD_ST.CDC_DT) stg ON (coalesce(stg.SBSD_PRD_ID, 0) = coalesce(dv.SBSD_PRD_ID, 0)
                                        AND coalesce(stg.CTRY_DIV_ID, 0) = coalesce(dv.CTRY_DIV_ID, 0)
                                        AND coalesce(stg.CTRY_DIV_CAT_ID, 0) = coalesce(dv.CTRY_DIV_CAT_ID, 0)
                                        AND coalesce(stg.CTRY_DIV_REF_ID, 0) = coalesce(dv.CTRY_DIV_REF_ID, 0)) 
WHEN MATCHED
	AND dv.LOAD_DT <> stg.LOAD_DT
  AND dv.LOAD_END_DT = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD')
  AND (coalesce(stg.ST_NM, 'X') <> coalesce(dv.ST_NM, 'X')
       OR coalesce(stg.SBSD_PRD_NM, 0) <> coalesce(dv.SBSD_PRD_NM, 0)
       OR coalesce(stg.SBSD_PRD_ST_ID, 0) <> coalesce(dv.SBSD_PRD_ST_ID, 0)
       OR coalesce(stg.DATA_STAT_CD, 'X') <> coalesce(dv.DATA_STAT_CD, 'X')
       OR coalesce(stg.ST_FSA_CD, 'X') <> coalesce(dv.ST_FSA_CD, 'X')
       OR coalesce(stg.SCHL_PYMT_LMT_IND, 'X') <> coalesce(dv.SCHL_PYMT_LMT_IND, 'X'))
THEN
UPDATE
SET LOAD_END_DT = stg.LOAD_DT,
    DATA_EFF_END_DT = stg.DATA_EFF_STRT_DT