MERGE INTO EDV.SBSD_LOC_AREA_RS dv USING (
SELECT DISTINCT COALESCE(LOC_AREA_MRT_SRC_RS.LOC_AREA_NM, '[NULL IN SOURCE]') LOC_AREA_NM,
                COALESCE(LOC_AREA_MRT_SRC_RS.LOC_AREA_CAT_NM, '[NULL IN SOURCE]') LOC_AREA_CAT_NM,
                COALESCE(SubQry.CTRY_DIV_NM, '[NULL IN SOURCE]') CTRY_DIV_NM,
                SubQry.LOAD_DT LOAD_DT,
                SubQry.CDC_DT DATA_EFF_STRT_DT,
                SubQry.DATA_SRC_NM DATA_SRC_NM,
                SubQry.LAST_CHG_DT SRC_LAST_CHG_DT,
                SubQry.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM,
                SubQry.ST_CNTY_FSA_CD ST_CNTY_FSA_CD,
                SubQry.CNTY_FSA_SVC_CTR_ID CNTY_FSA_SVC_CTR_ID,
                LOC_AREA_MRT_SRC_RS.LOC_AREA_REF_ID LOC_AREA_REF_ID,
                LOC_AREA_MRT_SRC_RS.LOC_AREA_ID LOC_AREA_ID,
                LOC_AREA_MRT_SRC_RS.LOC_AREA_CAT_ID LOC_AREA_CAT_ID,
                LOC_AREA_MRT_SRC_RS.CTRY_DIV_ID CTRY_DIV_ID,
                LOC_AREA_MRT_SRC_RS.CTRY_DIV_REF_ID CTRY_DIV_REF_ID,
                TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD') DATA_EFF_END_DT,
                TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD') LOAD_END_DT
FROM (
SELECT CASE
           WHEN ST_RS.ST_NM IS NOT NULL THEN ST_RS.ST_NM
           WHEN ST_RS.ST_NM IS NULL
                AND OTLY_AREA_RS.OTLY_AREA_NM IS NOT NULL THEN OTLY_AREA_RS.OTLY_AREA_NM
           WHEN ST_RS.ST_NM IS NULL
                AND OTLY_AREA_RS.OTLY_AREA_NM IS NULL
                AND DIST_RS.DIST_NM IS NOT NULL THEN DIST_RS.DIST_NM
           WHEN ST_RS.ST_NM IS NULL
                AND OTLY_AREA_RS.OTLY_AREA_NM IS NULL
                AND DIST_RS.DIST_NM IS NULL
                AND ADM_AREA_REF_MRT_RS.ADM_AREA_NM IS NOT NULL THEN ADM_AREA_REF_MRT_RS.ADM_AREA_NM
           WHEN ST_RS.ST_NM IS NULL
                AND OTLY_AREA_RS.OTLY_AREA_NM IS NULL
                AND DIST_RS.DIST_NM IS NULL
                AND ADM_AREA_REF_MRT_RS.ADM_AREA_NM IS NULL
                AND MNR_OTLY_AREA_RS.MNR_OTLY_AREA_NM IS NOT NULL THEN MNR_OTLY_AREA_RS.MNR_OTLY_AREA_NM
           ELSE '[NULL IN SOURCE]'
       END AS CTRY_DIV_NM,
       SUBSTR(FSA_CNTY.ST_CNTY_FSA_CD, 1, 2) ST_FSA_CD,
       SUBSTR(FSA_CNTY.ST_CNTY_FSA_CD, 3, 5) FSA_CNTY_CD,
       FSA_CNTY.LOAD_DT,
       FSA_CNTY.CDC_DT,
       FSA_CNTY.DATA_SRC_NM,
       FSA_CNTY.LAST_CHG_DT,
       FSA_CNTY.LAST_CHG_USER_NM,
       FSA_CNTY.ST_CNTY_FSA_CD,
       FSA_CNTY.CNTY_FSA_SVC_CTR_ID
FROM SBSD_STG.FSA_CNTY
LEFT OUTER JOIN
  (SELECT ST_MRT_CD,
          ST_NM
   FROM
     (SELECT ST_RS.*,
             ROW_NUMBER() OVER(PARTITION BY ST_MRT_CD
                               ORDER BY LOAD_END_DT DESC) AS rnum
      FROM EDV.ST_RS
      WHERE trim(lower(ST_MRT_CD_SRC_ACRO)) = 'fsa' ) q1
   WHERE rnum=1 ) ST_RS ON (ST_RS.ST_MRT_CD = SUBSTR(ST_CNTY_FSA_CD, 1, 2))
LEFT OUTER JOIN
  (SELECT OTLY_AREA_MRT_CD,
          OTLY_AREA_NM
   FROM
     (SELECT OTLY_AREA_MRT_CD,
             OTLY_AREA_NM,
             ROW_NUMBER() OVER(PARTITION BY OTLY_AREA_MRT_CD
                               ORDER BY LOAD_END_DT DESC) AS rnum
      FROM EDV.OTLY_AREA_RS
      WHERE trim(lower(OTLY_AREA_MRT_CD_SRC_ACRO))= 'fsa' ) q2
   WHERE rnum=1 ) OTLY_AREA_RS ON (OTLY_AREA_RS.OTLY_AREA_MRT_CD = SUBSTR(ST_CNTY_FSA_CD, 1, 2))
LEFT OUTER JOIN
  (SELECT DIST_MRT_CD,
          DIST_NM
   FROM
     (SELECT DIST_MRT_CD,
             DIST_NM,
             ROW_NUMBER() OVER(PARTITION BY DIST_MRT_CD
                               ORDER BY LOAD_END_DT DESC) AS rnum
      FROM EDV.DIST_REF_MRT_RS
      WHERE trim(lower(DIST_MRT_CD_SRC_ACRO)) = 'fsa' ) q3
   WHERE rnum=1 ) DIST_RS 
   ON (DIST_RS.DIST_MRT_CD = SUBSTR(ST_CNTY_FSA_CD, 1, 2))
LEFT OUTER JOIN
  (SELECT ADM_AREA_MRT_CD,
          ADM_AREA_NM
   FROM
     (SELECT ADM_AREA_REF_MRT_RS.*,
             ROW_NUMBER() OVER(PARTITION BY ADM_AREA_MRT_CD
                               ORDER BY LOAD_END_DT DESC) AS rnum
      FROM EDV.ADM_AREA_REF_MRT_RS
      WHERE trim(lower(ADM_AREA_MRT_CD_SRC_ACRO)) = 'fsa' ) q4
   WHERE rnum=1 ) ADM_AREA_REF_MRT_RS ON (ADM_AREA_REF_MRT_RS.ADM_AREA_MRT_CD = SUBSTR(ST_CNTY_FSA_CD, 1, 2))
LEFT OUTER JOIN
  (SELECT MNR_OTLY_MRT_CD,
          MNR_OTLY_AREA_NM
   FROM
     (SELECT MNR_OTLY_MRT_CD,
             MNR_OTLY_AREA_NM,
             ROW_NUMBER() OVER(PARTITION BY MNR_OTLY_MRT_CD
                               ORDER BY LOAD_END_DT DESC) AS rnum
      FROM EDV.MNR_OTLY_AREA_RS
      WHERE trim(lower(MNR_OTLY_MRT_CD_SRC_ACRO))= 'fsa' ) q5
   WHERE rnum=1 ) MNR_OTLY_AREA_RS ON (MNR_OTLY_AREA_RS.MNR_OTLY_MRT_CD = SUBSTR(ST_CNTY_FSA_CD, 1, 2))
WHERE FSA_CNTY.CDC_OPER_CD <>'D'
  AND date(FSA_CNTY.CDC_DT) = date(TO_TIMESTAMP ('{ETL_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF'))
  AND FSA_CNTY.LOAD_DT = (SELECT MAX(LOAD_DT) FROM SBSD_STG.FSA_CNTY) ) SubQry
  JOIN
    (SELECT LOC_AREA_MRT_CD,
            LOC_AREA_NM,
            LOC_AREA_CAT_NM,
            CTRY_DIV_NM,
            LOC_AREA_REF_ID,
            LOC_AREA_ID,
            LOC_AREA_CAT_ID,
            CTRY_DIV_ID,
            CTRY_DIV_REF_ID
     FROM
       (SELECT LOC_AREA_MRT_SRC_RS.*,
               ROW_NUMBER() OVER(PARTITION BY LOC_AREA_MRT_CD, LOC_AREA_NM, LOC_AREA_CAT_NM, CTRY_DIV_NM
                                 ORDER BY LOAD_END_DT DESC) AS rnum
        FROM EDV.LOC_AREA_MRT_SRC_RS
        WHERE trim(lower(LOC_AREA_MRT_CD_SRC_ACRO))= 'fsa' ) q6
     WHERE rnum=1 ) LOC_AREA_MRT_SRC_RS ON (LOC_AREA_MRT_SRC_RS.CTRY_DIV_NM=SubQry.CTRY_DIV_NM
                                            AND LOC_AREA_MRT_SRC_RS.LOC_AREA_MRT_CD=SubQry.FSA_CNTY_CD) ) stg ON (coalesce(stg.LOC_AREA_CAT_ID, 0) = coalesce(dv.LOC_AREA_CAT_ID, 0)
                                                                                                                  AND coalesce(stg.CNTY_FSA_SVC_CTR_ID, 0) = coalesce(dv.CNTY_FSA_SVC_CTR_ID, 0)
                                                                                                                  AND coalesce(stg.LOC_AREA_REF_ID, 0) = coalesce(dv.LOC_AREA_REF_ID, 0)
                                                                                                                  AND coalesce(stg.CTRY_DIV_ID, 0) = coalesce(dv.CTRY_DIV_ID, 0)
                                                                                                                  AND coalesce(stg.LOC_AREA_ID, 0) = coalesce(dv.LOC_AREA_ID, 0)
                                                                                                                  AND coalesce(stg.CTRY_DIV_REF_ID, 0) = coalesce(dv.CTRY_DIV_REF_ID, 0)) 
WHEN MATCHED
	AND dv.LOAD_DT <> stg.LOAD_DT
  AND dv.LOAD_END_DT = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD')
  AND (coalesce(stg.LOC_AREA_CAT_NM, 'X')<>coalesce(dv.LOC_AREA_CAT_NM, 'X')
       OR coalesce(stg.LOC_AREA_NM, 'X')<>coalesce(dv.LOC_AREA_NM, 'X')
       OR coalesce(stg.ST_CNTY_FSA_CD, 'X')<>coalesce(dv.ST_CNTY_FSA_CD, 'X')
       OR coalesce(stg.CTRY_DIV_NM, 'X')<>coalesce(dv.CTRY_DIV_NM, 'X'))
THEN
  UPDATE
  SET LOAD_END_DT = stg.LOAD_DT,
      DATA_EFF_END_DT = stg.DATA_EFF_STRT_DT 