UPDATE edv.FLD_CNTY_LS dv
SET LOAD_END_DT = stg.LOAD_DT,
    DATA_EFF_END_DT = stg.DATA_EFF_STRT_DT
FROM
  (SELECT DISTINCT MD5 (UPPER (COALESCE (TRIM (SubQry.ST_FSA_CD::varchar(32)) , '--')) || '~~' || UPPER (COALESCE (TRIM (SubQry.CNTY_FSA_CD::varchar(32)) , '--')) || '~~' || UPPER (COALESCE (TRIM (SubQry.FARM_NBR::varchar(32)) , '--')) || '~~' || COALESCE (TRIM (SubQry.TR_NBR::varchar(32)), '-1') || '~~' || UPPER (COALESCE (TRIM (SubQry.FLD_NBR::varchar(32)) , '--')) || '~~' || UPPER (COALESCE (TRIM (CNTY_REF_MRT_RS.CNTY_NM::varchar(32)) , '[NULL IN SOURCE]')) || '~~' || UPPER (COALESCE (TRIM (CNTY_REF_MRT_RS.CNTY_CAT_NM::varchar(32)) , '[NULL IN SOURCE]')) || '~~' || UPPER (TRIM (COALESCE (CNTY_REF_MRT_RS.CNTY_NM::varchar(32), SubQry.CTRY_DIV_NM::varchar(32), '[NULL IN SOURCE]')))) AS FLD_CNTY_L_ID,
                   COALESCE (SubQry.FLD_ID,
                             0) FLD_ID,
                            SubQry.LOAD_DT LOAD_DT,
                            SubQry.LOAD_END_DT LOAD_END_DT,
                            SubQry.DATA_EFF_END_DT DATA_EFF_END_DT,
                            SubQry.DATA_EFF_STRT_DT DATA_EFF_STRT_DT,
                            SubQry.DATA_SRC_NM DATA_SRC_NM,
                            MD5 (TRIM (SubQry.ST_FSA_CD::varchar(32)) || '~~' || TRIM (SubQry.CNTY_FSA_CD::varchar(32)) || '~~' || TRIM (SubQry.FARM_NBR::varchar(32)) || '~~' || TRIM (SubQry.TR_NBR::varchar(32)) || '~~' || TRIM (SubQry.FLD_NBR::varchar(32)) || '~~' || TRIM (CNTY_REF_MRT_RS.CNTY_NM::varchar(32)) || '~~' || TRIM (CNTY_REF_MRT_RS.CNTY_CAT_NM::varchar(32)) || '~~' || TRIM (COALESCE (CNTY_REF_MRT_RS.CNTY_NM::varchar(32), SubQry.CTRY_DIV_NM::varchar(32), '[NULL IN SOURCE]')) || '~~' || SubQry.FLD_ID || '~~' || TRIM (SubQry.DATA_STAT_CD::varchar(32)) || '~~' || TO_CHAR (SubQry.SRC_CRE_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TRIM (SubQry.SRC_CRE_USER_NM)::varchar(32) || '~~' || TO_CHAR (SubQry.SRC_LAST_CHG_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TRIM (SubQry.SRC_LAST_CHG_USER_NM::varchar(32)) || '~~' || TO_CHAR (SubQry.DATA_IACTV_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TRIM (SubQry.ST_ANSI_CD::varchar(32)) || '~~' || TRIM (SubQry.CNTY_ANSI_CD::varchar(32)) || '~~' || SubQry.PGM_YR::varchar(32)) AS HASH_DIF,
                            SubQry.DATA_STAT_CD DATA_STAT_CD,
                            SubQry.SRC_CRE_DT SRC_CRE_DT,
                            SubQry.SRC_CRE_USER_NM SRC_CRE_USER_NM,
                            SubQry.SRC_LAST_CHG_DT SRC_LAST_CHG_DT,
                            SubQry.SRC_LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM,
                            SubQry.DATA_IACTV_DT AS DATA_IACTV_DT,
                            SubQry.ST_ANSI_CD AS ST_ANSI_CD,
                            SubQry.CNTY_ANSI_CD AS CNTY_ANSI_CD,
                            SubQry.PGM_YR PGM_YR
   FROM
     (SELECT CASE
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
             FIELD.ST_FSA_CD AS ST_FSA_CD,
             FIELD.CNTY_FSA_CD AS CNTY_FSA_CD,
             FIELD.FARM_NBR AS FARM_NBR,
             FIELD.TR_NBR AS TR_NBR,
             FIELD.FLD_NBR AS FLD_NBR,
             FIELD.PGM_YR AS PGM_YR,
             FIELD.ST_ANSI_CD AS ST_ANSI_CD,
             FIELD.CNTY_ANSI_CD AS CNTY_ANSI_CD,
             FIELD.DATA_IACTV_DT AS DATA_IACTV_DT,
             FIELD.CRE_USER_NM AS SRC_CRE_USER_NM,
             TO_DATE ('9999-12-31',
                      'YYYY-MM-DD') DATA_EFF_END_DT,
                     TO_DATE ('9999-12-31',
                              'YYYY-MM-DD') LOAD_END_DT,
                             COALESCE (FIELD.FLD_ID,
                                       0) FLD_ID,
                                      FIELD.LOAD_DT LOAD_DT,
                                      FIELD.CDC_DT DATA_EFF_STRT_DT,
                                      FIELD.DATA_SRC_NM DATA_SRC_NM,
                                      FIELD.DATA_STAT_CD DATA_STAT_CD,
                                      FIELD.CRE_DT SRC_CRE_DT,
                                      FIELD.LAST_CHG_DT SRC_LAST_CHG_DT,
                                      FIELD.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM

      FROM CARS_STG.FIELD
      LEFT OUTER JOIN
        (SELECT ST_MRT_CD,
                ST_NM
         FROM
           (SELECT ST_RS.*,
                   ROW_NUMBER () OVER (PARTITION BY ST_MRT_CD
                                       ORDER BY LOAD_END_DT DESC) AS rnum
            FROM edv.ST_RS
            WHERE TRIM (LOWER (ST_MRT_CD_SRC_ACRO)) = 'fips' ) AS tmp1
         WHERE rnum = 1 ) ST_RS ON (ST_RS.ST_MRT_CD = FIELD.ST_ANSI_CD)
      LEFT OUTER JOIN
        (SELECT OTLY_AREA_MRT_CD,
                OTLY_AREA_NM
         FROM
           (SELECT OTLY_AREA_MRT_CD,
                   OTLY_AREA_NM,
                   ROW_NUMBER () OVER (PARTITION BY OTLY_AREA_MRT_CD
                                       ORDER BY LOAD_END_DT DESC) AS rnum
            FROM edv.OTLY_AREA_RS
            WHERE TRIM (LOWER (OTLY_AREA_MRT_CD_SRC_ACRO)) = 'fips' ) AS tmp2
         WHERE rnum = 1 ) OTLY_AREA_RS ON (OTLY_AREA_RS.OTLY_AREA_MRT_CD = FIELD.ST_ANSI_CD)
      LEFT OUTER JOIN
        (SELECT DIST_MRT_CD,
                DIST_NM
         FROM
           (SELECT DIST_MRT_CD,
                   DIST_NM,
                   ROW_NUMBER () OVER (PARTITION BY DIST_MRT_CD
                                       ORDER BY LOAD_END_DT DESC) AS rnum
            FROM edv.DIST_REF_MRT_RS
            WHERE TRIM (LOWER (DIST_MRT_CD_SRC_ACRO)) = 'fips' ) AS tmp3
         WHERE rnum = 1 ) DIST_RS ON (DIST_RS.DIST_MRT_CD = FIELD.ST_ANSI_CD)
      LEFT OUTER JOIN
        (SELECT ADM_AREA_MRT_CD,
                ADM_AREA_NM
         FROM
           (SELECT ADM_AREA_REF_MRT_RS.*,
                   ROW_NUMBER () OVER (PARTITION BY ADM_AREA_MRT_CD
                                       ORDER BY LOAD_END_DT DESC) AS rnum
            FROM edv.ADM_AREA_REF_MRT_RS
            WHERE TRIM (LOWER (ADM_AREA_MRT_CD_SRC_ACRO)) = 'fips' ) AS tmp4
         WHERE rnum = 1 ) ADM_AREA_REF_MRT_RS ON (ADM_AREA_REF_MRT_RS.ADM_AREA_MRT_CD = FIELD.ST_ANSI_CD)
      LEFT OUTER JOIN
        (SELECT MNR_OTLY_MRT_CD,
                MNR_OTLY_AREA_NM
         FROM
           (SELECT MNR_OTLY_MRT_CD,
                   MNR_OTLY_AREA_NM,
                   ROW_NUMBER () OVER (PARTITION BY MNR_OTLY_MRT_CD
                                       ORDER BY LOAD_END_DT DESC) AS rnum
            FROM edv.MNR_OTLY_AREA_RS
            WHERE TRIM (LOWER (MNR_OTLY_MRT_CD_SRC_ACRO)) = 'fips' ) AS tmp5
         WHERE rnum = 1 ) MNR_OTLY_AREA_RS ON (MNR_OTLY_AREA_RS.MNR_OTLY_MRT_CD = FIELD.ST_ANSI_CD)
      WHERE FIELD.cdc_oper_cd IN ('I',
                                  'UN')
        AND DATE (FIELD.CDC_DT) = DATE (TO_TIMESTAMP ('{ETL_START_TIMESTAMP}', 'YYYY-MM-DD HH24:MI:SS.FF'))
        AND FIELD.LOAD_DT = TO_TIMESTAMP (TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD'),
                                          'YYYY-MM-DD HH24:MI:SS.FF')
      ORDER BY FIELD.CDC_DT) SubQry
   LEFT OUTER JOIN
     (SELECT CNTY_CAT_NM,
             CNTY_NM,
             CNTY_ANSI_CD,
             CTRY_DIV_NM,
             CNTY_MRT_CD_SRC_ACRO,
             DATA_EFF_END_DT
      FROM
        (SELECT CNTY_REF_MRT_RS.*,
                ROW_NUMBER () OVER (PARTITION BY CNTY_CAT_NM,
                                                 CNTY_NM
                                    ORDER BY CNTY_ACTV_IND DESC NULLS LAST,
                                                                LOAD_END_DT DESC, SRC_LAST_CHG_DT DESC) AS rnum
         FROM edv.CNTY_REF_MRT_RS) AS tmp6
      WHERE rnum = 1 ) CNTY_REF_MRT_RS ON (CNTY_REF_MRT_RS.CTRY_DIV_NM = SubQry.CTRY_DIV_NM
                                           AND CNTY_REF_MRT_RS.CNTY_ANSI_CD = SubQry.CNTY_ANSI_CD
                                           AND TRIM (CNTY_REF_MRT_RS.CNTY_MRT_CD_SRC_ACRO) = 'FIPS'
                                           AND CNTY_REF_MRT_RS.DATA_EFF_END_DT = TO_DATE ('12/31/9999',
                                                                                          'MM/DD/YYYY'))) AS stg
WHERE COALESCE (stg.FLD_ID,
                0) = COALESCE (dv.FLD_ID,
                               0)
  AND dv.LOAD_DT <> stg.LOAD_DT
  AND dv.LOAD_END_DT = TO_TIMESTAMP ('9999-12-31',
                                     'YYYY-MM-DD')
  AND stg.HASH_DIF <> dv.HASH_DIF