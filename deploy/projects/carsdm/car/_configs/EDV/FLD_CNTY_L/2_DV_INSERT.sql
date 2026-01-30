INSERT INTO edv.FLD_CNTY_L (FLD_CNTY_L_ID, LOAD_DT, DATA_SRC_NM, FLD_H_ID, CTRY_DIV_NM, CNTY_NM, CNTY_CAT_NM)
  (SELECT temp.FLD_CNTY_L_ID,
          temp.LOAD_DT,
          temp.DATA_SRC_NM,
          temp.FLD_H_ID,
          temp.CTRY_DIV_NM,
          temp.CNTY_NM,
          temp.CNTY_CAT_NM
   FROM
     (SELECT MD5 (UPPER (TRIM (stg.ST_FSA_CD)) || '~~' || UPPER (TRIM (stg.CNTY_FSA_CD)) || '~~' || UPPER (TRIM (stg.FARM_NBR)) || '~~' || TRIM (stg.TR_NBR) || '~~' || UPPER (TRIM (stg.FLD_NBR)) || '~~' || UPPER (TRIM (stg.CNTY_NM)) || '~~' || UPPER (TRIM (stg.CNTY_CAT_NM)) || '~~' || UPPER (TRIM (stg.CTRY_DIV_NM))) AS FLD_CNTY_L_ID,
             stg.LOAD_DT,
             stg.DATA_SRC_NM,
             MD5 (UPPER (TRIM (stg.ST_FSA_CD)) || '~~' || UPPER (TRIM (stg.CNTY_FSA_CD)) || '~~' || UPPER (TRIM (stg.FARM_NBR)) || '~~' || stg.TR_NBR || '~~' || UPPER (TRIM (stg.FLD_NBR))) AS FLD_H_ID,
             stg.CTRY_DIV_NM,
             stg.CNTY_NM,
             stg.CNTY_CAT_NM,
             stg.STG_EFF_DT_RANK
      FROM
        (SELECT DISTINCT CAST(COALESCE (CNTY_REF_MRT_RS.CNTY_CAT_NM, '[NULL IN SOURCE]') AS VARCHAR(30)) CNTY_CAT_NM,
                         CAST(COALESCE (CNTY_REF_MRT_RS.CNTY_NM, '[NULL IN SOURCE]') AS VARCHAR(30)) CNTY_NM,
                         CAST(COALESCE (CNTY_REF_MRT_RS.CNTY_NM, SubQry.CTRY_DIV_NM, '[NULL IN SOURCE]') AS VARCHAR(30)) CTRY_DIV_NM,
                         SubQry.CNTY_ANSI_CD AS CNTY_ANSI_CD,
                         CAST(COALESCE (SubQry.ST_FSA_CD, '--') AS VARCHAR(30)) AS ST_FSA_CD,
                         CAST(COALESCE (SubQry.CNTY_FSA_CD, '--') AS VARCHAR(30)) AS CNTY_FSA_CD,
                         CAST(COALESCE (SubQry.FARM_NBR, '--') AS VARCHAR(30)) AS FARM_NBR,
                         CAST(COALESCE (SubQry.TR_NBR, -1) AS VARCHAR(30)) AS TR_NBR,
                         CAST(COALESCE (SubQry.FLD_NBR,'--') AS VARCHAR(30)) AS FLD_NBR,
                         SubQry.LOAD_DT AS LOAD_DT,
                         SubQry.CDC_DT AS CDC_DT,
                         SubQry.DATA_SRC_NM AS DATA_SRC_NM,
                         ROW_NUMBER () OVER (PARTITION BY COALESCE (CNTY_REF_MRT_RS.CNTY_CAT_NM, '[NULL IN SOURCE]') ,
                                                          COALESCE (CNTY_REF_MRT_RS.CNTY_NM,'[NULL IN SOURCE]') ,
                                                          COALESCE (CNTY_REF_MRT_RS.CNTY_NM, SubQry.CTRY_DIV_NM, '[NULL IN SOURCE]') , 
                                                          COALESCE (SubQry.ST_FSA_CD, '--') , 
                                                          COALESCE (SubQry.CNTY_FSA_CD, '--') , 
                                                          COALESCE (SubQry.FARM_NBR,'--') , 
                                                          COALESCE (SubQry.TR_NBR, -1) ,
                                                          COALESCE (SubQry.FLD_NBR, '--')
                                                  ORDER BY SubQry.CDC_DT DESC, SubQry.LOAD_DT DESC) STG_EFF_DT_RANK
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
                   FIELD.CNTY_ANSI_CD AS CNTY_ANSI_CD,
                   FIELD.ST_FSA_CD AS ST_FSA_CD,
                   FIELD.CNTY_FSA_CD AS CNTY_FSA_CD,
                   FIELD.FARM_NBR AS FARM_NBR,
                   FIELD.TR_NBR AS TR_NBR,
                   FIELD.FLD_NBR AS FLD_NBR,
                   FIELD.LOAD_DT AS LOAD_DT,
                   FIELD.CDC_DT AS CDC_DT,
                   FIELD.DATA_SRC_NM AS DATA_SRC_NM
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
                                        'UN',
                                        'D')
              AND (FIELD.ST_ANSI_CD IS NOT NULL
                   OR FIELD.CNTY_ANSI_CD IS NOT NULL) ) SubQry
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
                                                                                                'MM/DD/YYYY'))) stg) TEMP
   LEFT JOIN edv.FLD_CNTY_L dv ON (temp.FLD_CNTY_L_ID = dv.FLD_CNTY_L_ID)
   WHERE (dv.FLD_CNTY_L_ID IS NULL)
     AND temp.STG_EFF_DT_RANK = 1 )