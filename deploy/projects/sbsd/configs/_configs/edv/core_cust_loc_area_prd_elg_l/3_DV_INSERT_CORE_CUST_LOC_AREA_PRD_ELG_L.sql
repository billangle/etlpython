INSERT INTO EDV.CORE_CUST_LOC_AREA_PRD_ELG_L (CORECUST_LOC_AREA_PRD_ELG_L_ID, CORE_CUST_ID, LOC_AREA_CAT_NM, LOC_AREA_NM, CTRY_DIV_NM, SBSD_PRD_NM, LOAD_DT, DATA_SRC_NM)
  (SELECT MD5  (stg.CORE_CUST_ID || stg.LOC_AREA_CAT_NM || stg.LOC_AREA_NM || stg.CTRY_DIV_NM || stg.SBSD_PRD_NM) CORECUST_LOC_AREA_PRD_ELG_L_ID,
          stg.CORE_CUST_ID,
          stg.LOC_AREA_CAT_NM,
          stg.LOC_AREA_NM,
          stg.CTRY_DIV_NM,
          stg.SBSD_PRD_NM,
          stg.LOAD_DT,
          stg.DATA_SRC_NM
   FROM
     (SELECT DISTINCT COALESCE (SBSD_CUST.CORE_CUST_ID,
                           '-1') CORE_CUST_ID,
                          COALESCE (SBSD_LOC_AREA_RS.LOC_AREA_CAT_NM,
                               '[NULL IN SOURCE]') LOC_AREA_CAT_NM,
                              COALESCE (SBSD_LOC_AREA_RS.LOC_AREA_NM,
                                   '[NULL IN SOURCE]') LOC_AREA_NM,
                                  COALESCE (SBSD_LOC_AREA_RS.CTRY_DIV_NM,
                                       '[NULL IN SOURCE]') CTRY_DIV_NM,
                                      SBSD_PRD.SBSD_PRD_NM SBSD_PRD_NM,
                                          CNTY_CUST_PRFL_ASSN.LOAD_DT LOAD_DT,
                                          CNTY_CUST_PRFL_ASSN.DATA_SRC_NM DATA_SRC_NM,
                                          ROW_NUMBER () OVER (PARTITION BY COALESCE (SBSD_CUST.CORE_CUST_ID,
                                                                                '-1') , COALESCE (SBSD_LOC_AREA_RS.LOC_AREA_CAT_NM,
                                                                                             '[NULL IN SOURCE]') , COALESCE (SBSD_LOC_AREA_RS.LOC_AREA_NM,
                                                                                                                        '[NULL IN SOURCE]') , COALESCE (SBSD_LOC_AREA_RS.CTRY_DIV_NM,
                                                                                                                                                   '[NULL IN SOURCE]') , SBSD_PRD.SBSD_PRD_NM
                                                              ORDER BY CNTY_CUST_PRFL_ASSN.CDC_DT DESC, CNTY_CUST_PRFL_ASSN.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM
        (SELECT *
         FROM
           (SELECT CNTY_CUST_PRFL_ASSN.*,
                   ROW_NUMBER () OVER (PARTITION BY CNTY_FSA_SVC_CTR_ID,
                                                    SBSD_CUST_PRFL_ID
                                       ORDER BY LOAD_DT DESC) rnum
            FROM SBSD_STG.CNTY_CUST_PRFL_ASSN) a
         WHERE rnum=1) CNTY_CUST_PRFL_ASSN
      JOIN
        (SELECT SBSD_PRD_CUST_ELG_PRFL_ID,
                SBSD_CUST_ID,
                SBSD_PRD_ID
         FROM
           (SELECT CORE_CUST_PRD_ELG_LS.*,
                   ROW_NUMBER() OVER (PARTITION BY SBSD_PRD_CUST_ELG_PRFL_ID
                                      ORDER BY LOAD_END_DT DESC) rnum
            FROM EDV.CORE_CUST_PRD_ELG_LS
            WHERE SBSD_PRD_CUST_ELG_PRFL_ID IN
                (SELECT SBSD_CUST_PRFL_ID
                 FROM SBSD_STG.CNTY_CUST_PRFL_ASSN)) c
         WHERE rnum=1 ) SBSD_CUST_PRFL 
        ON (SBSD_CUST_PRFL.SBSD_PRD_CUST_ELG_PRFL_ID = CNTY_CUST_PRFL_ASSN.SBSD_CUST_PRFL_ID)
      JOIN EDV.V_SBSD_PRD SBSD_PRD ON (SBSD_PRD.SBSD_PRD_ID = SBSD_CUST_PRFL.SBSD_PRD_ID)
      JOIN EDV.V_SBSD_CUST SBSD_CUST ON (SBSD_CUST.SBSD_CUST_ID = SBSD_CUST_PRFL.SBSD_CUST_ID)
      INNER JOIN
        (SELECT LOC_AREA_CAT_NM,
                LOC_AREA_NM,
                CTRY_DIV_NM,
                ST_CNTY_FSA_CD,
                CNTY_FSA_SVC_CTR_ID
         FROM
           (SELECT SBSD_LOC_AREA_RS.*,
                   ROW_NUMBER() OVER (PARTITION BY CNTY_FSA_SVC_CTR_ID
                                      ORDER BY LOAD_END_DT DESC) AS rnum
            FROM EDV.SBSD_LOC_AREA_RS) d
         WHERE rnum = 1 ) SBSD_LOC_AREA_RS 
      ON (SBSD_LOC_AREA_RS.CNTY_FSA_SVC_CTR_ID = CNTY_CUST_PRFL_ASSN.CNTY_FSA_SVC_CTR_ID)) stg
   LEFT JOIN EDV.CORE_CUST_LOC_AREA_PRD_ELG_L dv ON (stg.CORE_CUST_ID = dv.CORE_CUST_ID
                                                     AND stg.LOC_AREA_CAT_NM = dv.LOC_AREA_CAT_NM
                                                     AND stg.LOC_AREA_NM = dv.LOC_AREA_NM
                                                     AND stg.CTRY_DIV_NM = dv.CTRY_DIV_NM
                                                     AND stg.SBSD_PRD_NM = dv.SBSD_PRD_NM)
   WHERE (dv.CORE_CUST_ID IS NULL
          OR dv.LOC_AREA_CAT_NM IS NULL
          OR dv.LOC_AREA_NM IS NULL
          OR dv.CTRY_DIV_NM IS NULL
          OR dv.SBSD_PRD_NM IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )