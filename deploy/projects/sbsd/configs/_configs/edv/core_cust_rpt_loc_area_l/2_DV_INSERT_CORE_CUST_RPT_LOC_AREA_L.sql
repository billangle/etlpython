INSERT INTO EDV.CORE_CUST_RPT_LOC_AREA_L (CORE_CUST_RPT_LOC_AREA_L_ID, CORE_CUST_ID, LOC_AREA_CAT_NM, LOC_AREA_NM, CTRY_DIV_NM, DATA_SRC_NM, LOAD_DT)
  (SELECT MD5  (stg.CORE_CUST_ID || stg.LOC_AREA_CAT_NM || stg.LOC_AREA_NM || stg.CTRY_DIV_NM) CORE_CUST_RPT_LOC_AREA_L_ID,
          stg.CORE_CUST_ID,
          stg.LOC_AREA_CAT_NM,
          stg.LOC_AREA_NM,
          stg.CTRY_DIV_NM,
          stg.DATA_SRC_NM,
          stg.LOAD_DT
   FROM
     (SELECT DISTINCT COALESCE (SBSD_CUST.CORE_CUST_ID,
                           '-1') CORE_CUST_ID,
                          COALESCE (SBSD_LOC_AREA_RS.LOC_AREA_CAT_NM,
                               '[NULL IN SOURCE]') LOC_AREA_CAT_NM,
                              COALESCE (SBSD_LOC_AREA_RS.LOC_AREA_NM,
                                   '[NULL IN SOURCE]') LOC_AREA_NM,
                                  COALESCE (SBSD_LOC_AREA_RS.CTRY_DIV_NM,
                                       '[NULL IN SOURCE]') CTRY_DIV_NM,
                                      SBSD_CUST.DATA_SRC_NM DATA_SRC_NM,
                                      SBSD_CUST.LOAD_DT LOAD_DT,
                                      ROW_NUMBER () OVER (PARTITION BY COALESCE (SBSD_CUST.CORE_CUST_ID,
                                                                            '-1') , COALESCE (SBSD_LOC_AREA_RS.LOC_AREA_CAT_NM,
                                                                                         '[NULL IN SOURCE]') , COALESCE (SBSD_LOC_AREA_RS.LOC_AREA_NM,
                                                                                                                    '[NULL IN SOURCE]') , COALESCE (SBSD_LOC_AREA_RS.CTRY_DIV_NM,
                                                                                                                                               '[NULL IN SOURCE]')
                                                          ORDER BY SBSD_CUST.CDC_DT DESC, SBSD_CUST.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM SBSD_STG.SBSD_CUST
      INNER JOIN
        (SELECT LOC_AREA_NM,
                LOC_AREA_CAT_NM,
                CTRY_DIV_NM,
                CNTY_FSA_SVC_CTR_ID
         FROM
           (SELECT SBSD_LOC_AREA_RS.*,
                   ROW_NUMBER() OVER(PARTITION BY LOC_AREA_NM, LOC_AREA_CAT_NM, CTRY_DIV_NM
                                     ORDER BY LOAD_END_DT DESC) AS rnum
            FROM EDV.SBSD_LOC_AREA_RS) a
         WHERE rnum=1 ) SBSD_LOC_AREA_RS
      ON (SBSD_CUST.CNTY_FSA_SVC_CTR_ID=SBSD_LOC_AREA_RS.CNTY_FSA_SVC_CTR_ID)) stg
   LEFT JOIN EDV.CORE_CUST_RPT_LOC_AREA_L dv ON (stg.CORE_CUST_ID = dv.CORE_CUST_ID
                                                 AND stg.LOC_AREA_CAT_NM = dv.LOC_AREA_CAT_NM
                                                 AND stg.LOC_AREA_NM = dv.LOC_AREA_NM
                                                 AND stg.CTRY_DIV_NM = dv.CTRY_DIV_NM)
   WHERE (dv.CORE_CUST_ID IS NULL
          OR dv.LOC_AREA_CAT_NM IS NULL
          OR dv.LOC_AREA_NM IS NULL
          OR dv.CTRY_DIV_NM IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )