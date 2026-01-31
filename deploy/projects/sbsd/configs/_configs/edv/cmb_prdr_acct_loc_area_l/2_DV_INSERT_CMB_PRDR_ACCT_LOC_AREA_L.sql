INSERT INTO EDV.CMB_PRDR_ACCT_LOC_AREA_L (CMB_PRDR_ACCT_LOC_AREA_L_ID, CMB_PRDR_ACCT_H_ID, LOC_AREA_NM, LOC_AREA_CAT_NM, CTRY_DIV_NM, LOAD_DT, DATA_SRC_NM)
  (SELECT MD5  (stg.CMB_PRDR_ACCT_H_ID || stg.LOC_AREA_NM || stg.LOC_AREA_CAT_NM || stg.CTRY_DIV_NM) CMB_PRDR_ACCT_LOC_AREA_L_ID,
          stg.CMB_PRDR_ACCT_H_ID,
          stg.LOC_AREA_NM,
          stg.LOC_AREA_CAT_NM,
          stg.CTRY_DIV_NM,
          stg.LOAD_DT,
          stg.DATA_SRC_NM
   FROM
     (SELECT DISTINCT MD5 (coalesce(CMB_PRDR_ACCT.CMB_PRDR_ACCT_ID::varchar, '-1')) CMB_PRDR_ACCT_H_ID,
                      COALESCE (SBSD_LOC_AREA_RS.LOC_AREA_NM,
                           '[NULL IN SOURCE]') LOC_AREA_NM,
                          COALESCE (SBSD_LOC_AREA_RS.LOC_AREA_CAT_NM,
                               '[NULL IN SOURCE]') LOC_AREA_CAT_NM,
                              COALESCE (SBSD_LOC_AREA_RS.CTRY_DIV_NM,
                                   '[NULL IN SOURCE]') CTRY_DIV_NM,
                                  CMB_PRDR_ACCT.LOAD_DT LOAD_DT,
                                  CMB_PRDR_ACCT.DATA_SRC_NM DATA_SRC_NM,
                                  ROW_NUMBER () OVER (PARTITION BY MD5 (coalesce(CMB_PRDR_ACCT.CMB_PRDR_ACCT_ID::varchar, '-1')),
                                                                   COALESCE (SBSD_LOC_AREA_RS.LOC_AREA_NM,
                                                                        '[NULL IN SOURCE]') , COALESCE (SBSD_LOC_AREA_RS.LOC_AREA_CAT_NM,
                                                                                                   '[NULL IN SOURCE]') , COALESCE (SBSD_LOC_AREA_RS.CTRY_DIV_NM,
                                                                                                                              '[NULL IN SOURCE]')
                                                      ORDER BY CMB_PRDR_ACCT.CDC_DT DESC, CMB_PRDR_ACCT.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM SBSD_STG.CMB_PRDR_ACCT
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
            FROM EDV.SBSD_LOC_AREA_RS) q1
         WHERE rnum = 1 ) SBSD_LOC_AREA_RS 
   ON (SBSD_LOC_AREA_RS.CNTY_FSA_SVC_CTR_ID = CMB_PRDR_ACCT.CNTY_FSA_SVC_CTR_ID)) stg
   LEFT JOIN EDV.CMB_PRDR_ACCT_LOC_AREA_L dv ON (stg.CMB_PRDR_ACCT_H_ID = dv.CMB_PRDR_ACCT_H_ID
                                                 AND stg.LOC_AREA_NM = dv.LOC_AREA_NM
                                                 AND stg.LOC_AREA_CAT_NM = dv.LOC_AREA_CAT_NM
                                                 AND stg.CTRY_DIV_NM = dv.CTRY_DIV_NM)
   WHERE (dv.CMB_PRDR_ACCT_H_ID IS NULL
          OR dv.LOC_AREA_NM IS NULL
          OR dv.LOC_AREA_CAT_NM IS NULL
          OR dv.CTRY_DIV_NM IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )