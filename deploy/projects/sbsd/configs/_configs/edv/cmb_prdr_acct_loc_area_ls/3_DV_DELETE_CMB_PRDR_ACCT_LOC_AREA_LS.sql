MERGE INTO EDV.CMB_PRDR_ACCT_LOC_AREA_LS dv USING
  (SELECT DISTINCT MD5 (MD5 (coalesce(CMB_PRDR_ACCT.CMB_PRDR_ACCT_ID::varchar, '-1'))||COALESCE (SBSD_LOC_AREA_RS.LOC_AREA_NM, '[NULL IN SOURCE]') ||COALESCE (SBSD_LOC_AREA_RS.LOC_AREA_CAT_NM, '[NULL IN SOURCE]') ||COALESCE (SBSD_LOC_AREA_RS.CTRY_DIV_NM, '[NULL IN SOURCE]')) AS CMB_PRDR_ACCT_LOC_AREA_L_ID,
                   CMB_PRDR_ACCT.LOAD_DT,
                   CMB_PRDR_ACCT.CDC_DT,
                   CMB_PRDR_ACCT.DATA_SRC_NM,
                   CMB_PRDR_ACCT.DATA_STAT_CD,
                   CMB_PRDR_ACCT.LAST_CHG_DT,
                   CMB_PRDR_ACCT.LAST_CHG_USER_NM,
                   CMB_PRDR_ACCT.CNTY_FSA_SVC_CTR_ID,
                   CMB_PRDR_ACCT.CMB_PRDR_ACCT_ID,
                   SBSD_LOC_AREA_RS.LOC_AREA_REF_ID,
                   SBSD_LOC_AREA_RS.LOC_AREA_ID,
                   SBSD_LOC_AREA_RS.LOC_AREA_CAT_ID,
                   SBSD_LOC_AREA_RS.CTRY_DIV_ID,
                   SBSD_LOC_AREA_RS.CTRY_DIV_REF_ID,
                   CMB_PRDR_ACCT.CDC_OPER_CD
   FROM SBSD_STG.CMB_PRDR_ACCT
   INNER JOIN
     (SELECT LOC_AREA_CAT_NM,
             LOC_AREA_NM,
             CTRY_DIV_NM,
             ST_CNTY_FSA_CD,
             CNTY_FSA_SVC_CTR_ID,
             LOC_AREA_REF_ID,
             LOC_AREA_ID,
             LOC_AREA_CAT_ID,
             CTRY_DIV_ID,
             CTRY_DIV_REF_ID
      FROM
        (SELECT SBSD_LOC_AREA_RS.*,
                ROW_NUMBER() OVER (PARTITION BY CNTY_FSA_SVC_CTR_ID
                                   ORDER BY LOAD_END_DT DESC) AS rnum
         FROM EDV.SBSD_LOC_AREA_RS) a
      WHERE rnum = 1 ) SBSD_LOC_AREA_RS
     ON (SBSD_LOC_AREA_RS.CNTY_FSA_SVC_CTR_ID = CMB_PRDR_ACCT.CNTY_FSA_SVC_CTR_ID)
   WHERE CMB_PRDR_ACCT.CDC_OPER_CD='D'
     AND DATE_TRUNC('day',CMB_PRDR_ACCT.CDC_DT) = DATE_TRUNC('day',TO_TIMESTAMP ('{ETL_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF'))
     AND CMB_PRDR_ACCT.LOAD_DT = (SELECT MAX(LOAD_DT) FROM SBSD_STG.CMB_PRDR_ACCT)
   ORDER BY CMB_PRDR_ACCT.CDC_DT) stg
ON (coalesce(stg.CMB_PRDR_ACCT_ID, 0) = coalesce(dv.CMB_PRDR_ACCT_ID, 0)) 
WHEN MATCHED 
  AND dv.LOAD_DT <> stg.LOAD_DT
  AND dv.LOAD_END_DT = to_date('9999-12-31', 'YYYY-MM-DD')
THEN
UPDATE
SET LOAD_END_DT=stg.LOAD_DT,
    DATA_EFF_END_DT=stg.CDC_DT
WHEN NOT MATCHED THEN
  INSERT (CMB_PRDR_ACCT_LOC_AREA_L_ID,
          LOAD_DT,
          DATA_EFF_STRT_DT,
          DATA_SRC_NM,
          DATA_STAT_CD,
          SRC_LAST_CHG_DT,
          SRC_LAST_CHG_USER_NM,
          CNTY_FSA_SVC_CTR_ID,
          CMB_PRDR_ACCT_ID,
          LOC_AREA_REF_ID,
          LOC_AREA_ID,
          LOC_AREA_CAT_ID,
          CTRY_DIV_ID,
          CTRY_DIV_REF_ID,
          DATA_EFF_END_DT,
          LOAD_END_DT)
  VALUES (stg.CMB_PRDR_ACCT_LOC_AREA_L_ID,
          stg.LOAD_DT,
          stg.CDC_DT,
          stg.DATA_SRC_NM,
          stg.DATA_STAT_CD,
          stg.LAST_CHG_DT,
          stg.LAST_CHG_USER_NM,
          stg.CNTY_FSA_SVC_CTR_ID,
          stg.CMB_PRDR_ACCT_ID,
          stg.LOC_AREA_REF_ID,
          stg.LOC_AREA_ID,
          stg.LOC_AREA_CAT_ID,
          stg.CTRY_DIV_ID,
          stg.CTRY_DIV_REF_ID,
          stg.CDC_DT,
          stg.LOAD_DT)