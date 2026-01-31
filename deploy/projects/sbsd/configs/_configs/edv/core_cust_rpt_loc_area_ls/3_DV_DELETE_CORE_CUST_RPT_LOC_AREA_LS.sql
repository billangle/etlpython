MERGE INTO EDV.CORE_CUST_RPT_LOC_AREA_LS dv USING
  (SELECT DISTINCT MD5 (COALESCE (SBSD_CUST.CORE_CUST_ID::varchar, '-1') ||COALESCE (SBSD_LOC_AREA_RS.LOC_AREA_CAT_NM, '[NULL IN SOURCE]') ||COALESCE (SBSD_LOC_AREA_RS.LOC_AREA_NM, '[NULL IN SOURCE]') ||COALESCE (SBSD_LOC_AREA_RS.CTRY_DIV_NM, '[NULL IN SOURCE]')) AS CORE_CUST_RPT_LOC_AREA_L_ID,
                   SBSD_CUST.LOAD_DT,
                   SBSD_CUST.CDC_DT,
                   SBSD_CUST.DATA_SRC_NM,
                   SBSD_CUST.DATA_STAT_CD,
                   SBSD_CUST.LAST_CHG_DT,
                   SBSD_CUST.LAST_CHG_USER_NM,
                   SBSD_CUST.CNTY_FSA_SVC_CTR_ID,
                   SBSD_CUST.SBSD_CUST_ID,
                   SBSD_LOC_AREA_RS.LOC_AREA_REF_ID,
                   SBSD_LOC_AREA_RS.LOC_AREA_ID,
                   SBSD_LOC_AREA_RS.LOC_AREA_CAT_ID,
                   SBSD_LOC_AREA_RS.CTRY_DIV_ID,
                   SBSD_LOC_AREA_RS.CTRY_DIV_REF_ID,
                   SBSD_CUST.CDC_OPER_CD
   FROM SBSD_STG.SBSD_CUST
   INNER JOIN
     (SELECT LOC_AREA_NM,
             LOC_AREA_CAT_NM,
             CTRY_DIV_NM,
             CNTY_FSA_SVC_CTR_ID,
             LOC_AREA_REF_ID,
             LOC_AREA_ID,
             LOC_AREA_CAT_ID,
             CTRY_DIV_ID,
             CTRY_DIV_REF_ID
      FROM
        (SELECT SBSD_LOC_AREA_RS.*,
                ROW_NUMBER() OVER(PARTITION BY LOC_AREA_NM, LOC_AREA_CAT_NM, CTRY_DIV_NM
                                  ORDER BY LOAD_END_DT DESC) AS rnum
         FROM EDV.SBSD_LOC_AREA_RS) a
      WHERE rnum=1 ) SBSD_LOC_AREA_RS
   ON (SBSD_CUST.CNTY_FSA_SVC_CTR_ID=SBSD_LOC_AREA_RS.CNTY_FSA_SVC_CTR_ID)
   WHERE SBSD_CUST.CDC_OPER_CD='D'
     AND DATE_TRUNC('day',SBSD_CUST.CDC_DT) = DATE_TRUNC('day',TO_TIMESTAMP ('{ETL_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF'))
     AND SBSD_CUST.LOAD_DT = (SELECT MAX(LOAD_DT) FROM SBSD_STG.SBSD_CUST)
   ORDER BY SBSD_CUST.CDC_DT
  ) stg 
ON (coalesce(stg.SBSD_CUST_ID, 0) = coalesce(dv.SBSD_CUST_ID, 0)) 
WHEN MATCHED 
  AND dv.LOAD_DT <> stg.LOAD_DT
  AND dv.LOAD_END_DT = to_date('9999-12-31', 'YYYY-MM-DD') 
THEN
UPDATE
SET LOAD_END_DT=stg.LOAD_DT,
    DATA_EFF_END_DT=stg.CDC_DT
WHEN NOT MATCHED THEN
  INSERT (CORE_CUST_RPT_LOC_AREA_L_ID,
          LOAD_DT,
          DATA_EFF_STRT_DT,
          DATA_SRC_NM,
          DATA_STAT_CD,
          SRC_LAST_CHG_DT,
          SRC_LAST_CHG_USER_NM,
          CNTY_FSA_SVC_CTR_ID,
          SBSD_CUST_ID,
          LOC_AREA_REF_ID,
          LOC_AREA_ID,
          LOC_AREA_CAT_ID,
          CTRY_DIV_ID,
          CTRY_DIV_REF_ID,
          DATA_EFF_END_DT,
          LOAD_END_DT)
  VALUES (stg.CORE_CUST_RPT_LOC_AREA_L_ID,
          stg.LOAD_DT,
          stg.CDC_DT,
          stg.DATA_SRC_NM,
          stg.DATA_STAT_CD,
          stg.LAST_CHG_DT,
          stg.LAST_CHG_USER_NM,
          stg.CNTY_FSA_SVC_CTR_ID,
          stg.SBSD_CUST_ID,
          stg.LOC_AREA_REF_ID,
          stg.LOC_AREA_ID,
          stg.LOC_AREA_CAT_ID,
          stg.CTRY_DIV_ID,
          stg.CTRY_DIV_REF_ID,
          stg.CDC_DT,
          stg.LOAD_DT)