INSERT INTO EDV.CORE_CUST_LOC_AREA_PRD_ELG_LS (CORECUST_LOC_AREA_PRD_ELG_L_ID, LOAD_DT, DATA_EFF_STRT_DT, DATA_SRC_NM, SRC_LAST_CHG_DT, SRC_LAST_CHG_USER_NM, CNTY_FSA_SVC_CTR_ID, SBSD_CUST_PRFL_ID, SBSD_PRD_ID, LOC_AREA_REF_ID, LOC_AREA_ID, LOC_AREA_CAT_ID, CTRY_DIV_ID, CTRY_DIV_REF_ID, DATA_EFF_END_DT, LOAD_END_DT)
  (SELECT stg.*
   FROM
     (SELECT DISTINCT MD5 (coalesce(SBSD_CUST.CORE_CUST_ID::varchar, '-1')||COALESCE (SBSD_LOC_AREA_RS.LOC_AREA_CAT_NM, '[NULL IN SOURCE]') ||COALESCE (SBSD_LOC_AREA_RS.LOC_AREA_NM, '[NULL IN SOURCE]') || COALESCE (SBSD_LOC_AREA_RS.CTRY_DIV_NM, '[NULL IN SOURCE]') ||SBSD_PRD.SBSD_PRD_NM) AS CORECUST_LOC_AREA_PRD_ELG_L_ID,
                      CNTY_CUST_PRFL_ASSN.LOAD_DT,
                      CNTY_CUST_PRFL_ASSN.CDC_DT,
                      CNTY_CUST_PRFL_ASSN.DATA_SRC_NM,
                      CNTY_CUST_PRFL_ASSN.LAST_CHG_DT,
                      CNTY_CUST_PRFL_ASSN.LAST_CHG_USER_NM,
                      CNTY_CUST_PRFL_ASSN.CNTY_FSA_SVC_CTR_ID,
                      CNTY_CUST_PRFL_ASSN.SBSD_CUST_PRFL_ID,
                      SBSD_CUST_PRFL.SBSD_PRD_ID,
                      SBSD_LOC_AREA_RS.LOC_AREA_REF_ID,
                      SBSD_LOC_AREA_RS.LOC_AREA_ID,
                      SBSD_LOC_AREA_RS.LOC_AREA_CAT_ID,
                      SBSD_LOC_AREA_RS.CTRY_DIV_ID,
                      SBSD_LOC_AREA_RS.CTRY_DIV_REF_ID,
                      to_date('9999-12-31', 'YYYY-MM-DD') DATA_EFF_END_DT,
                      to_date('9999-12-31', 'YYYY-MM-DD') LOAD_END_DT
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
                 FROM SBSD_STG.CNTY_CUST_PRFL_ASSN)) b
         WHERE rnum=1 ) SBSD_CUST_PRFL 
      ON (SBSD_CUST_PRFL.SBSD_PRD_CUST_ELG_PRFL_ID = CNTY_CUST_PRFL_ASSN.SBSD_CUST_PRFL_ID)
      JOIN EDV.V_SBSD_PRD SBSD_PRD ON (SBSD_PRD.SBSD_PRD_ID = SBSD_CUST_PRFL.SBSD_PRD_ID)
      JOIN EDV.V_SBSD_CUST SBSD_CUST ON (SBSD_CUST.SBSD_CUST_ID = SBSD_CUST_PRFL.SBSD_CUST_ID)
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
            FROM EDV.SBSD_LOC_AREA_RS) c
         WHERE rnum = 1 ) SBSD_LOC_AREA_RS 
      ON (SBSD_LOC_AREA_RS.CNTY_FSA_SVC_CTR_ID = CNTY_CUST_PRFL_ASSN.CNTY_FSA_SVC_CTR_ID)
      WHERE CNTY_CUST_PRFL_ASSN.CDC_OPER_CD<>'D'
        AND DATE_TRUNC('day',CNTY_CUST_PRFL_ASSN.CDC_DT) = DATE_TRUNC('day',TO_TIMESTAMP ('{ETL_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF'))
        AND CNTY_CUST_PRFL_ASSN.LOAD_DT = (SELECT MAX(LOAD_DT) FROM SBSD_STG.CNTY_CUST_PRFL_ASSN) ) stg
   LEFT JOIN EDV.CORE_CUST_LOC_AREA_PRD_ELG_LS dv ON (dv.CORECUST_LOC_AREA_PRD_ELG_L_ID= stg.CORECUST_LOC_AREA_PRD_ELG_L_ID
                                                      AND coalesce(stg.CNTY_FSA_SVC_CTR_ID, 0) = coalesce(dv.CNTY_FSA_SVC_CTR_ID, 0)
                                                      AND coalesce(stg.SBSD_CUST_PRFL_ID, 0) = coalesce(dv.SBSD_CUST_PRFL_ID, 0)
                                                      AND coalesce(stg.SBSD_PRD_ID, 0) = coalesce(dv.SBSD_PRD_ID, 0)
                                                      AND coalesce(stg.LOC_AREA_REF_ID, 0) = coalesce(dv.LOC_AREA_REF_ID, 0)
                                                      AND coalesce(stg.LOC_AREA_ID, 0) = coalesce(dv.LOC_AREA_ID, 0)
                                                      AND coalesce(stg.LOC_AREA_CAT_ID, 0) = coalesce(dv.LOC_AREA_CAT_ID, 0)
                                                      AND coalesce(stg.CTRY_DIV_ID, 0) = coalesce(dv.CTRY_DIV_ID, 0)
                                                      AND coalesce(stg.CTRY_DIV_REF_ID, 0) = coalesce(dv.CTRY_DIV_REF_ID, 0)
                                                      AND dv.LOAD_END_DT = to_date('9999-12-31', 'YYYY-MM-DD'))
   WHERE dv.CORECUST_LOC_AREA_PRD_ELG_L_ID IS NULL )