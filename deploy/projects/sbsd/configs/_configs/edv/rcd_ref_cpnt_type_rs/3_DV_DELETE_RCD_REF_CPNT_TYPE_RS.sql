UPDATE EDV.RCD_REF_CPNT_TYPE_RS /*Replace with DV table Name*/
SET DATA_EFF_END_DT = TO_TIMESTAMP ('{ETL_START_TIMESTAMP}',
                                    'YYYY-MM-DD HH24:MI:SS.FF'), /*Use CDC Start date variable*/ LOAD_END_DT =
  (SELECT coalesce(max(load_dt), current_timestamp)
   FROM SBSD_STG.RCD_REF_CPNT_TYPE /*Replace with Staging table name*/
   WHERE CDC_DT = TO_TIMESTAMP ('{ETL_START_TIMESTAMP}',
                                'YYYY-MM-DD HH24:MI:SS.FF')/*Use CDC Start date variable*/ )
WHERE (coalesce(RCD_REF_CPNT_TYPE_ID, 0))/*Use dv upd key(s)*/ NOT IN /*Identify Records that we have received in Staging for a given CDC date*/
    (SELECT coalesce(RCD_REF_CPNT_TYPE_ID, 0) /*Use DV upd key(s)*/ /*STAGING SRC PK*/
     FROM SBSD_STG.RCD_REF_CPNT_TYPE
     WHERE cdc_dt = TO_TIMESTAMP ('{ETL_START_TIMESTAMP}',
                                  'YYYY-MM-DD HH24:MI:SS.FF') )
  AND LOAD_END_DT = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD')
  AND 0 = /*below validation query gives 1, if delete count exceeds threshold, else return 0
        we want to perform delete update only if the below query return 0
        */
    (SELECT count(*)
     FROM
       (SELECT DV_CNT,
               UPD_CNT,
               round((UPD_CNT/DV_CNT)*100, 2) UPD_PERC
        FROM
          (SELECT count(DISTINCT RCD_REF_CPNT_TYPE_ID) DV_CNT /*Replace with DV src pk field name*/
           FROM EDV.RCD_REF_CPNT_TYPE_RS /*Replace with DV table name*/
           WHERE DATA_EFF_END_DT = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD') ) dv,

          (SELECT count(DISTINCT RCD_REF_CPNT_TYPE_ID) UPD_CNT /*Replace with DV src pk field name*/
           FROM EDV.RCD_REF_CPNT_TYPE_RS /*Replace with DV table name*/
           WHERE (coalesce(RCD_REF_CPNT_TYPE_ID, 0))/*Replace with DV src pk field name*/ NOT IN
               (SELECT coalesce(RCD_REF_CPNT_TYPE_ID, 0) /*Replace with Stage SRC pk Field name*/
                FROM SBSD_STG.RCD_REF_CPNT_TYPE /*Replace with Full load Stage Table Name*/
                WHERE cdc_dt = TO_TIMESTAMP ('{ETL_START_TIMESTAMP}',
                                             'YYYY-MM-DD HH24:MI:SS.FF')/*Replace with CDC Date*/ )
             AND LOAD_END_DT = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD') ) upd) TEMP /*coalesce used for parm_val, so that if that parameter is not setup due to some reason, it final outer query return 1 and delete update will not be performed*/
     WHERE UPD_PERC > coalesce(
                            (SELECT PARM_VAL::numeric
                             FROM EDV.ETL_PARM
                             WHERE TGT_SCHM_NM = 'SBSD_DA'
                               AND PARM_NM = 'DELETE_FULL_LD_THRES_PERC' ),0) )