UPDATE /*+ parallel(4) */ PYMT_DM_STG.DA_RDN_TXN_FACT
SET DATA_STAT_CD = 'I',
LAST_CHG_DT = TO_TIMESTAMP('{ETL_START_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF')   
WHERE ( PYMT_ATRB_RDN_ID,PYMT_ATRB_ID, PRPS_PYMT_ID , (DATA_ACTN_DT,TO_TIMESTAMP('1900-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF'))  ) IN (
      
     Select PYMT_ATRB_RDN_ID, PYMT_ATRB_ID, PRPS_PYMT_ID,(DATA_ACTN_DT,TO_TIMESTAMP('1900-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF')) DATA_ACTN_DT
        From (
         SELECT PYMT_ATRB_RDN_ID, PYMT_ATRB_ID, PRPS_PYMT_ID, TO_TIMESTAMP('','YYYY:MM:DD HH24:MI:SS.FF') DATA_ACTN_DT
         FROM EDV.PYMT_ATRB_RDN_LS DV_DR1
         WHERE (dv_dr1.DATA_EFF_END_DT) =  TO_TIMESTAMP('{ETL_START_DATE}','YYYY-MM-DD')
         UNION
         SELECT  PYMT_ATRB_RDN_ID, PYMT_ATRB_ID, PRPS_PYMT_ID, DATA_ACTN_DT
         FROM EDV.PR_PYMT_ATRB_RDN_LS DV_DR2
         WHERE (dv_dr2.DATA_EFF_END_DT) =  TO_TIMESTAMP('{ETL_START_DATE}','YYYY-MM-DD')
         ) DV_DEL_DR
         /* The below condition to identify true deletes (not updates) */
         where not exists (
                         Select 1 From (
                           Select PYMT_ATRB_RDN_ID,  PYMT_ATRB_ID, PRPS_PYMT_ID, (DATA_ACTN_DT,TO_TIMESTAMP('1900-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF')) DATA_ACTN_DT
                           From (
                         select PYMT_ATRB_RDN_ID, PYMT_ATRB_ID, PRPS_PYMT_ID  , TO_TIMESTAMP('','YYYY:MM:DD HH24:MI:SS.FF') DATA_ACTN_DT
                         FROM EDV.PYMT_ATRB_RDN_LS  
                         where (DATA_EFF_END_DT) > TO_TIMESTAMP('{ETL_START_DATE}','YYYY-MM-DD')   
                        UNION
                         select PYMT_ATRB_RDN_ID ,PYMT_ATRB_ID, PRPS_PYMT_ID  , DATA_ACTN_DT
                         FROM EDV.PR_PYMT_ATRB_RDN_LS  
                         where (DATA_EFF_END_DT) > TO_TIMESTAMP('{ETL_START_DATE}','YYYY-MM-DD')
                                  )   as t2             
                          ) as t1  
                         
                         WHERE 
                         /* dv update/delete keys of the driving table */
                         PYMT_ATRB_RDN_ID = DV_DEL_DR.PYMT_ATRB_RDN_ID
                         And PYMT_ATRB_ID = DV_DEL_DR.PYMT_ATRB_ID
                         And PRPS_PYMT_ID = DV_DEL_DR.PRPS_PYMT_ID
                         And DV_DEL_DR.DATA_ACTN_DT = TO_TIMESTAMP ('1900-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF')                          
                         )    
                         
    ) 
 AND  DATA_STAT_CD = 'A' 