INSERT INTO CAR_DM_STG.PRDR_CROP_HIST_RPT_PRMPT
(CRE_DT,
PGM_YR,
ADM_ST_NM,
ADM_CNTY_NM,
FSA_CROP_NM,
FSA_CROP_TYPE_NM,
INTN_USE_CD,
INTN_USE_NM
)
SELECT  DISTINCT 
               current_date,
               T426.PGM_YR AS C1,
               T389.ST_NM AS C2,
               T389.CNTY_NM AS C3,
               T275.FSA_CROP_NM AS C4,
               T275.FSA_CROP_TYPE_NM AS C5,
               T275.INTN_USE_CD AS C6    ,  
               T275.INTN_USE_NM as C7           
          FROM 
               BUS_PTY_DM_STG.CUST_CNTCT_FACT T152 /* ALS_PRDR_CUST_CNTCT_FACT */ ,
               CAR_DM_STG.AG_PROD_PLAN_DIM T275 /* ALS_AG_PROD_PLAN_DIM */ ,
               CAR_DM_STG.AG_PROD_PLAN_BUS_PTY_FACT T321 /* ALS_AG_PROD_PLAN_BUS_PTY_FACT */ ,
               cmn_dim_dm_stg.FSA_ST_CNTY_DIM T389 /* ALS_ADMIN_FSA_ST_CNTY_DIM */ ,
               cmn_dim_dm_stg.PGM_YR_DIM T426 /* ALS_PGM_YR_DIM */ ,
               cmn_dim_dm_stg.CUST_DIM T277 /* ALS_PRDR_CUST_DIM */ 
          WHERE T152.CUST_DURB_ID = T277.CUST_DURB_ID 
                AND T275.AG_PROD_PLAN_DURB_ID = T321.AG_PROD_PLAN_DURB_ID
                AND T277.CUST_DURB_ID = T321.CUST_DURB_ID
                AND T321.ADM_FSA_ST_CNTY_DURB_ID = T389.FSA_ST_CNTY_DURB_ID
                AND T321.PGM_YR = T426.PGM_YR
                AND T152.CUR_RCD_IND = 1                  
                AND T275.CUR_RCD_IND = 1 
                AND T277.CUR_RCD_IND = 1 
                AND T277.SRC_SOFT_DEL_IND = 0 
                AND T321.SRC_DATA_STAT_CD = 'A'                                                    
                AND T389.CUR_RCD_IND = 1  
            ORDER BY C1, C2, C3, C4, C5, C6,C7;
