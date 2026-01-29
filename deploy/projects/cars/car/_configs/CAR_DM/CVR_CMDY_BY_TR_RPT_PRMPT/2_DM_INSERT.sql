INSERT CAR_DM_STG.CVR_CMDY_BY_TR_RPT_PRMPT
(CRE_DT,
PGM_YR,
ADM_ST_NM,
ADM_CNTY_NM,
TR_NBR
)
WITH base
AS(	   
SELECT DISTINCT              
               T897.PGM_YR AS C13,
               T881.ST_NM                    AS C5,
               T881.CNTY_NM                   AS C3,
               T813.TR_NBR                    AS C10
          FROM cmn_dim_dm_stg.PGM_YR_DIM      T897            /* ALS_PGM_YR_DIM */
                                              ,
               CAR_DM_STG.AG_PROD_PLAN_DIM    T813      /* ALS_AG_PROD_PLAN_DIM */
                                              ,
               CAR_DM_STG.PLNT_SCND_STAT_DIM  T791    /* ALS_PLNT_SCND_STAT_DIM */
                                              ,
               --(
                   cmn_dim_dm_stg.FSA_ST_CNTY_DIM  T881
                   INNER JOIN CAR_DM_STG.AG_PROD_PLAN_FACT T812
                       ON T812.ADM_FSA_ST_CNTY_DURB_ID =
                          T881.FSA_ST_CNTY_DURB_ID--)
         WHERE (    T812.PGM_YR = T897.PGM_YR
                AND T791.PGM_YR = T812.PGM_YR
                AND T791.PLNT_SCND_STAT_DURB_ID = T812.PLNT_SCND_STAT_DURB_ID
                AND T791.CUR_RCD_IND = 1
                AND T812.SRC_DATA_STAT_CD = 'A'
                AND T812.SRC_AG_PROD_PLAN_ID = T813.SRC_AG_PROD_PLAN_ID
                AND T813.SRC_DATA_STAT_CD = 'A'
                AND T813.CUR_RCD_IND = 1
                AND T881.CUR_RCD_IND = 1
                AND T897.DATA_STAT_CD = 'A'
                AND (T791.PLNT_SCND_STAT_CD IN ('-', 'F', 'P'))
                AND (TRIM (BOTH ' ' FROM T813.FSA_CROP_NM) IN
                         ('BARLEY',
                          'CANOLA',
                          'CORN',
                          'COTTON, ELS',
                          'COTTON, UPLAND',
                          'COTTON-ELS',
                          'COTTON-UPLAND',
                          'CRAMBE',
                          'FLAX',
                          'FORAGE SOYBEAN/SORGHUM',
                          'LENTILS',
                          'MUSTARD',
                          'OATS',
                          'PEANUTS',
                          'PEAS',
                          'RAPESEED',
                          'RICE',
                          'RICE, SWEET',
                          'RICE-SWEET',
                          'SAFFLOWER',
                          'SESAME',
                          'SORGHUM',
                          'SORGHUM FORAGE',
                          'SORGHUM, DUAL PURPOSE',
                          'SORGHUM-DUAL PURPOSE',
                          'SOYBEANS',
                          'SUNFLOWERS',
                          'WHEAT')))
)						  
SELECT current_date AS SYS_DATE,
       C13     AS PGM_YR,
       C5      AS ADM_ST_NM,
       C3      AS ADM_CNTY_NM,
       C10     AS TR_NBR
FROM base	   
	   