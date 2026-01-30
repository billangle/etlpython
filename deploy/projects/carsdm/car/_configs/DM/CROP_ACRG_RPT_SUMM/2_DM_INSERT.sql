INSERT INTO CAR_DM_STG.CROP_ACRG_RPT_SUMM
(
   CRE_DT, 
   PGM_YR, 
   ST_FSA_CD , 
   CNTY_FSA_CD,  
   ACRG_RPT_CRE_CT,
   ACRG_RPT_UPD_CT,  
   CROP_FLD_ADD_CT,
   CROP_FLD_UPD_CT, 
   CROP_FLD_CT,   
   PRV_PGM_YR_CROP_FLD_CT,
   ACRG_RPT_ACTV_CT,
   ACRG_RPT_DEL_CT,
   FULL_RPT_ACRG_RPT_CT, 
   PRV_PGMYR_FULL_RPT_ACRG_RPT_CT, 
   PGM_YR_CROP_FLD_CT, 
   PGM_YR_FULL_RPT_ACRG_RPT_CT, 
   PGM_YR_ACRG_RPT_ACTV_CT
)
WITH years
AS (
    SELECT PGM_YR
        , ADD_MONTHS(TRUNC(current_date)-1, (1-rownum)*12) AS Yesterday
        ,   rownum as Rnbr
    FROM (  SELECT PGM_YR
            FROM cmn_dim_dm_stg.PGM_YR_DIM 
            WHERE DATA_STAT_CD='A' 
                AND current_date BETWEEN PGM_YR_STRT_DT AND ADD_MONTHS(PGM_YR_END_DT,36)
            ORDER BY 1 DESC )
)
, STCNTYYR
AS
(
    SELECT
            years.pgm_yr
        ,   years.Yesterday
        ,   years.rNbr
        ,   cnty.ST_FSA_CD
        ,   cnty.CNTY_FSA_CD
    FROM years
        CROSS JOIN (    SELECT
                                ST_FSA_CD
                            ,   CNTY_FSA_CD
                        FROM cmn_dim_dm_stg.FSA_ST_CNTY_DIM
                        WHERE CUR_RCD_IND = 1
                            AND FSA_ST_CNTY_DURB_ID > 0
                            AND CNTY_FSA_CD <> '000' ) cnty
)
,   Apps
AS
(
    SELECT   appd.PGM_YR
        ,   appd.ST_FSA_CD
        ,   appd.CNTY_FSA_CD
        --,   COUNT(DISTINCT appd.CNTY_FSA_CD ) AS Counties
        ,   SUM(CASE WHEN TRUNC (appd.AG_PROD_PLAN_CRE_DT) = (TRUNC(current_date)-1) THEN 1 ELSE 0 END) AS CropFieldAdded
        ,   SUM(CASE WHEN TRUNC (appd.AG_PROD_PLAN_LAST_CHG_DT) = (TRUNC(current_date)-1)
                        AND TRUNC (appd.AG_PROD_PLAN_LAST_CHG_DT) != TRUNC (appd.AG_PROD_PLAN_CRE_DT)
                    THEN 1
                    ELSE 0 END ) AS CropFieldUpdated
        ,   COUNT(*) AS TotalField
        ,   SUM(CASE WHEN TRUNC (appd.AG_PROD_PLAN_CRE_DT) <= years.Yesterday THEN 1 ELSE 0 END) AS CropFieldPgmYr
    FROM CAR_DM_STG.AG_PROD_PLAN_DIM appd
        INNER JOIN years ON years.pgm_yr = appd.pgm_yr
        INNER JOIN CAR_DM_STG.AG_PROD_PLAN_FACT B ON appd.SRC_AG_PROD_PLAN_ID=b.SRC_AG_PROD_PLAN_ID
        INNER JOIN CAR_DM_STG.CROP_ACRG_RPT_DIM C on B.CROP_ACRG_RPT_DURB_ID=C.CROP_ACRG_RPT_DURB_ID
    WHERE
            appd.CUR_RCD_IND = 1 
        AND appd.SRC_DATA_STAT_CD = 'A'
        AND B.DATA_STAT_CD = 'A'
        AND B.SRC_DATA_STAT_CD= 'A'
        AND C.SRC_DATA_STAT_CD= 'A'
        AND C.CUR_RCD_IND = 1
    GROUP BY appd.PGM_YR
        ,   appd.ST_FSA_CD
        ,   appd.CNTY_FSA_CD
)
,   Cars
AS
(   
    SELECT cars.pgm_yr
        ,   cars.ST_FSA_CD
        ,   cars.CNTY_FSA_CD
        ,   SUM(CASE WHEN cars.SRC_DATA_STAT_CD = 'A'
                        AND TRUNC (cars.CROP_ACRG_RPT_CRE_DT) =  (TRUNC(current_date)-1) 
                    THEN 1 
                    ELSE 0 END) AS CreatedReports
        ,   SUM(CASE WHEN cars.SRC_DATA_STAT_CD = 'A'
                        AND TRUNC (CARS.ACRG_RPT_LAST_MOD_DT) = (TRUNC(current_date)-1)
                        AND TRUNC (CARS.ACRG_RPT_LAST_MOD_DT) != TRUNC (CARS.CROP_ACRG_RPT_CRE_DT)
                    THEN 1
                    ELSE 0 END ) AS UpdatedReports
        ,   SUM(CASE WHEN cars.SRC_DATA_STAT_CD = 'A' THEN 1 ELSE 0 END) AS TotalReport
--        ,   SUM(CASE WHEN cars.SRC_DATA_STAT_CD = 'D' THEN 1 ELSE 0 END) AS DeleteReport
        ,   SUM(CASE WHEN cars.SRC_DATA_STAT_CD = 'A'
                        AND ( cars.RQR_PLNT_STAT_CPLT_IND = 1 -- 'Y'
                                OR cars.ACRG_RPT_CNTNT_EXST_IND = 1  -- 'Y'
                            )
                    THEN 1
                    ELSE 0 END ) AS CompleteReport
        ,   SUM(CASE WHEN SRC_DATA_STAT_CD = 'A'
                        AND ( ( cars.RQR_PLNT_STAT_CPLT_IND = 1 -- 'Y'
                                AND TRUNC(cars.RQR_PLNT_STAT_CPLT_DT) <= years.Yesterday )
                              OR ( cars.ACRG_RPT_CNTNT_EXST_IND = 1  -- 'Y'
                                    AND TRUNC(cars.LAST_OVRRD_CHG_DT) <= years.Yesterday ) )
                    THEN 1
                    ELSE 0 END ) AS FullReportPgmYr
        ,   SUM(CASE WHEN cars.SRC_DATA_STAT_CD = 'A' 
                        AND TRUNC(cars.CROP_ACRG_RPT_CRE_DT) <= years.Yesterday 
                    THEN 1 
                    ELSE 0 END) AS AcrgRptPgmYr
    FROM   CAR_DM_STG.CROP_ACRG_RPT_DIM cars
        inner join years ON years.pgm_yr = cars.pgm_yr
    WHERE   cars.CUR_RCD_IND = 1
        AND cars.SRC_DATA_STAT_CD IN ( 'A', 'D' )
        AND cars.DOC_CERT_STAT_CD <> 'N'  --Rev 4.0 RA 07/13/2015
    GROUP BY   cars.pgm_yr
        , cars.ST_FSA_CD
        , cars.CNTY_FSA_CD
)
, dv
AS
(
    SELECT 
            h.pgm_yr
        ,   h.st_fsa_cd
        ,   h.cnty_fsa_cd
        ,   COUNT(DISTINCT hs.CROP_ACRG_RPT_ID) AS DeleteReport
    FROM edv.crop_acrg_rpt_h h
        inner join years
            on h.pgm_yr = years.pgm_yr
        inner join edv.crop_acrg_rpt_hs hs
            on h.crop_acrg_rpt_h_id = hs.crop_acrg_rpt_h_id
               -- and data_eff_end_dt = to_date('99991231', 'yyyymmdd')
    where hs.data_stat_cd = 'D'
        and hs.doc_cert_stat_cd <> 'N'
    group by h.pgm_yr, h.st_fsa_cd, h.cnty_fsa_cd
)
SELECT  /*+PARALLEL(8) */
        current_date as CRE_DT
    ,   STCNTYYR.PGM_YR
    ,   STCNTYYR.ST_FSA_CD
    ,   STCNTYYR.CNTY_FSA_CD
    --,   Apps.Counties
    ,   Cars.CreatedReports as ACRG_RPT_CRE_CT
    ,   Cars.UpdatedReports as ACRG_RPT_UPD_CT
    ,   Apps.CropFieldAdded as CROP_FLD_ADD_CT
    ,   Apps.CropFieldUpdated as CROP_FLD_UPD_CT
    ,   Apps.TotalField as CROP_FLD_CT
    ,   PrevApps.TotalField AS PRV_PGM_YR_CROP_FLD_CT
    --,   CASE WHEN COALESCE(PrevApps.TotalField,0) = 0 THEN 0.0
    --        ELSE ( Apps.TotalField / PrevApps.TotalField ) * 100.0  END AS TotalFieldPercent
    ,   Cars.TotalReport as ACRG_RPT_ACTV_CT
    ,   dv.DeleteReport as ACRG_RPT_DEL_CT
    ,   Cars.CompleteReport as FULL_RPT_ACRG_RPT_CT
    ,   PrevCars.CompleteReport AS PRV_PGMYR_FULL_RPT_ACRG_RPT_CT
    --,   CASE WHEN COALESCE(PrevCArs.CompleteReport,0) = 0 THEN 0.0
    --        ELSE ( Cars.CompleteReport / PrevCars.CompleteReport ) * 100.0 END AS CompleteReportPercent
    ,   Apps.CropFieldPgmYr as PGM_YR_CROP_FLD_CT
    ,   Cars.FullReportPgmYr as PGM_YR_FULL_RPT_ACRG_RPT_CT 
    ,   Cars.AcrgRptPgmYr as PGM_YR_ACRG_RPT_ACTV_CT
FROM STCNTYYR
    FULL OUTER JOIN Apps
        ON STCNTYYR.PGM_YR = Apps.PGM_YR
            AND STCNTYYR.ST_FSA_CD = Apps.ST_FSA_CD
            AND STCNTYYR.CNTY_FSA_CD = Apps.CNTY_FSA_CD
    FULL OUTER JOIN Apps PrevApps
        ON STCNTYYR.PGM_YR - 1 = PrevApps.PGM_YR
            AND STCNTYYR.ST_FSA_CD = PrevApps.ST_FSA_CD
            AND STCNTYYR.CNTY_FSA_CD = PrevApps.CNTY_FSA_CD
    FULL OUTER JOIN dv
        ON STCNTYYR.PGM_YR = dv.PGM_YR
            AND STCNTYYR.ST_FSA_CD = dv.ST_FSA_CD
            AND STCNTYYR.CNTY_FSA_CD = dv.CNTY_FSA_CD
    FULL OUTER JOIN Cars 
        ON STCNTYYR.PGM_YR = Cars.PGM_YR
            AND STCNTYYR.ST_FSA_CD = Cars.ST_FSA_CD
            AND STCNTYYR.CNTY_FSA_CD = Cars.CNTY_FSA_CD
    FULL OUTER JOIN Cars PrevCars
        ON STCNTYYR.PGM_YR - 1 = PrevCars.PGM_YR
            AND STCNTYYR.ST_FSA_CD = PrevCars.ST_FSA_CD
            AND STCNTYYR.CNTY_FSA_CD = PrevCars.CNTY_FSA_CD
WHERE   STCNTYYR.Rnbr <= 3'
