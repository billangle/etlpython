Select  TO_TIMESTAMP('{ETL_DATE}','YYYY-MM-DD') as DATA_EFF_END_DT, 
        0 as CUR_RCD_IND, 
        h.DURB_ID FLD_DURB_ID , 
        DV_DR.PGM_YR
from
(
 SELECT  FLD_H_ID ,PGM_YR
        FROM  EDV.FLD_CLU_YR_HS dv_dr
        WHERE CAST(dv_dr.DATA_EFF_END_DT AS DATE) =  TO_TIMESTAMP('{ETL_DATE}','YYYY-MM-DD') 
        AND not exists (select '1'
                        FROM edv.FLD_CLU_YR_HS inn
                        WHERE 
                        inn.FLD_H_ID = DV_DR.FLD_H_ID
                        And inn.PGM_YR = DV_DR.PGM_YR
                        And inn.DATA_EFF_END_DT = TO_TIMESTAMP('9999-12-31','YYYY-MM-DD')
                        AND inn.SRC_DATA_STAT_CD <> 'D' 
                        )
) DV_DR Inner join  EDV.FLD_H h
 ON (  COALESCE(h.FLD_H_ID,'EDW DEFAULT') = COALESCE(DV_DR.FLD_H_ID ,'EDW DEFAULT')
    AND h.DATA_SRC_NM <> 'SYSGEN'    
    )