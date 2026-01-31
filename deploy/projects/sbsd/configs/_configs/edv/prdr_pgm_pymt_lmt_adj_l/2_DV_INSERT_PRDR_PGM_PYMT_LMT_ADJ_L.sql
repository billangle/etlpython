insert into EDV.PRDR_PGM_PYMT_LMT_ADJ_L
    (PRDR_PGM_PYMT_LMT_ADJ_L_ID ,
	    CORE_CUST_ID,
	    LTD_PYMT_PGM_NM,
	    SBSD_PRD_STRT_YR,
	    PRDR_PYMT_LMT_ADJ_TYPE_CD,
	    LOAD_DT,
	    DATA_SRC_NM)
(
	select
		stg.PRDR_PGM_PYMT_LMT_ADJ_L_ID,
		stg.CORE_CUST_ID,
		stg.LTD_PYMT_PGM_NM,
		stg.SBSD_PRD_STRT_YR,
		stg.PRDR_PYMT_LMT_ADJ_TYPE_CD,
		stg.LOAD_DT,
		stg.DATA_SRC_NM
	from
		(
		select
			distinct
            coalesce (PRDR_PGM_PYMT_LMT_ADJ.CORE_CUST_ID, '-1') CORE_CUST_ID,
			coalesce (PRDR_PGM_PYMT_LMT_ADJ.PGM_NM, '[NULL IN SOURCE]') LTD_PYMT_PGM_NM,
			coalesce (PRDR_PGM_PYMT_LMT_ADJ.SBSD_PRD_STRT_YR, '-1') SBSD_PRD_STRT_YR,
			coalesce (PRDR_PGM_PYMT_LMT_ADJ.PRDR_PYMT_LMT_ADJ_TYPE_CD, '--') PRDR_PYMT_LMT_ADJ_TYPE_CD,
			PRDR_PGM_PYMT_LMT_ADJ.LOAD_DT LOAD_DT,
			PRDR_PGM_PYMT_LMT_ADJ.DATA_SRC_NM DATA_SRC_NM,
			row_number () over (partition by coalesce (PRDR_PGM_PYMT_LMT_ADJ.CORE_CUST_ID, '-1') ,
                                             coalesce (PRDR_PGM_PYMT_LMT_ADJ.PGM_NM, '[NULL IN SOURCE]') ,
                                             coalesce (PRDR_PGM_PYMT_LMT_ADJ.SBSD_PRD_STRT_YR, '-1') ,
                                             coalesce (PRDR_PGM_PYMT_LMT_ADJ.PRDR_PYMT_LMT_ADJ_TYPE_CD, '--')
		                        order by PRDR_PGM_PYMT_LMT_ADJ.CDC_DT desc,
			                             PRDR_PGM_PYMT_LMT_ADJ.LOAD_DT desc) 
                                as STG_EFF_DT_RANK,
			md5(coalesce (PRDR_PGM_PYMT_LMT_ADJ.CORE_CUST_ID, '-1') || '~~' || upper(coalesce (trim(PRDR_PGM_PYMT_LMT_ADJ.PGM_NM), '[NULL IN SOURCE]')) || '~~' || coalesce (PRDR_PGM_PYMT_LMT_ADJ.SBSD_PRD_STRT_YR, '-1') || '~~' || upper(coalesce (trim(PRDR_PGM_PYMT_LMT_ADJ.PRDR_PYMT_LMT_ADJ_TYPE_CD), '--')) ) PRDR_PGM_PYMT_LMT_ADJ_L_ID
		from
			SBSD_STG.PRDR_PGM_PYMT_LMT_ADJ
		where
			PRDR_PGM_PYMT_LMT_ADJ.cdc_oper_cd in ('I', 'UN', 'D')
) stg
left join EDV.PRDR_PGM_PYMT_LMT_ADJ_L dv
       on stg.PRDR_PGM_PYMT_LMT_ADJ_L_ID = dv.PRDR_PGM_PYMT_LMT_ADJ_L_ID
    where dv.PRDR_PGM_PYMT_LMT_ADJ_L_ID is null
      and stg.STG_EFF_DT_RANK = 1
)