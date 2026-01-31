insert into EDV.FSA_CNTY_RH
    (ST_FSA_CD,
	 CNTY_FSA_CD,
	 DATA_SRC_NM,
	 LOAD_DT)
(
	select
		stg.ST_FSA_CD,
		stg.CNTY_FSA_CD,
		stg.DATA_SRC_NM,
		stg.LOAD_DT
	from (
		select
			distinct
            coalesce (trim(PRPS_PYMT.ST_FSA_CD), '--') ST_FSA_CD,
			coalesce (trim(PRPS_PYMT.CNTY_FSA_CD), '--') CNTY_FSA_CD,
			PRPS_PYMT.DATA_SRC_NM DATA_SRC_NM,
			PRPS_PYMT.LOAD_DT LOAD_DT,
			row_number () over (partition by coalesce (trim(PRPS_PYMT.ST_FSA_CD), '--'),
			coalesce (trim(PRPS_PYMT.CNTY_FSA_CD), '--')
		order by
			PRPS_PYMT.CDC_DT desc,
			PRPS_PYMT.LOAD_DT desc) STG_EFF_DT_RANK
		from
			SBSD_STG.PRPS_PYMT
		where
			PRPS_PYMT.cdc_oper_cd in ('I', 'UN')
) stg
	left join EDV.FSA_CNTY_RH dv
    on  stg.ST_FSA_CD = dv.ST_FSA_CD
    and stg.CNTY_FSA_CD = dv.CNTY_FSA_CD
	where (dv.ST_FSA_CD is null or dv.CNTY_FSA_CD is null)
		and stg.STG_EFF_DT_RANK = 1
)