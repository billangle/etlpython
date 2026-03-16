# CNSV Runtime Error Analysis

## Summary

- Total repeated errors found: 274
- Unique tables affected: 137
- Successful STG tables: 0/137 per execution (0.00% success).
- Across the combined runtime log payloads: 0/274 successful STG attempts (0.00% success).
- `plan.dvGroups` is not duplicated: 268 total DV table entries, 268 unique entries.
- Why failures look duplicated: `runtime.txt` contains 2 JSON execution payloads with the same 137 STG tables and the same failed-table set, so each table appears twice in the combined log.
- How the list is created: `BuildProcessingPlan` reads metadata from S3 (`_configs/metadata/stg_tables.csv`, `dv_tables.csv`, `dv_tbl_dpnds.csv`), returns `stgTables`, and builds dependency-ordered `dvGroups`.
- How the list is used by Step Functions: the state machine maps over `$.plan.stgTables` first (`ProcessStageTables`), then only proceeds to `$.plan.dvGroups` (`ProcessEDVGroups`) if stage checks pass.
- Root cause summary: metadata-driven `stgTables` included tables whose STG config artifacts could not be resolved by Glue EXEC-SQL (missing folder/file casing and/or missing incremental artifact under table folder), causing STG failures and preventing transition to EDV processing.
- One sentence summary: The runtime failures are repeated SKIPPED_MISSING_CONFIG errors in Glue EXEC-SQL where STG config paths cannot be resolved, caused by either missing table folders or missing incremental-specific artifacts under existing table folders.

## Referenced Code Line

Runtime error references:

```text
 274 Failed Line Number: 908
```

File: deploy/projects/cnsv/glue/EXEC-SQL.py

```python
# around referenced line 908
 905: 
 906: 
 907: if __name__ == "__main__":
 908:     main()
```

## Complete Missing-Config Table Audit

Legend:
- dir_found: whether STG/<TABLE> exists under deploy/projects/cnsv/configs/_configs/STG
- incremental_artifact_found: whether any file in that table folder contains 'incremental' in the filename
- similar_candidates: for dir_found=yes, sample files in the folder; for dir_found=no, close folder-name matches

| table_name | error_count | dir_found | incremental_artifact_found | similar_candidates |
|---|---:|---|---|---|
| ccms_acrg_chg_evnt | 2 | no | no | CCMS_CTR_RINST_EVNT |
| ccms_cnsv_pgm | 2 | no | no | CNSV_CNSV_PGM, CCMS_CTR_CNSV_PRAC, CNSV_CNSV_PRAC |
| ccms_cnsv_prac_stat | 2 | no | no | CCMS_CTR_CNSV_PRAC, CCMS_CTR_STAT, CNSV_CNSV_PRAC |
| ccms_crp_acrg | 2 | no | no | CCMS_CTR_ACTV_RSN, CCMS_CTR_RDN, CCMS_CTR_CLU |
| ccms_ctr_actv_rsn | 2 | yes | no | CCMS_CTR_ACTV_RSN.sql |
| ccms_ctr_actv_rsn_type | 2 | no | no | CCMS_CTR_ACTV_RSN, CCMS_CTR_FARM_TR, CCMS_CTR_CNSV_PRAC |
| ccms_ctr_actv_type | 2 | no | no | CCMS_CTR_ACTV_RSN, CCMS_CTR_CNSV_PRAC, CCMS_CTR_STAT |
| ccms_ctr_clu | 2 | yes | no | CCMS_CTR_CLU.sql |
| ccms_ctr_cnsv_prac | 2 | yes | no | CCMS_CTR_CNSV_PRAC.sql |
| ccms_ctr_core_det | 2 | no | no | CCMS_CTR_DET, CCMS_CTR_RDN, CCMS_CTR_PRDR |
| ccms_ctr_det | 2 | yes | no | CCMS_CTR_DET.sql |
| ccms_ctr_det_extn | 2 | yes | no | CCMS_CTR_DET_EXTN.sql |
| ccms_ctr_erly_out | 2 | yes | no | CCMS_CTR_ERLY_OUT.sql |
| ccms_ctr_fam_prdr_agi_ovrrd | 2 | no | no | CCMS_CTR_FARM_TR, CCMS_CTR_PRDR |
| ccms_ctr_fam_ptcp | 2 | yes | no | CCMS_CTR_FAM_PTCP.sql |
| ccms_ctr_farm_tr | 2 | yes | no | CCMS_CTR_FARM_TR.sql |
| ccms_ctr_prdr | 2 | yes | no | CCMS_CTR_PRDR.sql |
| ccms_ctr_ptcp_err_type | 2 | no | no | CCMS_CTR_PRDR, CCMS_CTR_FAM_PTCP, CCMS_CTR_DET_EXTN |
| ccms_ctr_rdn | 2 | yes | no | CCMS_CTR_RDN.sql |
| ccms_ctr_rinst_evnt | 2 | yes | no | CCMS_CTR_RINST_EVNT.sql |
| ccms_ctr_stat | 2 | yes | no | CCMS_CTR_STAT.sql |
| ccms_ctr_stat_type | 2 | no | no | CCMS_CTR_STAT, CCMS_CTR_STRT_DT_COR, CCMS_CTR_FARM_TR |
| ccms_ctr_strt_dt_cor | 2 | yes | no | CCMS_CTR_STRT_DT_COR.sql |
| ccms_extn_type | 2 | no | no | - |
| ccms_ivld_ctr_rsn | 2 | yes | no | CCMS_IVLD_CTR_RSN.sql |
| ccms_ivld_ctr_rsn_type | 2 | no | no | CCMS_IVLD_CTR_RSN |
| ccms_migr_ctr_prdr_prrt | 2 | yes | no | CCMS_MIGR_CTR_PRDR_PRRT.sql |
| ccms_mst_ctr | 2 | yes | no | CCMS_MST_CTR.sql |
| ccms_prdr_invl | 2 | no | no | CCMS_CTR_PRDR |
| ccms_pymt_lmt_type | 2 | no | no | - |
| ccms_rdn_type | 2 | no | no | - |
| ccms_sgnp | 2 | yes | no | CCMS_SGNP.sql |
| ccms_sgnp_sub_cat | 2 | no | no | CCMS_SGNP |
| ccms_sgnp_type | 2 | no | no | CCMS_SGNP |
| ccms_strt_dt_cor_evnt | 2 | no | no | CCMS_CTR_STRT_DT_COR, CCMS_CTR_RINST_EVNT, CCMS_CTR_DET_EXTN |
| ccms_term_proc | 2 | no | no | CCMS_CTR_PRDR, CCMS_CTR_FAM_PTCP, CCMS_CTR_CNSV_PRAC |
| ccpf_acct_pgm | 2 | no | no | - |
| ccpf_dir_atrb_cnfrm | 2 | no | no | - |
| ccpf_prps_pymt | 2 | no | no | - |
| ccpf_prps_pymt_stat | 2 | no | no | - |
| ccpf_prps_pymt_stat_rsn | 2 | no | no | - |
| ccpf_pymt_atrb_rdn | 2 | no | no | CCMS_CTR_RDN |
| ccpf_pymt_enty | 2 | no | no | - |
| ccpf_pymt_evnt | 2 | no | no | - |
| cnsv_adr | 2 | no | no | - |
| cnsv_afl_init | 2 | no | no | CNSV_AFL_INIT_CAT |
| cnsv_afl_init_cat | 2 | yes | no | CNSV_AFL_INIT_CAT.sql |
| cnsv_app_acct | 2 | yes | no | CNSV_APP_ACCT.sql |
| cnsv_app_acct_svc | 2 | yes | no | CNSV_APP_ACCT_SVC.sql |
| cnsv_bus_rule | 2 | yes | no | CNSV_BUS_RULE.sql |
| cnsv_bus_rule_grp | 2 | no | no | CNSV_BUS_RULE, CNSV_CNSV_PRAC_BUS_RULE_GRP |
| cnsv_bus_type | 2 | no | no | CNSV_BUS_RULE, CNSV_CNSV_PRAC_USE_TYPE |
| cnsv_cat_cnfg | 2 | yes | no | CNSV_CAT_CNFG.sql |
| cnsv_cnsv_elg_ar | 2 | no | no | CNSV_CNSV_OFR_ELG_AR, CNSV_CNSV_ELG_AR_ASSN, CNSV_CNSV_PGM |
| cnsv_cnsv_elg_ar_assn | 2 | yes | no | CNSV_CNSV_ELG_AR_ASSN.sql |
| cnsv_cnsv_evnt_mst | 2 | no | no | CNSV_CNSV_PGM, CNSV_CNSV_ELG_AR_ASSN |
| cnsv_cnsv_ofr_elg_ar | 2 | yes | no | CNSV_CNSV_OFR_ELG_AR.sql |
| cnsv_cnsv_pgm | 2 | yes | no | CNSV_CNSV_PGM.sql |
| cnsv_cnsv_prac | 2 | yes | no | CNSV_CNSV_PRAC.sql |
| cnsv_cnsv_prac_bus_rule_grp | 2 | yes | no | CNSV_CNSV_PRAC_BUS_RULE_GRP.sql |
| cnsv_cnsv_prac_use_type | 2 | yes | no | CNSV_CNSV_PRAC_USE_TYPE.sql |
| cnsv_cpnt | 2 | no | no | CNSV_CPNT_LOC, CNSV_ENTY, CNSV_CPNT_GRP_CPNT |
| cnsv_cpnt_cat | 2 | no | no | CNSV_CPNT_LOC, CNSV_CPNT_GRP_CPNT, CNSV_AFL_INIT_CAT |
| cnsv_cpnt_cost_shr_rt_type | 2 | no | no | CNSV_CPNT_LOC_COST_SHR_RT_TYPE, CNSV_CNSV_PRAC_USE_TYPE |
| cnsv_cpnt_grp | 2 | no | no | CNSV_CPNT_GRP_CPNT, CNSV_CPNT_LOC, CNSV_CNSV_PGM |
| cnsv_cpnt_grp_cnsv_prac | 2 | no | no | CNSV_CPNT_GRP_CPNT, CCMS_CTR_CNSV_PRAC, CNSV_CNSV_PRAC |
| cnsv_cpnt_grp_cpnt | 2 | yes | no | CNSV_CPNT_GRP_CPNT.sql |
| cnsv_cpnt_loc | 2 | yes | no | CNSV_CPNT_LOC.sql |
| cnsv_cpnt_loc_cost_shr_rt_type | 2 | yes | no | CNSV_CPNT_LOC_COST_SHR_RT_TYPE.sql |
| cnsv_cpnt_sub_cat | 2 | no | no | CNSV_CPNT_GRP_CPNT, CNSV_CPNT_LOC |
| cnsv_crp_grsld_ofr_prac | 2 | yes | no | CNSV_CRP_GRSLD_OFR_PRAC.sql |
| cnsv_crp_grsld_ofr_scnr | 2 | yes | no | CNSV_CRP_GRSLD_OFR_SCNR.sql |
| cnsv_ctr_prdr | 2 | no | no | CCMS_CTR_PRDR, CCMS_CTR_RDN, CNSV_FLAT_RT_PRAC |
| cnsv_dir_atrb_rqst | 2 | yes | no | CNSV_DIR_ATRB_RQST.sql |
| cnsv_enty | 2 | yes | no | CNSV_ENTY.sql |
| cnsv_ewt100soillandtype | 2 | no | no | - |
| cnsv_ewt101ofrhdwdtreectr | 2 | yes | no | CNSV_EWT101OFRHDWDTREECTR.sql |
| cnsv_ewt102_prac_rt_cat | 2 | yes | no | CNSV_EWT102_PRAC_RT_CAT.sql |
| cnsv_ewt103_cnty_prac_rt_cat | 2 | yes | no | CNSV_EWT103_CNTY_PRAC_RT_CAT.sql |
| cnsv_ewt12pract | 2 | no | no | CNSV_EWT42OFPRAC, CNSV_EWT84SPRAC, CNSV_EWT102_PRAC_RT_CAT |
| cnsv_ewt13ajcncnty | 2 | no | no | CNSV_EWT83SCCRP, CNSV_EWT51OCHST, CNSV_EWT41OCUST |
| cnsv_ewt14sgnp | 2 | no | no | CNSV_EWT84SPRAC, CNSV_EWT94NEBI, CNSV_EWT81SAVL |
| cnsv_ewt40ofrsc | 2 | yes | no | CNSV_EWT40OFRSC.sql |
| cnsv_ewt41ocust | 2 | yes | no | CNSV_EWT41OCUST.sql |
| cnsv_ewt42ofprac | 2 | yes | no | CNSV_EWT42OFPRAC.sql |
| cnsv_ewt43osoil | 2 | yes | no | CNSV_EWT43OSOIL.sql |
| cnsv_ewt44orent | 2 | yes | no | CNSV_EWT44ORENT.sql |
| cnsv_ewt45owbp | 2 | yes | no | CNSV_EWT45OWBP.sql |
| cnsv_ewt46oenvr | 2 | yes | no | CNSV_EWT46OENVR.sql |
| cnsv_ewt50prce | 2 | yes | no | CNSV_EWT50PRCE.sql |
| cnsv_ewt51ochst | 2 | yes | no | CNSV_EWT51OCHST.sql |
| cnsv_ewt76_sgnp_elg_crit | 2 | yes | no | CNSV_EWT76_SGNP_ELG_CRIT.sql |
| cnsv_ewt77_scnr_elg_resp | 2 | yes | no | CNSV_EWT77_SCNR_ELG_RESP.sql |
| cnsv_ewt80schyr | 2 | yes | no | CNSV_EWT80SCHYR.sql |
| cnsv_ewt81savl | 2 | yes | no | CNSV_EWT81SAVL.sql |
| cnsv_ewt82spgyr | 2 | yes | no | CNSV_EWT82SPGYR.sql |
| cnsv_ewt83sccrp | 2 | yes | no | CNSV_EWT83SCCRP.sql |
| cnsv_ewt84sprac | 2 | yes | no | CNSV_EWT84SPRAC.sql |
| cnsv_ewt85sebi | 2 | yes | no | CNSV_EWT85SEBI.sql |
| cnsv_ewt86shloc | 2 | yes | no | CNSV_EWT86SHLOC.sql |
| cnsv_ewt94nebi | 2 | yes | no | CNSV_EWT94NEBI.sql |
| cnsv_ewt96elprc | 2 | yes | no | CNSV_EWT96ELPRC.sql |
| cnsv_ewt97nhuc | 2 | yes | no | CNSV_EWT97NHUC.sql |
| cnsv_flat_rt_prac | 2 | yes | no | CNSV_FLAT_RT_PRAC.sql |
| cnsv_grsld_rank_fctr | 2 | no | no | CNSV_CRP_GRSLD_OFR_PRAC |
| cnsv_grsld_rank_sfctr | 2 | no | no | CNSV_CRP_GRSLD_OFR_SCNR |
| cnsv_grsld_rank_sfctr_cnty | 2 | no | no | CNSV_CRP_GRSLD_OFR_SCNR |
| cnsv_grsld_rank_sfctr_prac_use | 2 | no | no | CNSV_CRP_GRSLD_OFR_PRAC |
| cnsv_grsld_rank_sfctr_qstn | 2 | no | no | CNSV_CRP_GRSLD_OFR_SCNR |
| cnsv_grsld_rank_sfctr_rslt | 2 | no | no | - |
| cnsv_grsld_rank_sfctr_user_res | 2 | no | no | - |
| cnsv_grsld_use_type | 2 | no | no | CNSV_CNSV_PRAC_USE_TYPE |
| cnsv_gvt_lvl | 2 | no | no | - |
| cnsv_obl_rqst | 2 | yes | no | CNSV_OBL_RQST.sql |
| cnsv_ofr_clu | 2 | no | no | CNSV_EWT40OFRSC, CCMS_CTR_CLU |
| cnsv_ofr_cnsv_evnt | 2 | no | no | - |
| cnsv_ofr_farm_tr | 2 | yes | no | CNSV_OFR_FARM_TR.sql |
| cnsv_ofr_mnl_rt_det | 2 | no | no | CNSV_OFR_FARM_TR |
| cnsv_ofr_prac_afl_init | 2 | no | no | CNSV_AFL_INIT_CAT |
| cnsv_opymt_rqst | 2 | no | no | CNSV_OBL_RQST, CNSV_DIR_ATRB_RQST |
| cnsv_pgm_hrch_lvl | 2 | no | no | - |
| cnsv_prac_cpnt_cat | 2 | no | no | CNSV_AFL_INIT_CAT, CNSV_EWT102_PRAC_RT_CAT, CNSV_CPNT_LOC |
| cnsv_prac_init_cat | 2 | no | no | CNSV_AFL_INIT_CAT, CNSV_EWT102_PRAC_RT_CAT, CNSV_EWT103_CNTY_PRAC_RT_CAT |
| cnsv_prac_tech_prac | 2 | no | no | CNSV_FLAT_RT_PRAC |
| cnsv_prdr_rdn | 2 | no | no | - |
| cnsv_prps_pymt | 2 | no | no | CNSV_CPNT_GRP_CPNT, CNSV_APP_ACCT |
| cnsv_pvsn_pgm_elg_qstn | 2 | no | no | - |
| cnsv_pymt_impl | 2 | no | no | CNSV_CPNT_LOC |
| cnsv_pymt_rcpt | 2 | no | no | - |
| cnsv_pymt_rqst | 2 | no | no | CNSV_OBL_RQST, CNSV_DIR_ATRB_RQST |
| cnsv_pymt_type | 2 | no | no | - |
| cnsv_pymt_vld_ctl | 2 | no | no | CNSV_CPNT_LOC |
| cnsv_rdn_type | 2 | no | no | CNSV_ENTY |
| cnsv_sub_cat_cpnt | 2 | no | no | CNSV_CAT_CNFG, CNSV_CPNT_GRP_CPNT |
| cnsv_tech_prac | 2 | no | no | CNSV_CNSV_PRAC, CNSV_FLAT_RT_PRAC, CNSV_EWT84SPRAC |
| cnsv_tech_prac_cpnt_cat | 2 | no | no | CNSV_EWT102_PRAC_RT_CAT, CNSV_EWT103_CNTY_PRAC_RT_CAT, CNSV_AFL_INIT_CAT |
| cnsv_uom | 2 | no | no | - |
