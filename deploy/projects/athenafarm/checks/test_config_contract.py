"""
================================================================================
Unit tests: athenafarm Glue script / config contract
================================================================================

Version History:
    - 2026-03-27: Added TPY-04 runtime-hotfix contract coverage and evaluation
        data assertions for new-errors-2 timeout remediation.

Guards against the class of regressions that caused errors-10.txt and earlier:
  1. Stale database name defaults hardcoded in scripts (e.g. "sss", "farm_ref")
  2. Config JobParameters referencing stale database names
  3. Config passing --source_database when the script reads --target_database
     (parameter name mismatch — silent misconfiguration)
  4. Config parameters that the script never reads (dead config)

These tests touch NO AWS services and need NO Glue/Spark environment.
They parse Python source and JSON with stdlib only.

Run:
    python -m pytest deploy/projects/athenafarm/checks/test_config_contract.py -v
    # or without pytest:
    python deploy/projects/athenafarm/checks/test_config_contract.py
================================================================================
"""
from __future__ import annotations

import ast
import json
import re
import sys
import unittest
from pathlib import Path
from typing import Dict, List, Set

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_HERE = Path(__file__).resolve().parent
_PROJ = _HERE.parent
_GLUE_DIR = _PROJ / "glue"
_CONFIG_DIR = _PROJ.parent.parent / "config" / "athenafarm"
_STATE_FILE = _PROJ / "states" / "Main.param.asl.json"
_TRACT_STATE_FILE = _PROJ / "states" / "TractProducerYear.param.asl.json"
_EVAL_DATA_FILE = _HERE / "data" / "tpy01_effectiveness_eval.json"
_EVAL_DATA_TPY04_FILE = _HERE / "data" / "tpy04_effectiveness_eval.json"

# ---------------------------------------------------------------------------
# Scripts under test.
# CDC, Reference, and SSS ingest scripts are EXCLUDED — they are working and
# must not be modified.  Tests only guard the scripts that have been changed.
# ---------------------------------------------------------------------------
SCRIPTS_UNDER_TEST: List[str] = [
    "Transform-Tract-Producer-Year",
    "Transform-Farm-Producer-Year",
    "Sync-Iceberg-To-RDS",
    "Iceberg-Maintenance",
]

# All scripts — used for config-parameter-name-mismatch checks only, which
# check the config side without touching the scripts themselves.
ALL_SCRIPT_STEMS: List[str] = [
    "Ingest-SSS-Farmrecords",
    "Ingest-PG-Reference-Tables",
    "Ingest-PG-CDC-Targets",
    "Transform-Tract-Producer-Year",
    "Transform-Farm-Producer-Year",
    "Sync-Iceberg-To-RDS",
    "Iceberg-Maintenance",
]

CONFIG_FILES: List[str] = ["prod.json", "dev.json", "steamdev.json"]

# Database names that must NEVER appear as a script default or config value.
# These are the old names we've spent several sessions eliminating.
STALE_DB_NAMES: Set[str] = {
    "sss",
    "farm_ref",
    "farm_records",          # not the same as "farm_records_reporting" (RDS target, still valid)
    "fpac_farm_records",
}

# Stale names that must not appear for *database* args specifically.
# farm_records_reporting is the valid RDS target DB name — NOT a Glue catalog DB.
# Scripts that read --target_database for Glue catalog must NOT default to it.
STALE_GLUE_DB_NAMES: Set[str] = STALE_DB_NAMES | {"farm_records_reporting"}

# Expected DB name suffixes per role (any environment prefix is allowed)
_RAW_SUFFIX   = "_raw"
_REF_SUFFIX   = "_ref"
_CDC_SUFFIX   = "_cdc"
_GOLD_SUFFIX  = "_gold"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _script_text(stem: str) -> str:
    return (_GLUE_DIR / f"{stem}.py").read_text(encoding="utf-8")


def _extract_arg_defaults(stem: str) -> Dict[str, str]:
    """
    Return {key: default} for every args.get("key", "default") and
    _opt("key", "default") call in the script.
    """
    text = _script_text(stem)
    pattern = re.compile(
        r"""(?:args\.get|_opt)\(\s*['"](\w+)['"]\s*,\s*['"]([^'"]*)['"]\s*\)"""
    )
    return {m.group(1): m.group(2) for m in pattern.finditer(text)}


def _extract_required_args(stem: str) -> List[str]:
    """Return the required_args list declared in the script."""
    text = _script_text(stem)
    m = re.search(r"required_args\s*=\s*(\[[^\]]+\])", text)
    if not m:
        return []
    try:
        return ast.literal_eval(m.group(1))
    except Exception:
        return []


def _extract_all_arg_keys(stem: str) -> Set[str]:
    """
    Return every key the script reads via args.get()/args[]/getResolvedOptions
    — used to check that config params are actually consumed.
    """
    text = _script_text(stem)

    # Wrapper scripts delegate execution to Transform-Tract-Producer-Year.py.
    # Validate config keys against the delegated script contract.
    if (
        stem != "Transform-Tract-Producer-Year"
        and "runpy.run_path" in text
        and "Transform-Tract-Producer-Year.py" in text
    ):
        return _extract_all_arg_keys("Transform-Tract-Producer-Year")

    keys: Set[str] = set()
    # args.get("key") and _opt("key", ...)
    for m in re.finditer(r"""(?:args\.get|_opt)\(\s*['"](\w+)['"]""", text):
        keys.add(m.group(1))
    # args["key"]
    for m in re.finditer(r"""args\[['"](\w+)['"]\]""", text):
        keys.add(m.group(1))
    # required_args list
    keys.update(_extract_required_args(stem))
    return keys


def _load_config(filename: str) -> Dict:
    return json.loads((_CONFIG_DIR / filename).read_text(encoding="utf-8"))


def _job_params(config: Dict, stem: str) -> Dict[str, str]:
    """Extract JobParameters for a given script stem from a config's GlueConfig list."""
    for item in config.get("GlueConfig", []):
        if stem in item:
            return {k: v for k, v in item[stem].get("JobParameters", {}).items()}
    return {}


def _strip_dashes(key: str) -> str:
    """'--sss_database' → 'sss_database'"""
    return key.lstrip("-")


def _state_text() -> str:
    return _STATE_FILE.read_text(encoding="utf-8")


def _tract_state_text() -> str:
    return _TRACT_STATE_FILE.read_text(encoding="utf-8")


def _eval_data() -> Dict:
    return json.loads(_EVAL_DATA_FILE.read_text(encoding="utf-8"))


def _eval_data_tpy04() -> Dict:
    return json.loads(_EVAL_DATA_TPY04_FILE.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Test 1: No stale database names in script defaults
# ---------------------------------------------------------------------------

class TestScriptDefaults(unittest.TestCase):
    """
    Scripts under test must not fall back to stale Glue catalog database names.
    A stale default means a run without properly-deployed DefaultArguments would
    silently point at a non-existent catalog database and fail with
    'Table or view not found'.
    """

    def _check_stem(self, stem: str):
        defaults = _extract_arg_defaults(stem)
        db_keys = {k for k in defaults if "database" in k}
        for key in db_keys:
            default_val = defaults[key]
            with self.subTest(script=stem, arg=key, default=default_val):
                # rds_database is the PostgreSQL target DB name, not a Glue catalog DB —
                # it is allowed to be "farm_records_reporting"
                if key == "rds_database":
                    self.assertNotIn(
                        default_val, STALE_DB_NAMES,
                        f"{stem}: --{key} default '{default_val}' is a stale name",
                    )
                else:
                    self.assertNotIn(
                        default_val, STALE_GLUE_DB_NAMES,
                        f"{stem}: --{key} default '{default_val}' is a stale Glue catalog DB name — "
                        f"update to athenafarm_prod_* equivalent",
                    )

    def test_transform_tract(self):
        self._check_stem("Transform-Tract-Producer-Year")

    def test_transform_farm(self):
        self._check_stem("Transform-Farm-Producer-Year")

    def test_sync_iceberg_to_rds(self):
        self._check_stem("Sync-Iceberg-To-RDS")

    def test_iceberg_maintenance(self):
        self._check_stem("Iceberg-Maintenance")


# ---------------------------------------------------------------------------
# Test 2: No stale database names in any config JobParameters
# ---------------------------------------------------------------------------

class TestConfigValues(unittest.TestCase):
    """
    Every config file's JobParameters values must not reference stale names.
    """

    def _check_config_stem(self, cfg_file: str, stem: str):
        config = _load_config(cfg_file)
        params = _job_params(config, stem)
        for raw_key, value in params.items():
            key = _strip_dashes(raw_key)
            if "database" not in key:
                continue
            with self.subTest(config=cfg_file, script=stem, param=raw_key, value=value):
                if key == "rds_database":
                    self.assertNotIn(
                        value, STALE_DB_NAMES,
                        f"{cfg_file} / {stem}: {raw_key}='{value}' is a stale name",
                    )
                else:
                    self.assertNotIn(
                        value, STALE_GLUE_DB_NAMES,
                        f"{cfg_file} / {stem}: {raw_key}='{value}' is a stale Glue catalog DB name",
                    )

    def test_prod_json(self):
        for stem in ALL_SCRIPT_STEMS:
            self._check_config_stem("prod.json", stem)

    def test_dev_json(self):
        for stem in ALL_SCRIPT_STEMS:
            self._check_config_stem("dev.json", stem)

    def test_steamdev_json(self):
        for stem in ALL_SCRIPT_STEMS:
            self._check_config_stem("steamdev.json", stem)


# ---------------------------------------------------------------------------
# Test 3: Config parameter names must match what the script actually reads
# ---------------------------------------------------------------------------

class TestConfigScriptContract(unittest.TestCase):
    """
    Every --key in a config's JobParameters must correspond to a key the
    script actually reads (args.get/args[]/required_args).

    This catches silent misconfigurations like passing --source_database when
    the script reads --target_database.

    Only validates scripts under test (CDC/Reference/SSS are excluded).
    """

    def _check(self, cfg_file: str, stem: str):
        config = _load_config(cfg_file)
        params = _job_params(config, stem)
        if not params:
            return   # script not in this config — skip
        script_keys = _extract_all_arg_keys(stem)
        for raw_key in params:
            key = _strip_dashes(raw_key)
            with self.subTest(config=cfg_file, script=stem, param=raw_key):
                self.assertIn(
                    key, script_keys,
                    f"{cfg_file} passes {raw_key} to {stem}, but the script never reads "
                    f"args.get('{key}') — likely a parameter name mismatch",
                )

    def test_transform_tract_full_load_all_configs(self):
        for cfg in CONFIG_FILES:
            self._check(cfg, "Transform-Tract-Producer-Year")

    def test_transform_farm_all_configs(self):
        for cfg in CONFIG_FILES:
            self._check(cfg, "Transform-Farm-Producer-Year")

    def test_sync_all_configs(self):
        for cfg in CONFIG_FILES:
            self._check(cfg, "Sync-Iceberg-To-RDS")

    def test_iceberg_maintenance_all_configs(self):
        for cfg in CONFIG_FILES:
            self._check(cfg, "Iceberg-Maintenance")

    def test_no_source_database_in_transform_scripts(self):
        """
        Transform scripts must NOT be passed --source_database.
        They read --sss_database and --ref_database instead.
        """
        for cfg_file in CONFIG_FILES:
            config = _load_config(cfg_file)
            for stem in [
                "Transform-Tract-Producer-Year",
                "Transform-Farm-Producer-Year",
            ]:
                params = _job_params(config, stem)
                with self.subTest(config=cfg_file, script=stem):
                    self.assertNotIn(
                        "--source_database", params,
                        f"{cfg_file} / {stem}: use --sss_database and --ref_database, "
                        f"not --source_database",
                    )

    def test_no_source_database_in_sync_script(self):
        """
        Sync-Iceberg-To-RDS must NOT be passed --source_database.
        It reads --target_database for the Glue gold catalog DB.
        """
        for cfg_file in CONFIG_FILES:
            config = _load_config(cfg_file)
            params = _job_params(config, "Sync-Iceberg-To-RDS")
            with self.subTest(config=cfg_file):
                self.assertNotIn(
                    "--source_database", params,
                    f"{cfg_file} / Sync-Iceberg-To-RDS: use --target_database for the "
                    f"Glue gold catalog DB, not --source_database",
                )


# ---------------------------------------------------------------------------
# Test 4: Database semantic roles — wrong DB family is a mis-wire
# ---------------------------------------------------------------------------

class TestDatabaseSemantics(unittest.TestCase):
    """
    Verify that each arg resolves to the correct logical Glue catalog family.

    Rules:
      sss_database    → must end in _raw   (materialised SSS Iceberg tables)
      ref_database    → must end in _ref   (materialised PG reference tables)
      target_database → must end in _gold  (transform output / gold tables)
                        EXCEPT Ingest jobs, where target is _raw/_ref/_cdc
      cdc_database    → must end in _cdc   (CDC target tables)
    """

    TRANSFORM_STEMS = [
        "Transform-Tract-Producer-Year",
        "Transform-Farm-Producer-Year",
        "Sync-Iceberg-To-RDS",
        "Iceberg-Maintenance",
    ]

    def _assert_suffix(self, value: str, suffix: str, context: str):
        self.assertTrue(
            value.endswith(suffix),
            f"{context}: expected a ..{suffix} database, got '{value}'",
        )

    def _check_config(self, cfg_file: str, stem: str):
        config = _load_config(cfg_file)
        params = _job_params(config, stem)
        if not params:
            return

        sss = params.get("--sss_database")
        ref = params.get("--ref_database")
        tgt = params.get("--target_database")

        ctx = f"{cfg_file} / {stem}"

        if sss is not None:
            self._assert_suffix(sss, _RAW_SUFFIX, f"{ctx}: --sss_database")

        if ref is not None:
            self._assert_suffix(ref, _REF_SUFFIX, f"{ctx}: --ref_database")

        if tgt is not None and stem in self.TRANSFORM_STEMS:
            self._assert_suffix(tgt, _GOLD_SUFFIX, f"{ctx}: --target_database")

    def test_all_configs_transform_tract_full_load(self):
        for cfg in CONFIG_FILES:
            self._check_config(cfg, "Transform-Tract-Producer-Year")

    def test_all_configs_transform_farm(self):
        for cfg in CONFIG_FILES:
            self._check_config(cfg, "Transform-Farm-Producer-Year")

    def test_all_configs_sync(self):
        for cfg in CONFIG_FILES:
            self._check_config(cfg, "Sync-Iceberg-To-RDS")

    def test_all_configs_maintenance(self):
        for cfg in CONFIG_FILES:
            self._check_config(cfg, "Iceberg-Maintenance")

    def test_script_defaults_sss_db_is_raw(self):
        """Script-level defaults for sss_database must resolve to a _raw DB."""
        for stem in ["Transform-Tract-Producer-Year", "Transform-Farm-Producer-Year",
                     "Iceberg-Maintenance"]:
            defaults = _extract_arg_defaults(stem)
            val = defaults.get("sss_database")
            if val:
                with self.subTest(script=stem):
                    self._assert_suffix(val, _RAW_SUFFIX,
                                        f"{stem}: sss_database default")

    def test_script_defaults_ref_db_is_ref(self):
        """Script-level defaults for ref_database must resolve to a _ref DB."""
        for stem in ["Transform-Tract-Producer-Year", "Transform-Farm-Producer-Year",
                     "Iceberg-Maintenance"]:
            defaults = _extract_arg_defaults(stem)
            val = defaults.get("ref_database")
            if val:
                with self.subTest(script=stem):
                    self._assert_suffix(val, _REF_SUFFIX,
                                        f"{stem}: ref_database default")

    def test_script_defaults_target_db_is_gold_for_transforms(self):
        """Script-level target_database defaults must resolve to a _gold DB."""
        for stem in ["Transform-Tract-Producer-Year", "Transform-Farm-Producer-Year",
                     "Sync-Iceberg-To-RDS", "Iceberg-Maintenance"]:
            defaults = _extract_arg_defaults(stem)
            val = defaults.get("target_database")
            if val:
                with self.subTest(script=stem):
                    self._assert_suffix(val, _GOLD_SUFFIX,
                                        f"{stem}: target_database default")


# ---------------------------------------------------------------------------
# Test 5: Required args present
# ---------------------------------------------------------------------------

class TestRequiredArgs(unittest.TestCase):
    """
    All scripts under test must declare JOB_NAME, env, and iceberg_warehouse
    as required args so Glue validates them at startup rather than silently
    using a None value.
    """

    ALWAYS_REQUIRED = {"JOB_NAME", "env", "iceberg_warehouse"}

    def _check(self, stem: str):
        req = set(_extract_required_args(stem))
        for arg in self.ALWAYS_REQUIRED:
            with self.subTest(script=stem, arg=arg):
                self.assertIn(
                    arg, req,
                    f"{stem}: '{arg}' must be in required_args — "
                    f"currently has: {sorted(req)}",
                )

    def test_transform_tract(self):
        self._check("Transform-Tract-Producer-Year")

    def test_transform_farm(self):
        self._check("Transform-Farm-Producer-Year")

    def test_sync(self):
        self._check("Sync-Iceberg-To-RDS")

    def test_iceberg_maintenance(self):
        self._check("Iceberg-Maintenance")


# ---------------------------------------------------------------------------
# Test 6: First-run safety for missing gold target tables
# ---------------------------------------------------------------------------

class TestFirstRunTargetSafety(unittest.TestCase):
    """
    Transform jobs must be able to run on first deployment when the target
    Iceberg table does not exist yet.
    """

    def test_transform_tract_creates_target_if_missing(self):
        text = _script_text("Transform-Tract-Producer-Year")
        self.assertIn(
            'source_df.limit(0).write.format("iceberg").mode("ignore").saveAsTable(TARGET_FQN)',
            text,
            "Transform-Tract-Producer-Year must create target table if missing via DataFrame ignore bootstrap",
        )

class TestMergeFallbackSafety(unittest.TestCase):
    """
    Full-load mode must tolerate environments where Spark reports
    MERGE INTO unsupported and fall back to INSERT OVERWRITE.
    """

    def test_transform_tract_has_merge_fallback(self):
        text = _script_text("Transform-Tract-Producer-Year")
        self.assertIn('write.format("iceberg").mode("overwrite").saveAsTable(TARGET_FQN)', text)
        self.assertNotIn("falling back to DELETE + INSERT", text)
        self.assertNotIn("DELETE FROM {TARGET_FQN} WHERE true", text)

class TestOptionalArgParsingSafety(unittest.TestCase):
    """
    getResolvedOptions only returns explicitly requested args; optional
    values must be read from argv (via _opt), not args.get(...).
    """

    SCRIPTS = [
        "Transform-Tract-Producer-Year",
        "Transform-Farm-Producer-Year",
        "Sync-Iceberg-To-RDS",
        "Iceberg-Maintenance",
    ]

    OPTIONAL_KEYS = (
        "full_load",
        "sss_database",
        "ref_database",
        "target_database",
        "target_table",
        "rds_database",
        "snapshot_id_param",
        "snapshot_retention_hours",
        "debug",
    )

    def test_scripts_define_opt_helper(self):
        for stem in self.SCRIPTS:
            text = _script_text(stem)
            with self.subTest(script=stem):
                self.assertIn("def _opt(", text, f"{stem} must define _opt helper")

    def test_no_args_get_for_optional_keys(self):
        key_alt = "|".join(self.OPTIONAL_KEYS)
        pat = re.compile(rf"args\.get\(\s*['\"](?:{key_alt})['\"]")
        for stem in self.SCRIPTS:
            text = _script_text(stem)
            with self.subTest(script=stem):
                self.assertIsNone(
                    pat.search(text),
                    f"{stem} still uses args.get for optional Glue args; use _opt instead",
                )


class TestRuntimeOptimizationSafety(unittest.TestCase):
    """
    Guard against heavy debug actions that trigger large shuffles/spills.
    """

    def test_no_source_df_count_in_transforms(self):
        for stem in ["Transform-Tract-Producer-Year"]:
            text = _script_text(stem)
            with self.subTest(script=stem):
                self.assertNotIn("source_df.count()", text)

    def test_no_source_df_cache_in_transforms(self):
        for stem in ["Transform-Tract-Producer-Year"]:
            text = _script_text(stem)
            with self.subTest(script=stem):
                self.assertNotIn("source_df.cache()", text)

    def test_tract_no_incremental_delta_merge_path(self):
        text = _script_text("Transform-Tract-Producer-Year")
        self.assertNotIn("new_tract_producer_year_delta", text)
        self.assertNotIn("phase_delta_seconds", text)
        self.assertNotIn("MERGE INTO {TARGET_FQN}", text)

    def test_tract_enables_adaptive_and_skew_join(self):
        text = _script_text("Transform-Tract-Producer-Year")
        self.assertIn('"spark.sql.adaptive.enabled"', text)
        self.assertIn('"spark.sql.adaptive.skewJoin.enabled"', text)
        self.assertIn('"spark.sql.shuffle.partitions"', text)

    def test_full_load_uses_dataframe_iceberg_overwrite(self):
        for stem in ["Transform-Tract-Producer-Year"]:
            text = _script_text(stem)
            with self.subTest(script=stem):
                self.assertIn('write.format("iceberg").mode("overwrite").saveAsTable(TARGET_FQN)', text)

    def test_all_glue_jobs_enable_adaptive_settings(self):
        scripts = [
            "Ingest-SSS-Farmrecords",
            "Ingest-PG-Reference-Tables",
            "Ingest-PG-CDC-Targets",
            "Transform-Tract-Producer-Year",
            "Sync-Iceberg-To-RDS",
            "Iceberg-Maintenance",
        ]
        for stem in scripts:
            text = _script_text(stem)
            with self.subTest(script=stem):
                self.assertIn('"spark.sql.adaptive.enabled"', text)
                self.assertIn('"spark.sql.adaptive.skewJoin.enabled"', text)
                self.assertIn('"spark.sql.shuffle.partitions"', text)

    def test_sync_uses_distributed_partition_writes(self):
        text = _script_text("Sync-Iceberg-To-RDS")
        self.assertIn("foreachPartition", text)
        self.assertNotIn("delta_df.collect()", text)

    def test_tract_has_progress_markers(self):
        text = _script_text("Transform-Tract-Producer-Year")
        self.assertIn("def progress_log(", text)
        self.assertNotIn('if ENV != "PROD":', text)
        self.assertIn('[PROGRESS] progress_pct=', text)

    def test_tract_does_not_set_immutable_spark_conf_at_runtime(self):
        text = _script_text("Transform-Tract-Producer-Year")
        self.assertNotIn('spark.conf.set("spark.network.timeout"', text)
        self.assertNotIn('spark.conf.set("spark.executor.heartbeatInterval"', text)
        self.assertNotIn('spark.conf.set("spark.stage.maxConsecutiveAttempts"', text)
        self.assertNotIn('spark.conf.set("spark.task.maxFailures"', text)


class TestTractModeDispatchContract(unittest.TestCase):
    """
    Step Functions must dispatch tract transform via explicit full-load job only.
    """

    def test_state_machine_uses_nested_tract_pipeline_placeholder(self):
        text = _state_text()
        self.assertIn("__TRACT_TPY_PIPELINE_SM_ARN__", text)
        self.assertNotIn("__TRANSFORM_TRACT_PY_GLUE_JOB_NAME__", text)
        self.assertNotIn("__TRANSFORM_TRACT_PY_INCR_GLUE_JOB_NAME__", text)

    def test_state_machine_no_choice_for_tract_mode(self):
        text = _state_text()
        self.assertNotIn("ChooseTransformTractMode", text)

    def test_state_machine_no_legacy_tract_path_choice(self):
        text = _state_text()
        self.assertNotIn("SelectTransformTractPath", text)
        self.assertNotIn("force_legacy_tract_path", text)

    def test_state_machine_runs_farm_then_tract_pipeline(self):
        sm = json.loads(_state_text())
        states = sm["States"]

        self.assertIn("TransformFarmProducerYear", states)
        self.assertIn("RunTractProducerYearPipeline", states)
        self.assertEqual(states["TransformFarmProducerYear"]["Next"], "RunTractProducerYearPipeline")
        self.assertEqual(states["RunTractProducerYearPipeline"]["Resource"], "arn:aws:states:::states:startExecution.sync")
        self.assertEqual(
            states["RunTractProducerYearPipeline"]["Parameters"].get("StateMachineArn"),
            "__TRACT_TPY_PIPELINE_SM_ARN__",
        )

    def test_tract_pipeline_state_machine_contains_all_tpy_jobs(self):
        tract_sm = json.loads(_tract_state_text())
        states = tract_sm["States"]

        expected_states = {
            "TPY01SpineBase",
            "TPY02InstanceGuidMap",
            "TPY03And04And05",
            "TPY06TractCandidateAssemble",
            "TPY07PartnerCustomerResolve",
            "TPY08FarmTractResolve",
            "TPY09TractYearResolve",
            "TPY10DedupAndPublish",
            "TPYFail",
        }
        self.assertTrue(expected_states.issubset(set(states.keys())))

        self.assertEqual(tract_sm["StartAt"], "TPY01SpineBase")
        self.assertEqual(states["TPY10DedupAndPublish"].get("End"), True)

    def test_core_tract_script_forces_full_mode(self):
        text = _script_text("Transform-Tract-Producer-Year")
        self.assertIn("FULL_LOAD         = True", text)
        self.assertIn("forcing full-load execution", text)


class TestTPYSplitEffectivenessContract(unittest.TestCase):
    """
    Contract tests for TPY split-stage effectiveness and evaluation data hygiene.
    """

    def test_tpy01_is_ibsp_only_stage(self):
        text = _script_text("TPY-01-SpineBase")
        self.assertIn('spark.table(f"glue_catalog.{SSS_DB}.ibsp")', text)
        self.assertNotIn('spark.table(f"glue_catalog.{SSS_DB}.ibst")', text)
        self.assertNotIn('spark.table(f"glue_catalog.{SSS_DB}.ibib")', text)
        self.assertNotIn('spark.table(f"glue_catalog.{SSS_DB}.ibin")', text)

    def test_tpy02_takes_over_heavy_structure_expansion(self):
        text = _script_text("TPY-02-InstanceGuidMap")
        self.assertIn("ibst", text)
        self.assertIn("ibib", text)
        self.assertIn("ibin", text)
        self.assertIn("root_struct", text)

    def test_tpy06_uses_enriched_tpy02_payload(self):
        text = _script_text("TPY-06-TractCandidateAssemble")
        self.assertIn("FROM tpy_instance_guid_map ig", text)
        self.assertNotIn("FROM tpy_spine_base", text)

    def test_effectiveness_eval_data_file_exists(self):
        self.assertTrue(_EVAL_DATA_FILE.exists(), f"Missing evaluation data file: {_EVAL_DATA_FILE}")

    def test_effectiveness_eval_data_schema(self):
        d = _eval_data()
        self.assertIn("goal", d)
        self.assertIn("baseline_error", d)
        self.assertIn("validation_runs", d)

        self.assertEqual(d["goal"].get("max_duration_seconds"), 600)
        self.assertEqual(d["goal"].get("max_duration_minutes"), 10)

        baseline = d["baseline_error"]
        self.assertEqual(baseline.get("run_state"), "TIMEOUT")
        self.assertGreater(int(baseline.get("execution_time_seconds", 0)), 600)

        self.assertTrue(isinstance(d["validation_runs"], list))
        self.assertGreaterEqual(len(d["validation_runs"]), 1)

    def test_tpy04_contains_key_pruned_staged_joins(self):
        text = _script_text("TPY-04-ZmiMap")
        self.assertIn("guid_keys", text)
        self.assertIn("zd_keys", text)
        self.assertIn("fragment_keys", text)
        self.assertIn("zmi_filtered", text)

    def test_tpy04_has_version_history_comment_for_hotfix(self):
        text = _script_text("TPY-04-ZmiMap")
        self.assertIn("Version History", text)
        self.assertIn("2026-03-27", text)
        self.assertIn("new-errors-2 timeout", text)

    def test_tpy04_effectiveness_eval_data_file_exists(self):
        self.assertTrue(_EVAL_DATA_TPY04_FILE.exists(), f"Missing evaluation data file: {_EVAL_DATA_TPY04_FILE}")

    def test_tpy04_effectiveness_eval_data_schema(self):
        d = _eval_data_tpy04()
        self.assertEqual(d.get("metric"), "TPY-04-ZmiMap runtime")
        self.assertEqual(d.get("goal", {}).get("max_duration_minutes"), 15)
        self.assertEqual(d.get("baseline_error", {}).get("run_state"), "TIMEOUT")
        self.assertGreater(int(d.get("baseline_error", {}).get("execution_time_seconds", 0)), 3600)
        self.assertTrue(isinstance(d.get("validation_runs"), list))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for cls in [
        TestScriptDefaults,
        TestConfigValues,
        TestConfigScriptContract,
        TestDatabaseSemantics,
        TestRequiredArgs,
        TestFirstRunTargetSafety,
        TestMergeFallbackSafety,
        TestOptionalArgParsingSafety,
        TestRuntimeOptimizationSafety,
        TestTractModeDispatchContract,
        TestTPYSplitEffectivenessContract,
    ]:
        suite.addTests(loader.loadTestsFromTestCase(cls))

    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
