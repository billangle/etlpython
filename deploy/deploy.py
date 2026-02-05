#!/usr/bin/env python3
import argparse

from common.aws_common import read_json
from projects.fpac_pipeline.deploy import deploy as fpac_deploy
from projects.farm_records.deploy import deploy as farm_rec_deploy
from projects.flpids.deploy import deploy as flpids_deploy
from projects.tsthooks.deploy import deploy as tsthooks_deploy
from projects.cars.deploy import deploy as cars_deploy
from projects.carsdm.deploy import deploy as carsdm_deploy
from projects.sbsd.deploy import deploy as sbsd_deploy
from projects.pmrds.deploy import deploy as pmrds_deploy  
from projects.fmmi.deploy import deploy as fmmi_deploy    


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--region", required=True)
    ap.add_argument("--project-type", default="fpac", choices=["fpac","farmrec","flpids","tsthooks","cars","carsdm","sbsd","pmrds","fmmi"])
    args = ap.parse_args()

    cfg = read_json(args.config)

    if args.project_type == "fpac":
        summary = fpac_deploy(cfg, args.region)
    elif args.project_type == "farmrec":
        summary = farm_rec_deploy(cfg, args.region)
    elif args.project_type == "flpids":
        summary = flpids_deploy(cfg, args.region)
    elif args.project_type == "tsthooks":
        summary = tsthooks_deploy(cfg, args.region)
    elif args.project_type == "cars":
        summary = cars_deploy(cfg, args.region)
    elif args.project_type == "carsdm":  
        summary = carsdm_deploy(cfg, args.region)
    elif args.project_type == "sbsd":
        summary = sbsd_deploy(cfg, args.region)
    elif args.project_type == "pmrds":
        summary = pmrds_deploy(cfg, args.region)
    elif args.project_type == "fmmi":
        summary = fmmi_deploy(cfg, args.region)
    else:
        raise RuntimeError(f"Unknown project-type: {args.project_type}")

    print("\nDEPLOY SUMMARY")
    for k, v in summary.items():
        print(f"  {k}: {v}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

