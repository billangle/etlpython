#!/usr/bin/env python3
import argparse

from common.aws_common import read_json
from projects.fpac_pipeline.deploy import deploy as fpac_deploy


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--region", required=True)
    ap.add_argument("--project-type", default="fpac", choices=["fpac"])
    args = ap.parse_args()

    cfg = read_json(args.config)

    if args.project_type == "fpac":
        summary = fpac_deploy(cfg, args.region)
    else:
        raise RuntimeError(f"Unknown project-type: {args.project_type}")

    print("\nDEPLOY SUMMARY")
    for k, v in summary.items():
        print(f"  {k}: {v}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

