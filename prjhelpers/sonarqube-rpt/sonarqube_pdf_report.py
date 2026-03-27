#!/usr/bin/env python3
"""
Generate a PDF report for a SonarQube project.

Tested design target: SonarQube 9.9.x
Auth: SonarQube user token via HTTP basic auth with token as username and empty password.

Usage:
  export SONAR_HOST_URL="https://your-sonarqube.example.com"
  export SONAR_TOKEN="your_token"
  python3 sonarqube_pdf_report.py --project-key my-project --output my-project-sonar-report.pdf

Optional:
  python3 sonarqube_pdf_report.py \
      --project-key my-project \
      --branch main \
      --output my-project-main-sonar-report.pdf
"""

from __future__ import annotations

import argparse
import math
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
import urllib3
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

DEFAULT_TIMEOUT = 30


@dataclass
class SonarConfig:
    host_url: str
    token: str
    timeout: int = DEFAULT_TIMEOUT


class SonarQubeClient:
    def __init__(self, cfg: SonarConfig) -> None:
        self.cfg = cfg
        self.base = cfg.host_url.rstrip("/")
        self.session = requests.Session()
        # SonarQube 9.9 docs support token auth via basic auth: token as username, blank password.
        self.session.auth = (cfg.token, "")
        self.session.headers.update({"Accept": "application/json"})
        # Allow self-signed/invalid certs for SonarQube instances using non-public PKI.
        self.session.verify = False
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base}{path}"
        resp = self.session.get(url, params=params or {}, timeout=self.cfg.timeout)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and data.get("errors"):
            raise RuntimeError(f"SonarQube API error from {path}: {data['errors']}")
        return data

    def get_component_measures(self, project_key: str, branch: Optional[str] = None) -> Dict[str, Any]:
        metric_keys = ",".join(
            [
                "alert_status",
                "bugs",
                "vulnerabilities",
                "code_smells",
                "coverage",
                "duplicated_lines_density",
                "ncloc",
                "reliability_rating",
                "security_rating",
                "sqale_rating",
                "sqale_index",
                "security_hotspots",
                "tests",
                "test_errors",
                "test_failures",
                "skipped_tests",
            ]
        )
        params: Dict[str, Any] = {
            "component": project_key,
            "metricKeys": metric_keys,
        }
        if branch:
            params["branch"] = branch
        return self._get("/api/measures/component", params=params)

    def get_quality_gate_status(self, project_key: str, branch: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"projectKey": project_key}
        if branch:
            params["branch"] = branch
        return self._get("/api/qualitygates/project_status", params=params)

    def get_issues_summary(
        self,
        project_key: str,
        branch: Optional[str] = None,
        resolved: str = "false",
        page_size: int = 500,
        max_pages: int = 20,
    ) -> Tuple[int, Dict[str, int], Dict[str, int], List[Dict[str, Any]]]:
        """
        Returns:
          total_count,
          severities_count,
          types_count,
          first_page_issues
        """
        params: Dict[str, Any] = {
            "componentKeys": project_key,
            "resolved": resolved,
            "ps": page_size,
            "p": 1,
            "additionalFields": "_all",
        }
        if branch:
            params["branch"] = branch

        total = 0
        severity_counts: Dict[str, int] = {}
        type_counts: Dict[str, int] = {}
        first_page_issues: List[Dict[str, Any]] = []

        for page_num in range(1, max_pages + 1):
            params["p"] = page_num
            payload = self._get("/api/issues/search", params=params)

            paging = payload.get("paging", {})
            issues = payload.get("issues", [])
            if page_num == 1:
                first_page_issues = issues[:20]

            total = int(paging.get("total", len(issues)))

            for issue in issues:
                sev = issue.get("severity", "UNKNOWN")
                typ = issue.get("type", "UNKNOWN")
                severity_counts[sev] = severity_counts.get(sev, 0) + 1
                type_counts[typ] = type_counts.get(typ, 0) + 1

            if page_num * page_size >= total:
                break

        return total, severity_counts, type_counts, first_page_issues


def metric_value(measures_payload: Dict[str, Any], key: str, default: str = "N/A") -> str:
    measures = measures_payload.get("component", {}).get("measures", [])
    for item in measures:
        if item.get("metric") == key:
            return str(item.get("value", default))
    return default


def rating_to_letter(value: str) -> str:
    mapping = {
        "1.0": "A",
        "2.0": "B",
        "3.0": "C",
        "4.0": "D",
        "5.0": "E",
        "1": "A",
        "2": "B",
        "3": "C",
        "4": "D",
        "5": "E",
    }
    return mapping.get(str(value), str(value))


def safe_float(value: str) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def minutes_from_sqale_index(value: str) -> Optional[int]:
    try:
        return int(float(value))
    except Exception:
        return None


def human_minutes(total_minutes: Optional[int]) -> str:
    if total_minutes is None:
        return "N/A"
    days, rem = divmod(total_minutes, 60 * 8)
    hours, minutes = divmod(rem, 60)
    parts: List[str] = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes or not parts:
        parts.append(f"{minutes}m")
    return " ".join(parts)


def build_table(data: List[List[Any]], col_widths: Optional[List[float]] = None) -> Table:
    tbl = Table(data, colWidths=col_widths, repeatRows=1)
    tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#d9e2f3")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f7f7f7")]),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    return tbl


def make_pdf(
    output_file: str,
    project_key: str,
    branch: Optional[str],
    measures: Dict[str, Any],
    gate: Dict[str, Any],
    issue_total: int,
    issue_severity_counts: Dict[str, int],
    issue_type_counts: Dict[str, int],
    issue_examples: List[Dict[str, Any]],
) -> None:
    doc = SimpleDocTemplate(
        output_file,
        pagesize=letter,
        rightMargin=0.6 * inch,
        leftMargin=0.6 * inch,
        topMargin=0.6 * inch,
        bottomMargin=0.6 * inch,
        title=f"SonarQube Report - {project_key}",
    )

    styles = getSampleStyleSheet()
    title_style = styles["Title"]
    heading = styles["Heading2"]
    body = styles["BodyText"]
    mono = ParagraphStyle(
        "Mono",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=9,
        leading=11,
    )

    story: List[Any] = []

    story.append(Paragraph(f"SonarQube Project Report: {project_key}", title_style))
    if branch:
        story.append(Paragraph(f"Branch: {branch}", body))
    story.append(Spacer(1, 0.18 * inch))

    project_name = measures.get("component", {}).get("name", project_key)
    gate_status = gate.get("projectStatus", {}).get("status", "UNKNOWN")

    summary_rows = [
        ["Field", "Value"],
        ["Project", project_name],
        ["Project Key", project_key],
        ["Branch", branch or "Default"],
        ["Quality Gate", gate_status],
        ["Bugs", metric_value(measures, "bugs")],
        ["Vulnerabilities", metric_value(measures, "vulnerabilities")],
        ["Security Hotspots", metric_value(measures, "security_hotspots")],
        ["Code Smells", metric_value(measures, "code_smells")],
        ["Coverage", f"{metric_value(measures, 'coverage')}%"],
        ["Duplications", f"{metric_value(measures, 'duplicated_lines_density')}%"],
        ["Lines of Code", metric_value(measures, "ncloc")],
        ["Tests", metric_value(measures, "tests")],
    ]
    story.append(Paragraph("Executive Summary", heading))
    story.append(build_table(summary_rows, col_widths=[2.0 * inch, 4.7 * inch]))
    story.append(Spacer(1, 0.18 * inch))

    reliability = rating_to_letter(metric_value(measures, "reliability_rating"))
    security = rating_to_letter(metric_value(measures, "security_rating"))
    maintainability = rating_to_letter(metric_value(measures, "sqale_rating"))
    debt_minutes = minutes_from_sqale_index(metric_value(measures, "sqale_index"))

    ratings_rows = [
        ["Domain", "Rating / Value"],
        ["Reliability", reliability],
        ["Security", security],
        ["Maintainability", maintainability],
        ["Technical Debt", human_minutes(debt_minutes)],
        ["Test Errors", metric_value(measures, "test_errors")],
        ["Test Failures", metric_value(measures, "test_failures")],
        ["Skipped Tests", metric_value(measures, "skipped_tests")],
    ]
    story.append(Paragraph("Ratings and Maintainability", heading))
    story.append(build_table(ratings_rows, col_widths=[2.5 * inch, 4.2 * inch]))
    story.append(Spacer(1, 0.18 * inch))

    conditions = gate.get("projectStatus", {}).get("conditions", [])
    cond_rows = [["Metric", "Status", "Actual", "Comparator", "Error Threshold"]]
    if conditions:
        for cond in conditions:
            cond_rows.append(
                [
                    cond.get("metricKey", "N/A"),
                    cond.get("status", "N/A"),
                    cond.get("actualValue", "N/A"),
                    cond.get("comparator", "N/A"),
                    cond.get("errorThreshold", "N/A"),
                ]
            )
    else:
        cond_rows.append(["N/A", "N/A", "N/A", "N/A", "N/A"])

    story.append(Paragraph("Quality Gate Details", heading))
    story.append(build_table(cond_rows, col_widths=[1.8 * inch, 0.9 * inch, 1.1 * inch, 1.0 * inch, 1.7 * inch]))
    story.append(Spacer(1, 0.18 * inch))

    sev_rows = [["Severity", "Open Issue Count"]]
    for sev in ["BLOCKER", "CRITICAL", "MAJOR", "MINOR", "INFO"]:
        sev_rows.append([sev, str(issue_severity_counts.get(sev, 0))])

    type_rows = [["Type", "Open Issue Count"]]
    for typ in ["BUG", "VULNERABILITY", "CODE_SMELL"]:
        type_rows.append([typ, str(issue_type_counts.get(typ, 0))])
    type_rows.append(["TOTAL", str(issue_total)])

    story.append(Paragraph("Open Issue Summary", heading))
    story.append(build_table(sev_rows, col_widths=[2.5 * inch, 2.0 * inch]))
    story.append(Spacer(1, 0.10 * inch))
    story.append(build_table(type_rows, col_widths=[2.5 * inch, 2.0 * inch]))
    story.append(PageBreak())

    story.append(Paragraph("Example Open Issues", heading))
    if not issue_examples:
        story.append(Paragraph("No open issues found.", body))
    else:
        for idx, issue in enumerate(issue_examples[:15], start=1):
            rule = issue.get("rule", "N/A")
            sev = issue.get("severity", "N/A")
            typ = issue.get("type", "N/A")
            msg = issue.get("message", "N/A")
            component = issue.get("component", "N/A")
            line = issue.get("line", "N/A")
            status = issue.get("status", "N/A")

            story.append(Paragraph(f"{idx}. {typ} / {sev} / {status}", styles["Heading4"]))
            story.append(Paragraph(f"<b>Rule:</b> {rule}", mono))
            story.append(Paragraph(f"<b>Component:</b> {component}", mono))
            story.append(Paragraph(f"<b>Line:</b> {line}", mono))
            story.append(Paragraph(f"<b>Message:</b> {msg}", body))
            story.append(Spacer(1, 0.12 * inch))

    doc.build(story)


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a SonarQube project PDF report.")
    parser.add_argument("--host-url", default=None, help="SonarQube base URL. Can also use SONAR_HOST_URL env var.")
    parser.add_argument("--token", default=None, help="SonarQube token. Can also use SONAR_TOKEN env var.")
    parser.add_argument("--project-key", required=True, help="SonarQube project key.")
    parser.add_argument("--branch", default=None, help="Optional branch name.")
    parser.add_argument("--output", required=True, help="Output PDF file path.")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="HTTP timeout in seconds.")
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)

    import os

    host_url = args.host_url or os.environ.get("SONAR_HOST_URL")
    token = args.token or os.environ.get("SONAR_TOKEN")

    if not host_url:
        print("ERROR: --host-url or SONAR_HOST_URL is required.", file=sys.stderr)
        return 2
    if not token:
        print("ERROR: --token or SONAR_TOKEN is required.", file=sys.stderr)
        return 2

    cfg = SonarConfig(host_url=host_url, token=token, timeout=args.timeout)
    client = SonarQubeClient(cfg)

    try:
        measures = client.get_component_measures(args.project_key, args.branch)
        gate = client.get_quality_gate_status(args.project_key, args.branch)
        issue_total, sev_counts, type_counts, issue_examples = client.get_issues_summary(
            args.project_key,
            args.branch,
        )

        make_pdf(
            output_file=args.output,
            project_key=args.project_key,
            branch=args.branch,
            measures=measures,
            gate=gate,
            issue_total=issue_total,
            issue_severity_counts=sev_counts,
            issue_type_counts=type_counts,
            issue_examples=issue_examples,
        )
    except requests.HTTPError as exc:
        print(f"HTTP ERROR: {exc}", file=sys.stderr)
        if exc.response is not None:
            print(exc.response.text, file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(f"Created PDF report: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))