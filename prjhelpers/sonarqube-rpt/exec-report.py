# sonarqube_pdf_report_executive.py

import requests
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
import os
import argparse

def get_json(url, token):
    r = requests.get(url, auth=(token, ''))
    r.raise_for_status()
    return r.json()

def generate_report(host, token, project, output):
    measures = get_json(
        f"{host}/api/measures/component?component={project}&metricKeys=bugs,vulnerabilities,code_smells,coverage",
        token
    )

    gate = get_json(
        f"{host}/api/qualitygates/project_status?projectKey={project}",
        token
    )

    doc = SimpleDocTemplate(output)
    styles = getSampleStyleSheet()
    story = []

    status = gate['projectStatus']['status']

    story.append(Paragraph(f"<b>Project:</b> {project}", styles["Title"]))
    story.append(Spacer(1, 12))
    story.append(Paragraph(f"<b>Quality Gate:</b> {status}", styles["Heading2"]))
    story.append(Spacer(1, 12))

    for m in measures['component']['measures']:
        story.append(Paragraph(f"{m['metric']}: {m['value']}", styles["Normal"]))

    doc.build(story)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-key", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    host = os.environ["SONAR_HOST_URL"]
    token = os.environ["SONAR_TOKEN"]

    generate_report(host, token, args.project_key, args.output)