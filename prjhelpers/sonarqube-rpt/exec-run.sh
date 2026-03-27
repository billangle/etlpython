pip install requests reportlab

export SONAR_HOST_URL="https://your-sonar"
export SONAR_TOKEN="your-token"

python sonarqube_pdf_report_executive.py \
  --project-key your_project \
  --output report.pdf